package converter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// JSON-RPC generic message structures

type jsonrpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func (m *jsonrpcMessage) isRequest() bool {
	return m.Method != "" && len(m.ID) > 0
}

func (m *jsonrpcMessage) isNotification() bool {
	return m.Method != "" && len(m.ID) == 0
}

func (m *jsonrpcMessage) isResponse() bool {
	return m.Method == "" && len(m.ID) > 0 && (len(m.Result) > 0 || len(m.Error) > 0)
}

func (m *jsonrpcMessage) idKey() string {
	if len(m.ID) == 0 {
		return ""
	}
	return string(m.ID)
}

// SSE connection writer

type sseConn struct {
	w       http.ResponseWriter
	flusher http.Flusher
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
}

func newSSEConn(w http.ResponseWriter, req *http.Request) *sseConn {
	ctx, cancel := context.WithCancel(req.Context())
	return &sseConn{w: w, flusher: w.(http.Flusher), ctx: ctx, cancel: cancel}
}

func (s *sseConn) WriteEvent(data []byte, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.EOF
	}
	var buf bytes.Buffer
	if eventID != "" {
		buf.WriteString("id: ")
		buf.WriteString(eventID)
		buf.WriteString("\n")
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		buf.WriteString("data: ")
		buf.Write(line)
		buf.WriteString("\n")
	}
	buf.WriteString("\n")
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		s.closed = true
		return err
	}
	s.flusher.Flush()
	return nil
}

func (s *sseConn) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
}

// Session management

type session struct {
	id           string
	cmdStr       string
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	createdAt    time.Time
	inflightMu   sync.Mutex
	inflight     map[string]chan json.RawMessage
	serverMsgMu  sync.Mutex
	sseConns     []*sseConn // we will send to the first active only
	lastEventSeq int64
	shutdownOnce sync.Once
}

type Converter struct {
	cmdStr        string
	transportType string
	targetURL     *url.URL
	reverseProxy  *httputil.ReverseProxy
	cmd           *exec.Cmd
	sessionsMu    sync.Mutex
	sessions      map[string]*session
	pendingSSEMu  sync.Mutex
	pendingSSE    []*sseConn // SSE connections waiting for session creation
}

func NewConverter(cmdStr string, transportType string, internalPort string) *Converter {
	p := &Converter{
		cmdStr:        cmdStr,
		transportType: transportType,
		sessions:      make(map[string]*session),
		pendingSSE:    make([]*sseConn, 0),
	}

	if transportType == "http" || transportType == "sse" {
		// Get target URL from environment or use default
		targetURLStr := "http://localhost:" + internalPort
		targetURL, err := url.Parse(targetURLStr)
		if err != nil {
			log.Fatalf("invalid TARGET_URL: %v", err)
		}
		p.targetURL = targetURL
		p.reverseProxy = httputil.NewSingleHostReverseProxy(targetURL)
		// Start the command
		if err := p.startHTTPCommand(); err != nil {
			log.Fatalf("failed to start HTTP command: %v", err)
		}
	}

	return p
}

func (p *Converter) HandleEndpoint(w http.ResponseWriter, r *http.Request) {
	// For http/sse transport, just forward everything
	if p.transportType == "http" || p.transportType == "sse" {
		p.reverseProxy.ServeHTTP(w, r)
		return
	}

	// For stdio transport, use existing JSON-RPC handling
	switch r.Method {
	case http.MethodPost:
		p.handlePOST(w, r)
	case http.MethodGet:
		p.handleGET(w, r)
	case http.MethodDelete:
		p.handleDELETE(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (p *Converter) handlePOST(w http.ResponseWriter, r *http.Request) {
	accept := r.Header.Get("Accept")
	_ = accept // currently not used; we always return application/json for requests and 202 for notifications/responses

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	var msg jsonrpcMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "invalid JSON-RPC message", http.StatusBadRequest)
		return
	}

	sessID := r.Header.Get("Mcp-Session-Id")
	if msg.Method == "initialize" && sessID == "" {
		// Start a new session and forward initialize
		s, err := p.startSession()
		if err != nil {
			log.Printf("failed to start session: %v", err)
			http.Error(w, "failed to start session", http.StatusInternalServerError)
			return
		}

		// Associate any pending SSE connections with this new session
		p.pendingSSEMu.Lock()
		if len(p.pendingSSE) > 0 {
			s.serverMsgMu.Lock()
			s.sseConns = append(s.sseConns, p.pendingSSE...)
			s.serverMsgMu.Unlock()
			p.pendingSSE = nil // Clear pending connections
		}
		p.pendingSSEMu.Unlock()

		// forward request and wait for response
		resp, err := p.forwardRequestAwaitResponse(s, body, msg)
		if err != nil {
			log.Printf("initialize forwarding error: %v", err)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}

		// Also send the initialize response over SSE if there are SSE connections
		s.serverMsgMu.Lock()
		if len(s.sseConns) > 0 {
			// Send response over SSE as well
			s.lastEventSeq++
			idStr := fmt.Sprintf("%d", s.lastEventSeq)
			for _, conn := range s.sseConns {
				_ = conn.WriteEvent(resp, idStr)
			}
		}
		s.serverMsgMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Mcp-Session-Id", s.id)
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
		return
	}

	if sessID == "" {
		http.Error(w, "missing Mcp-Session-Id", http.StatusBadRequest)
		return
	}
	s := p.getSession(sessID)
	if s == nil {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}

	if msg.isNotification() || msg.isResponse() {
		if err := p.forwardFireAndForget(s, body); err != nil {
			log.Printf("forward notif/resp error: %v", err)
			http.Error(w, "forwarding error", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if msg.isRequest() {
		resp, err := p.forwardRequestAwaitResponse(s, body, msg)
		if err != nil {
			log.Printf("request forwarding error: %v", err)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
		return
	}

	http.Error(w, "unsupported JSON-RPC message", http.StatusBadRequest)
}

func (p *Converter) handleGET(w http.ResponseWriter, r *http.Request) {
	sessID := r.Header.Get("Mcp-Session-Id")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	flusher.Flush()

	sconn := newSSEConn(w, r)

	if sessID == "" {
		// No session ID - store as pending SSE connection
		// It will be associated with a session when initialize POST arrives
		p.pendingSSEMu.Lock()
		p.pendingSSE = append(p.pendingSSE, sconn)
		p.pendingSSEMu.Unlock()

		// block until client disconnects
		<-r.Context().Done()
		sconn.Close()

		// Remove from pending if still there
		p.pendingSSEMu.Lock()
		var active []*sseConn
		for _, c := range p.pendingSSE {
			if c != sconn {
				active = append(active, c)
			}
		}
		p.pendingSSE = active
		p.pendingSSEMu.Unlock()
		return
	}

	// Session ID provided - use existing session
	s := p.getSession(sessID)
	if s == nil {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}

	// register connection
	s.serverMsgMu.Lock()
	s.sseConns = append(s.sseConns, sconn)
	s.serverMsgMu.Unlock()

	// block until client disconnects
	<-r.Context().Done()
	sconn.Close()
	// cleanup closed conns
	s.serverMsgMu.Lock()
	var active []*sseConn
	for _, c := range s.sseConns {
		if c != sconn {
			active = append(active, c)
		}
	}
	s.sseConns = active
	s.serverMsgMu.Unlock()
}

func (p *Converter) handleDELETE(w http.ResponseWriter, r *http.Request) {
	sessID := r.Header.Get("Mcp-Session-Id")
	if sessID == "" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p.stopSession(sessID)
	w.WriteHeader(http.StatusNoContent)
}

// Process/session helpers

func (p *Converter) startSession() (*session, error) {
	id, err := randID()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("/bin/sh", "-lc", p.cmdStr)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &session{
		id:        id,
		cmdStr:    p.cmdStr,
		cmd:       cmd,
		stdin:     stdin,
		stdout:    stdout,
		stderr:    stderr,
		createdAt: time.Now(),
		inflight:  make(map[string]chan json.RawMessage),
		sseConns:  make([]*sseConn, 0, 1),
	}
	p.sessionsMu.Lock()
	p.sessions[id] = s
	p.sessionsMu.Unlock()

	go p.readStdoutLoop(s)
	go p.readStderrLoop(s)
	go p.waitProcess(s)

	return s, nil
}

func (p *Converter) getSession(id string) *session {
	p.sessionsMu.Lock()
	defer p.sessionsMu.Unlock()
	return p.sessions[id]
}

func (p *Converter) stopSession(id string) {
	p.sessionsMu.Lock()
	s := p.sessions[id]
	if s != nil {
		delete(p.sessions, id)
	}
	p.sessionsMu.Unlock()
	if s == nil {
		return
	}
	s.shutdownOnce.Do(func() {
		// Close stdin to signal EOF
		_ = s.stdin.Close()
		// Give it a moment to exit gracefully
		done := make(chan struct{})
		go func() {
			_ = s.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = s.cmd.Process.Kill()
		}
		// Close SSE conns
		s.serverMsgMu.Lock()
		for _, c := range s.sseConns {
			c.Close()
		}
		s.sseConns = nil
		s.serverMsgMu.Unlock()
	})
}

func (p *Converter) forwardFireAndForget(s *session, data []byte) error {
	// Ensure single-line JSON with trailing newline
	line := append(compactJSON(data), '\n')
	_, err := s.stdin.Write(line)
	return err
}

func (p *Converter) forwardRequestAwaitResponse(s *session, data []byte, msg jsonrpcMessage) ([]byte, error) {
	idKey := msg.idKey()
	if idKey == "" {
		return nil, errors.New("request missing id")
	}
	respCh := make(chan json.RawMessage, 1)
	s.inflightMu.Lock()
	s.inflight[idKey] = respCh
	s.inflightMu.Unlock()

	defer func() {
		s.inflightMu.Lock()
		delete(s.inflight, idKey)
		s.inflightMu.Unlock()
	}()

	if err := p.forwardFireAndForget(s, data); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(60 * time.Second):
		return nil, errors.New("upstream timeout")
	}
}

func (p *Converter) readStdoutLoop(s *session) {
	scanner := bufio.NewScanner(s.stdout)
	// Increase buffer for large messages if needed
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		// Skip lines that don't look like JSON (don't start with '{' or '[')
		// This filters out startup messages, warnings, etc. from tools like npx
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		firstChar := trimmed[0]
		if firstChar != '{' && firstChar != '[' {
			// Not JSON, likely a startup message or warning - skip it silently
			continue
		}
		// parse JSON-RPC
		var msg jsonrpcMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("[%s] invalid JSON from server: %v", s.id, err)
			continue
		}
		if msg.isResponse() {
			key := msg.idKey()
			s.inflightMu.Lock()
			ch := s.inflight[key]
			s.inflightMu.Unlock()
			if ch != nil {
				// Return directly to awaiting POST handler as a JSON object
				ch <- json.RawMessage(line)
				continue
			}
		}
		// For notifications and server-initiated requests (or orphan responses), stream over SSE
		p.streamToAnySSE(s, line)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[%s] stdout scan error: %v", s.id, err)
	}
	// process ended or pipe closed
	p.stopSession(s.id)
}

func (p *Converter) streamToAnySSE(s *session, payload []byte) {
	s.serverMsgMu.Lock()
	defer s.serverMsgMu.Unlock()
	if len(s.sseConns) == 0 {
		return
	}
	s.lastEventSeq++
	idStr := fmt.Sprintf("%d", s.lastEventSeq)
	// choose first active connection
	for len(s.sseConns) > 0 {
		c := s.sseConns[0]
		if err := c.WriteEvent(payload, idStr); err != nil {
			// drop dead connection
			c.Close()
			s.sseConns = s.sseConns[1:]
			continue
		}
		break
	}
}

func (p *Converter) readStderrLoop(s *session) {
	scanner := bufio.NewScanner(s.stderr)
	for scanner.Scan() {
		// Silently discard stderr output from MCP server
		_ = scanner.Text()
	}
}

func (p *Converter) waitProcess(s *session) {
	_ = s.cmd.Wait()
	p.stopSession(s.id)
}

// HTTP/SSE transport helpers
func (p *Converter) startHTTPCommand() error {
	cmd := exec.Command("/bin/sh", "-lc", p.cmdStr)
	// For HTTP/SSE mode, we don't need to capture stdio, just run it
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	p.cmd = cmd
	// Wait for the process in background
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("HTTP command exited with error: %v", err)
		}
	}()
	return nil
}

// Utilities

func randID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func compactJSON(in []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, bytes.TrimSpace(in)); err != nil {
		// fallback to original
		return bytes.TrimSpace(in)
	}
	// ensure no embedded newlines
	return []byte(strings.ReplaceAll(buf.String(), "\n", ""))
}
