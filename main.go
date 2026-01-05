package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/agentflare/af-cloud/stdio-to-http-converter/converter"
)

func main() {

	var (
		mcp_server_command = flag.String("mcp_server_command", "", "Command to run the MCP server (required)")
		transport_type     = flag.String("transport_type", "stdio", "Transport type: stdio, http, or sse")
		internal_port      = flag.String("internal_port", "8080", "Internal port to forward requests to")
	)
	flag.Parse()

	if *mcp_server_command == "" {
		log.Fatal("Command is required. Use -mcp_server_command flag to specify the command to run.")
	}

	if *transport_type != "stdio" && *transport_type != "http" && *transport_type != "sse" {
		log.Fatal("transport_type must be one of: stdio, http, sse")
	}

	endpoint := os.Getenv("MCP_ENDPOINT")
	if endpoint == "" {
		endpoint = "/mcp"
	}

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	addr = ":" + addr

	proxy := converter.NewConverter(*mcp_server_command, *transport_type, *internal_port)
	mux := http.NewServeMux()
	mux.HandleFunc(endpoint, proxy.HandleEndpoint)

	// Wrap with CORS - this must wrap everything to handle preflight
	// CORS middleware handles OPTIONS requests before they reach the mux
	handler := withCORS(mux)

	log.Printf("mcp-http-proxy listening on %s at endpoint %s (transport: %s)", addr, endpoint, *transport_type)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// withCORS adds permissive CORS for browser clients and handles preflight.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always allow all origins
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Handle preflight OPTIONS requests - must return immediately
		if r.Method == http.MethodOptions {
			reqHeaders := r.Header.Get("Access-Control-Request-Headers")
			if reqHeaders == "" {
				reqHeaders = "Content-Type, Accept, Mcp-Session-Id, Authorization, Cache-Control, X-Requested-With"
			}
			// Set all required CORS headers for preflight
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
			w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours
			w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id, Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Expose custom headers clients need to read for actual requests
		w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id, Content-Type")

		// Wrap the ResponseWriter to ensure CORS headers are always set
		corsW := newCorsResponseWriter(w)
		next.ServeHTTP(corsW, r)
	})
}

// corsResponseWriter wraps http.ResponseWriter to ensure CORS headers are set
type corsResponseWriter struct {
	http.ResponseWriter
	flusher        http.Flusher
	headersWritten bool
}

func newCorsResponseWriter(w http.ResponseWriter) *corsResponseWriter {
	cw := &corsResponseWriter{ResponseWriter: w}
	if f, ok := w.(http.Flusher); ok {
		cw.flusher = f
	}
	return cw
}

func (w *corsResponseWriter) Flush() {
	if w.flusher != nil {
		w.flusher.Flush()
	}
}

func (w *corsResponseWriter) WriteHeader(status int) {
	if !w.headersWritten {
		// Ensure CORS headers are set before writing status
		// Double-check they're still there (in case handler cleared them)
		if w.Header().Get("Access-Control-Allow-Origin") == "" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		// Ensure expose headers are set
		if w.Header().Get("Access-Control-Expose-Headers") == "" {
			w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id, Content-Type")
		}
		w.headersWritten = true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *corsResponseWriter) Write(b []byte) (int, error) {
	if !w.headersWritten {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
