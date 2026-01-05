package test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	testImageName     = "stdio-to-http-converter-test"
	testContainerName = "stdio-to-http-converter-test-container"
	testPort          = "18080"
	testURL           = "http://localhost:18080/mcp"
)

func TestConverterWithMCPServer(t *testing.T) {
	ctx := context.Background()

	// Step 0: Ensure go.sum exists
	t.Log("Ensuring go.sum exists...")
	if err := ensureGoSum(); err != nil {
		t.Fatalf("Failed to ensure go.sum exists: %v", err)
	}

	// Step 1: Build Docker image
	t.Log("Building Docker image...")
	if err := buildDockerImage(ctx); err != nil {
		t.Fatalf("Failed to build Docker image: %v", err)
	}
	defer cleanupDockerImage(ctx)

	// Step 2: Start container
	t.Log("Starting container...")
	if err := startContainer(ctx); err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}
	defer func() {
		// Save logs before stopping container
		if err := saveContainerLogs(ctx, t); err != nil {
			t.Logf("Warning: Failed to save container logs: %v", err)
		}
		stopContainer(ctx)
	}()

	// Step 3: Wait for container to be ready
	t.Log("Waiting for container to be ready...")
	if err := waitForContainer(ctx, 30*time.Second); err != nil {
		t.Fatalf("Container not ready: %v", err)
	}

	// Step 4: Test MCP connection
	t.Log("Testing MCP connection...")
	if err := testMCPConnection(ctx, t); err != nil {
		t.Fatalf("MCP connection test failed: %v", err)
	}

	t.Log("All tests passed!")
}

func ensureGoSum() error {
	// Run go mod tidy to ensure go.sum exists
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = ".."
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func buildDockerImage(ctx context.Context) error {
	// Build from parent directory so Dockerfile can access source files
	// The build context is the parent directory (stdio-to-http-converter)
	// and we specify the Dockerfile path relative to that context
	cmd := exec.CommandContext(ctx, "docker", "build", "-f", "test/Dockerfile", "-t", testImageName, ".")
	cmd.Dir = ".." // Change to parent directory (stdio-to-http-converter)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func cleanupDockerImage(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "docker", "rmi", testImageName)
	cmd.Run() // Ignore errors
}

func startContainer(ctx context.Context) error {
	// Stop and remove existing container if it exists
	exec.CommandContext(ctx, "docker", "stop", testContainerName).Run()
	exec.CommandContext(ctx, "docker", "rm", testContainerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", testContainerName,
		"-p", fmt.Sprintf("%s:8080", testPort),
		testImageName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func saveContainerLogs(ctx context.Context, t *testing.T) error {
	// Get logs from container
	cmd := exec.CommandContext(ctx, "docker", "logs", testContainerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get container logs: %w", err)
	}

	// Create logs directory if it doesn't exist
	logsDir := "test_logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Save logs to file with timestamp
	timestamp := time.Now().Format("20060102-150405")
	logFile := filepath.Join(logsDir, fmt.Sprintf("%s-%s.log", testContainerName, timestamp))

	if err := os.WriteFile(logFile, output, 0644); err != nil {
		return fmt.Errorf("failed to write log file: %w", err)
	}

	t.Logf("Container logs saved to: %s", logFile)
	return nil
}

func stopContainer(ctx context.Context) {
	exec.CommandContext(ctx, "docker", "stop", testContainerName).Run()
	exec.CommandContext(ctx, "docker", "rm", testContainerName).Run()
}

func waitForContainer(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		// Check if container is running
		cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", fmt.Sprintf("name=%s", testContainerName), "--format", "{{.Names}}")
		output, err := cmd.Output()
		if err != nil || string(output) == "" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Try to connect to the HTTP endpoint
		req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			// Any response (even 405 Method Not Allowed) means the server is up
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("container did not become ready within %v", timeout)
}

func testMCPConnection(ctx context.Context, t *testing.T) error {
	// Create MCP client
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "stdio-to-http-converter-test",
		Version: "1.0.0",
	}, nil)

	// Create transport (using http transport type)
	transport := &mcp.StreamableClientTransport{Endpoint: testURL}

	// Connect to the server
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to MCP server: %w", err)
	}
	defer session.Close()

	// Get initialize result
	initializeResult := session.InitializeResult()
	if initializeResult == nil {
		return fmt.Errorf("initialize result is nil")
	}

	t.Logf("Connected successfully! Server: %s, Protocol: %s",
		initializeResult.ServerInfo.Name,
		initializeResult.ProtocolVersion)

	// Check if tools are available
	if initializeResult.Capabilities.Tools == nil {
		return fmt.Errorf("server does not support tools")
	}

	// List tools
	tools, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		return fmt.Errorf("failed to list tools: %w", err)
	}

	if tools == nil {
		return fmt.Errorf("tools list is nil")
	}

	t.Logf("Successfully retrieved %d tools", len(tools.Tools))
	for _, tool := range tools.Tools {
		t.Logf("  - Tool: %s - %s", tool.Name, tool.Description)
	}

	if len(tools.Tools) == 0 {
		return fmt.Errorf("expected at least one tool, got 0")
	}

	return nil
}
