# Converter Integration Test

This test verifies that the stdio-to-http-converter works correctly with an MCP server.

## Overview

The test:

1. Builds a Docker image containing the converter and a test MCP server (calculator)
2. Runs the container locally
3. Connects to the container using an MCP client
4. Verifies it can connect and retrieve the tools list

## Prerequisites

- Docker installed and running
- Go 1.23.4 or later
- Network access to pull the calculator MCP server from npm

## Running the Test

From the `stdio-to-http-converter` directory:

```bash
go test -v ./test
```

Or from the test directory:

```bash
cd test
go test -v .
```

## What It Tests

- Docker image builds successfully with local converter source
- Container starts and exposes the converter on port 18080
- MCP client can connect to the converter endpoint
- Initialize handshake completes successfully
- Tools list can be retrieved from the MCP server

## Test Details

- **Test Image**: `stdio-to-http-converter-test`
- **Test Container**: `stdio-to-http-converter-test-container`
- **Test Port**: `18080` (mapped to container port 8080)
- **Test Endpoint**: `http://localhost:18080/mcp`
- **MCP Server**: `@wrtnlabs/calculator-mcp@latest` (via npx)

The test automatically cleans up the container and image after completion (or on failure).
