FROM golang:1.25-alpine AS stdio-to-http-converter-builder

WORKDIR /app

# Copy local converter source
# This Dockerfile expects to be built from the parent directory with:
# docker build -f test/Dockerfile -t stdio-to-http-converter-test .
COPY converter ./converter/
COPY main.go ./
COPY go.mod ./
# Copy go.sum if it exists (for reproducible builds)
# Note: go.sum should be generated with 'go mod tidy' before building
COPY go.sum ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o stdio-to-http-converter .

FROM node:22-alpine

WORKDIR /app

# Copy the built converter binary
COPY --from=stdio-to-http-converter-builder /app/stdio-to-http-converter .
RUN chmod +x stdio-to-http-converter

EXPOSE 8080

ENTRYPOINT ["./stdio-to-http-converter"]
CMD ["--mcp_server_command=npx -y @wrtnlabs/calculator-mcp@latest", "--transport_type=stdio"]

