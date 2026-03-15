# SAGE MCP Server — minimal container for MCP registry distribution
# Usage: docker run -i ghcr.io/l33tdawg/sage:latest
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=4.5.6
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /sage-gui ./cmd/sage-gui

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /sage-gui /usr/local/bin/sage-gui

LABEL org.opencontainers.image.source="https://github.com/l33tdawg/sage"
LABEL org.opencontainers.image.description="SAGE — Persistent consensus-validated memory for AI agents"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL io.modelcontextprotocol.server.name="io.github.l33tdawg/sage"

ENTRYPOINT ["sage-gui", "mcp"]
