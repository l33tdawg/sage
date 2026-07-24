# SAGE — Persistent consensus-validated memory for AI agents
# Server:
#   docker run -d --name sage -p 8080:8080 -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest
# MCP stdio bridge (run inside that same server container):
#   docker exec -i -e SAGE_PROVIDER=claude-code -e SAGE_PROJECT=my-project \
#     -e SAGE_IDENTITY_PATH=/root/.sage/agents/claude-code-my-project/agent.key \
#     sage /usr/local/bin/sage-gui mcp
FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
# The root module replaces CometBFT with SAGE's audited local source subset.
# Seed that module's metadata before dependency download; COPY . supplies the
# complete source tree for the build layer below.
COPY third_party/cometbft/go.mod third_party/cometbft/go.sum ./third_party/cometbft/
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /sage-gui ./cmd/sage-gui

# Runtime is glibc (Debian), not Alpine/musl: SAGE's managed reranker
# (internal/rerankd) downloads and execs a prebuilt llama.cpp `llama-server`
# engine, and llama.cpp publishes only glibc/Ubuntu Linux builds — no musl or
# fully-static asset (checked b9870 and latest). That binary's ELF interpreter
# is /lib64/ld-linux-x86-64.so.2 and it NEEDs libstdc++.so.6, libgomp.so.1,
# libssl.so.3 and libcrypto.so.3, none of which exist on alpine, so on the old
# Alpine image the engine exited instantly ("llama-server exited during
# startup") and the managed reranker was non-functional in the container.
# sage-gui itself is CGO_ENABLED=0 static and runs unchanged on either base.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates libstdc++6 libgomp1 libssl3 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /sage-gui /usr/local/bin/sage-gui

ENV SAGE_HOME=/root/.sage
ENV REST_ADDR=0.0.0.0:8080
EXPOSE 8080

LABEL org.opencontainers.image.source="https://github.com/l33tdawg/sage"
LABEL org.opencontainers.image.description="SAGE — Persistent consensus-validated memory for AI agents"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL io.modelcontextprotocol.server.name="io.github.l33tdawg/sage"

ENTRYPOINT ["sage-gui"]
CMD ["serve"]
