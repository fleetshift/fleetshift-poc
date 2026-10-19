FROM golang:1.26.6 AS ocm-fleet-builder

WORKDIR /src

# Copy server module manifests to cache dependencies.
COPY server/go.mod server/go.sum ./server/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd server && \
    for attempt in 1 2 3 4 5; do \
        if GODEBUG=http2client=0 go mod download; then exit 0; fi; \
        sleep "$((attempt * 5))"; \
    done; \
    exit 1

# Copy server source.
COPY server/ ./server/

# Build server binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd server && CGO_ENABLED=0 go build -o /bin/resource-manager ./cmd/resource-manager

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ocm-fleet-builder /bin/resource-manager /usr/local/bin/resource-manager

EXPOSE 50051 8085

ENTRYPOINT ["resource-manager"]
CMD ["serve", "--http-addr", ":8085", "--grpc-addr", ":50051", "--db", "/data/resource-manager.db", "--log-level", "debug"]
