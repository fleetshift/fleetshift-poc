FROM golang:1.25 AS builder

WORKDIR /src

# Copy go.mod/go.sum for all modules to cache deps
COPY fleetshift-server/go.mod fleetshift-server/go.sum ./fleetshift-server/
COPY fleetshift-cli/go.mod fleetshift-cli/go.sum ./fleetshift-cli/
COPY addons/go.mod addons/go.sum ./addons/
COPY gen/go.mod gen/go.sum ./gen/
RUN cd fleetshift-server && go mod download && \
    cd ../fleetshift-cli && go mod download && \
    cd ../addons && go mod download

# Copy all source
COPY fleetshift-server/ ./fleetshift-server/
COPY fleetshift-cli/ ./fleetshift-cli/
COPY addons/ ./addons/
COPY gen/ ./gen/
COPY proto/ ./proto/

# Build binaries
RUN cd fleetshift-server && CGO_ENABLED=0 go build -o /bin/fleetshift ./cmd/fleetshift
RUN cd fleetshift-cli && CGO_ENABLED=0 go build -o /bin/fleetctl ./cmd/fleetctl
RUN cd addons && CGO_ENABLED=0 go build -o /bin/monitoring-platform ./monitoring/cmd/platform/

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl docker.io \
    && rm -rf /var/lib/apt/lists/*

# Install kind
RUN ARCH=$(dpkg --print-architecture) && \
    curl -Lo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v0.31.0/kind-linux-${ARCH}" && \
    chmod +x /usr/local/bin/kind

# Install kubectl
RUN ARCH=$(dpkg --print-architecture) && \
    curl -Lo /usr/local/bin/kubectl "https://dl.k8s.io/release/v1.33.0/bin/linux/${ARCH}/kubectl" && \
    chmod +x /usr/local/bin/kubectl

COPY --from=builder /bin/fleetshift /usr/local/bin/fleetshift
COPY --from=builder /bin/fleetctl /usr/local/bin/fleetctl
COPY --from=builder /bin/monitoring-platform /usr/local/bin/monitoring-platform

EXPOSE 50051 8085

ENTRYPOINT ["fleetshift"]
CMD ["serve", "--http-addr", ":8085", "--grpc-addr", ":50051", "--db", "/data/fleetshift.db", "--log-level", "debug"]
