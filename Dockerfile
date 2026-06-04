FROM golang:1.25 AS fleetshift-builder

WORKDIR /src

# Copy go.mod/go.sum for both modules to cache deps
# CLI has a replace directive pointing to ../fleetshift-server
COPY fleetshift-server/go.mod fleetshift-server/go.sum ./fleetshift-server/
COPY fleetshift-cli/go.mod fleetshift-cli/go.sum ./fleetshift-cli/
RUN cd fleetshift-server && go mod download && \
    cd ../fleetshift-cli && go mod download

# Copy all source (server, cli)
COPY fleetshift-server/ ./fleetshift-server/
COPY fleetshift-cli/ ./fleetshift-cli/

# Build both binaries
RUN cd fleetshift-server && CGO_ENABLED=0 go build -o /bin/fleetshift ./cmd/fleetshift
RUN cd fleetshift-cli && CGO_ENABLED=0 go build -o /bin/fleetctl ./cmd/fleetctl

FROM golang:1.25 AS hypershift-builder

WORKDIR /src

ARG HYPERSHIFT_REF=v0.1.76

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --branch "${HYPERSHIFT_REF}" --depth 1 https://github.com/openshift/hypershift.git /src/hypershift
RUN cd /src/hypershift && CGO_ENABLED=0 go build -p 2 -o /bin/hypershift .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fleetshift-builder /bin/fleetshift /usr/local/bin/fleetshift
COPY --from=fleetshift-builder /bin/fleetctl /usr/local/bin/fleetctl
COPY --from=hypershift-builder /bin/hypershift /usr/local/bin/hypershift

EXPOSE 50051 8085

ENTRYPOINT ["fleetshift"]
CMD ["serve", "--http-addr", ":8085", "--grpc-addr", ":50051", "--db", "/data/fleetshift.db", "--log-level", "debug"]
