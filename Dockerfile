# Prebuilt multiarch hypershift CLI (openshift/hypershift v0.1.79).
# Quay tags this image by git SHA, not semver: 65839bbab… == v0.1.79.
# Override with --build-arg HYPERSHIFT_IMAGE=... when needed.
ARG HYPERSHIFT_IMAGE=quay.io/acm-d/rhtap-hypershift-operator:65839bbab12247d630a498e487af6f30d7788620

FROM golang:1.25 AS fleetshift-builder

WORKDIR /src

# Copy go.mod/go.sum for both modules to cache deps
# CLI has a replace directive pointing to ../server
COPY server/go.mod server/go.sum ./server/
COPY cli/go.mod cli/go.sum ./cli/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd server && go mod download && \
    cd ../cli && go mod download

# Copy all source (server, cli)
COPY server/ ./server/
COPY cli/ ./cli/

# Build both binaries
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd server && CGO_ENABLED=0 go build -o /bin/fleetshift ./cmd/fleetshift
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd cli && CGO_ENABLED=0 go build -o /bin/fleetctl ./cmd/fleetctl

FROM golang:1.26 AS dex-builder

WORKDIR /src

COPY poc/dex-idp/go.mod poc/dex-idp/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY poc/dex-idp/ ./
# CGO required for go-sqlite3
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /bin/dex-idp ./cmd/dex-idp

FROM ${HYPERSHIFT_IMAGE} AS hypershift

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fleetshift-builder /bin/fleetshift /usr/local/bin/fleetshift
COPY --from=fleetshift-builder /bin/fleetctl /usr/local/bin/fleetctl
COPY --from=dex-builder /bin/dex-idp /usr/local/bin/dex-idp
COPY --from=hypershift /usr/bin/hypershift /usr/local/bin/hypershift
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

EXPOSE 50051 5556 8085

ENTRYPOINT ["entrypoint.sh"]
CMD ["serve", "--http-addr", ":8085", "--grpc-addr", ":50051", "--db", "/data/fleetshift.db", "--log-level", "debug"]
