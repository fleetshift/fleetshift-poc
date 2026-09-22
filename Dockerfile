# Prebuilt multiarch hypershift CLI (openshift/hypershift v0.1.79).
# Quay tags this image by git SHA, not semver: 65839bbab… == v0.1.79.
# Override with --build-arg HYPERSHIFT_IMAGE=... when needed.
ARG HYPERSHIFT_IMAGE=quay.io/acm-d/rhtap-hypershift-operator:65839bbab12247d630a498e487af6f30d7788620

FROM golang:1.26.6 AS fleetshift-builder

WORKDIR /src

# Copy server module manifests to cache dependencies.
COPY server/go.mod server/go.sum ./server/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd server && go mod download

# Copy server source.
COPY server/ ./server/

# Build server binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd server && CGO_ENABLED=0 go build -o /bin/fleetshift ./cmd/fleetshift

FROM ${HYPERSHIFT_IMAGE} AS hypershift

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fleetshift-builder /bin/fleetshift /usr/local/bin/fleetshift
COPY --from=hypershift /usr/bin/hypershift /usr/local/bin/hypershift

EXPOSE 50051 8085

ENTRYPOINT ["fleetshift"]
CMD ["serve", "--http-addr", ":8085", "--grpc-addr", ":50051", "--db", "/data/fleetshift.db", "--log-level", "debug"]
