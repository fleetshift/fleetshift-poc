# Nx Remote Cache

Remote cache for Nx task outputs backed by MinIO and a lightweight Go proxy (`nx-cache-proxy`). Speeds up local builds, container image builds, and CI by sharing cached compilation results across machines.

## Architecture

```text
  Nx CLI ──(HTTP)──▶ nx-cache-proxy ──(S3)──▶ MinIO
                       :8080                   :9000
```

- **nx-cache-proxy** implements the [Nx custom remote cache OpenAPI spec](../deploy/minio-nx-cache/cache-openapi.json) — `GET /v1/cache/{hash}` and `PUT /v1/cache/{hash}` with bearer token auth.
- **MinIO** provides S3-compatible object storage for the cache entries.
- Cache entries expire after a configurable TTL (default 24h). MinIO lifecycle rules garbage-collect expired objects at the storage layer (3 days).

### Security — CVE-2025-36852 (CREEP) mitigation

The proxy enforces **immutable cache entries**: `PUT` returns `409 Conflict` if a hash already exists and hasn't expired. This prevents cache poisoning via modified CI workflows — a key vector in the CREEP vulnerability that affected `@nx/s3-cache`.

Two bearer tokens control access:
- **Read token** — can only `GET`. Intended for CI and container builds.
- **Write token** — can `GET` and `PUT`. Intended for developers.

## Quick start — local (podman)

1. **Add to `.env`:**

   ```sh
   NX_SELF_HOSTED_REMOTE_CACHE_SERVER=http://localhost:8420
   NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=local-write-token
   ```

2. **Start MinIO + proxy:**

   ```sh
   task podman:up -- -f overrides/nx-cache.yaml
   ```

   This starts MinIO on `:9000` (console `:9001`), creates the `nx-cache` bucket, and runs the proxy on `:8420`.

3. **Verify:**

   ```sh
   # Health check
   curl http://localhost:8420/healthz

   # Run a build and check cache hit rate
   npx nx run common:build
   npx nx reset
   npx nx run common:build   # should show "remote cache hit"
   ```

4. **MinIO console** is available at `http://localhost:9001` (credentials: `minio-admin` / `minio-local-password`).

## Container image builds

The `Dockerfile.web` accepts build args for remote cache. When building via the `dev.yaml` compose override, these are passed from `.env` automatically:

```yaml
# deploy/podman/overrides/dev.yaml
web-builder:
  build:
    args:
      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: "${NX_SELF_HOSTED_REMOTE_CACHE_SERVER:-}"
      NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN: "${NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN:-}"
```

The Dockerfile translates `localhost` to `host.containers.internal` so the build container can reach the host-mapped proxy. When using a real URL (e.g., OCP Route), no translation occurs.

For read-only container builds, set the read token in `.env`:

```sh
NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=local-read-token
```

Standalone build:

```sh
podman build \
  --build-arg NX_SELF_HOSTED_REMOTE_CACHE_SERVER=http://host.containers.internal:8420 \
  --build-arg NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=local-read-token \
  -f Dockerfile.web .
```

## OpenShift deployment

MinIO and the cache proxy run on the Keycloak OCP cluster. All `task minio:*` commands require an active `oc` session on that cluster:

```sh
# 1. Log in to the Keycloak cluster and set KC_CLUSTER_API in .env
oc login <keycloak-cluster-api-url>

# 2. Deploy (generates random credentials + bearer tokens, stores in OCP secrets)
task minio:deploy

# 3. Check status
task minio:status

# 4. Get connection info — prints the NX_SELF_HOSTED_REMOTE_CACHE_* values to put in .env
task minio:credentials

# 5. Tear down (interactive)
task minio:teardown
```

To retrieve credentials later (e.g., on a new machine), log in to the cluster and run `task minio:credentials` — it reads from the existing OCP secrets.

## Configuration reference

### nx-cache-proxy environment variables

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | *(required)* | MinIO S3 endpoint (e.g., `minio:9000`) |
| `MINIO_ACCESS_KEY` | *(required)* | MinIO access key |
| `MINIO_SECRET_KEY` | *(required)* | MinIO secret key |
| `MINIO_BUCKET` | `nx-cache` | Bucket name |
| `MINIO_SECURE` | `false` | Use TLS for MinIO connection |
| `NX_CACHE_READ_TOKEN` | *(required)* | Bearer token for read access |
| `NX_CACHE_WRITE_TOKEN` | *(required)* | Bearer token for read+write access |
| `CACHE_TTL` | `24h` | Cache entry time-to-live |
| `PORT` | `8080` | HTTP listen port |
| `LOG_LEVEL` | `info` | `info` or `debug` |

### Nx client environment variables

| Variable | Description |
|---|---|
| `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` | Proxy URL (e.g., `http://localhost:8420`) |
| `NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN` | Bearer token (read or write) |

Nx loads these from `.env` automatically.

## Source code

- Proxy: [`sdk/nx-cache-proxy/`](../sdk/nx-cache-proxy/)
- Dockerfile: [`Dockerfile.nx-cache`](../Dockerfile.nx-cache)
- OCP manifests: [`deploy/minio-nx-cache/manifests/`](../deploy/minio-nx-cache/manifests/)
- Compose override: [`deploy/podman/overrides/nx-cache.yaml`](../deploy/podman/overrides/nx-cache.yaml)
- OpenAPI spec: [`deploy/minio-nx-cache/cache-openapi.json`](../deploy/minio-nx-cache/cache-openapi.json)
