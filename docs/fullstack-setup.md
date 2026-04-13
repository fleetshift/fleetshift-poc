# Full-stack local development (Docker Compose)

One command starts the full stack: Keycloak, FleetShift server, mock API server,
mock UI plugins, and the GUI webpack dev server. Auth is bootstrapped
automatically — no manual steps needed.

## Prerequisites

- Docker with Compose v2
- Add `keycloak` to your hosts file so the browser can reach it:
  ```bash
  # macOS / Linux
  echo '127.0.0.1 keycloak' | sudo tee -a /etc/hosts
  ```
- Clone [fleetshift-user-interface](https://github.com/fleetshift/fleetshift-user-interface) next to this repo and set `UI_DIR` in `.env` (default: `../fleetshift-user-interface`)

## Start the stack

```bash
docker compose up --build
```

| Service        | URL                          | Credentials   |
|----------------|------------------------------|---------------|
| GUI            | http://localhost:3000         |               |
| Mock API       | http://localhost:4000         |               |
| Mock plugins   | http://localhost:8001         |               |
| FleetShift API | http://localhost:8085         |               |
| gRPC           | localhost:50051              |               |
| Keycloak       | http://keycloak:8180         | admin / admin |

The browser accesses Keycloak via HTTP on port 8180. HTTPS (port 8443) is used
internally by kube-apiserver in kind clusters — the CA cert is auto-generated
and mounted automatically.

## Create a kind cluster

```bash
docker compose exec fleetshift fleetctl deployment create my-cluster \
  --target kind-local \
  --manifest '{"name":"my-cluster"}'
```

## View logs

```bash
docker compose logs -f fleetshift    # server logs
docker compose logs -f gui           # GUI webpack dev server
docker compose logs -f mock-servers  # mock API
```

## Tear down

```bash
docker compose down -v               # -v removes volumes (Keycloak data, certs)
```

## SQLite database

The FleetShift server stores data in `./data/fleetshift.db` (bind-mounted).
Delete `./data/` to reset state.

## Architecture: HTTP vs HTTPS Keycloak

Keycloak serves on two ports:

- **HTTP 8180** — used by the browser (GUI login) and the mock API server (JWT
  verification). Avoids TLS cert issues in the browser.
- **HTTPS 8443** — used by kube-apiserver in kind clusters (`--oidc-issuer-url`
  requires HTTPS). Self-signed CA cert is auto-generated and mounted into kind
  nodes via `--oidc-ca-file`.

The `OIDC_HTTPS_PORT` env var on the FleetShift server controls this: when set,
the kind agent rewrites `http://keycloak:8180/...` to `https://keycloak:8443/...`
before injecting it into the kubeadm config. Unset it to disable the rewrite
(cluster creation will fail with "URL scheme must be https").

## Environment variables

| Variable                          | Service    | Purpose |
|-----------------------------------|------------|---------|
| `UI_DIR`                          | compose    | Path to the fleetshift-user-interface repo |
| `CONTAINER_HOST`                  | fleetshift | Rewrite localhost OIDC URLs for containers (e.g. `keycloak`) |
| `OIDC_HTTPS_PORT`                | fleetshift | Upgrade HTTP OIDC URLs to HTTPS on this port (e.g. `8443`) |
| `KIND_EXPERIMENTAL_DOCKER_NETWORK`| fleetshift | Docker network for kind clusters |
| `API_PROXY_TARGET`               | gui        | Backend URL for `/api` proxy |
| `KEYCLOAK_URL`                   | mock-servers | Keycloak base URL for JWKS |
| `MODE`                           | mock-servers | `live` for real K8s clusters, `mock` for seed data |

## Local development (without Docker)

### Build

```bash
make build          # builds bin/fleetshift and bin/fleetctl
```

### Run the server

```bash
./bin/fleetshift serve --http-addr :8085 --log-level debug
```

For kind cluster creation outside Docker, set `CONTAINER_HOST` and
`OIDC_HTTPS_PORT` so the OIDC issuer is reachable from kind containers:

```bash
CONTAINER_HOST=host.docker.internal \
OIDC_HTTPS_PORT=8443 \
./bin/fleetshift serve --http-addr :8085 --oidc-ca-file keycloak/certs/ca.crt --log-level debug
```

### Run tests

```bash
make test
```

### Generate protobuf code

```bash
make generate       # requires buf CLI
```

## Keycloak realm

The realm config (`keycloak/fleetshift-realm.json`) bootstraps:

- **Realm**: `fleetshift`
- **Clients**: `fleetshift-ui` (public, browser), `fleetshift-cli` (public, device flow)
- **Users**: `ops` / `dev` / `admin` (password: `test`)
- **Roles**: `ops`, `dev`

### TLS certificates

Auto-generated on first `docker compose up` by the `keycloak-certs` init
container into a Docker volume. To access the Keycloak admin console
(`https://keycloak:8443`), accept the self-signed certificate warning in
your browser.
