# FleetShift Deployment

Two deployment targets: **local development** (podman containers) and **OpenShift Keycloak** (production-like OIDC provider).

---

## Local Development (Podman)

Deploy the full FleetShift stack locally using podman compose.

> **macOS:** Podman only forwards IPv6 loopback. Add this one-time `/etc/hosts` entry or Keycloak will be unreachable:
> ```bash
> echo "::1 keycloak" | sudo tee -a /etc/hosts
> ```

### Prerequisites

- **podman** installed and running
- **docker-compose** installed (`podman-compose` is not compatible)
- **jq** installed
- **kind** installed (for local cluster provisioning)
- `.env` file — copy from `.env.template`

### Quick Start

```bash
task deploy:up                                   # demo mode (sqlite + local keycloak)
task deploy:up DEPLOY_MODE=prod                  # prod mode (postgres + external OIDC)
task deploy:up DB=postgres AUTH=local            # per-axis override
task deploy:dev                                  # dev mode (source mounts + hot-reload)
```

### Modes

| Mode | DB | Auth | Use Case |
|---|---|---|---|
| `demo` (default) | SQLite | Local Keycloak | Local dev, demos |
| `prod` | PostgreSQL | External OIDC | Production-like |

Override axes independently with `DB=sqlite|postgres` and `AUTH=local|external`.

### Common Tasks

```bash
task deploy:up                    # start stack
task deploy:dev                   # start with source mounts + hot-reload
task deploy:down                  # stop, preserve data
task deploy:clean                 # stop + wipe all data
task deploy:rebuild               # stop, rebuild images, restart
task deploy:logs                  # tail all logs
task deploy:logs:fleetshift-server # tail specific service
task deploy:status                # show running containers
task deploy:cli-setup             # configure fleetctl for local stack
task deploy:reset-keycloak        # wipe keycloak state, re-import realm
```

### Dev Mode

`task deploy:dev` builds frontend assets in a container (using `Dockerfile.web` from the UI repo) and starts the Go backend serving everything on `:8085`. No host Node.js or npm required. Requires `UI_DIR` in `.env` pointing to the `fleetshift-user-interface` repo.

After changing Go code, run `task deploy:rebuild` to rebuild and restart. After changing frontend code, run `task deploy:clean` then `task deploy:dev` to rebuild the web assets.

### Configuration

Copy `.env.template` to `.env`. Key settings:

| Variable | Default | Description |
|---|---|---|
| `DEPLOY_MODE` | `demo` | `demo` or `prod` |
| `KC_HOSTNAME` | `keycloak` | Keycloak hostname |
| `KC_HTTP_PORT` | `8180` | Keycloak HTTP port |
| `POSTGRES_PASSWORD` | `changeme` | PostgreSQL password (when `DB=postgres`) |
| `UI_DIR` | `../fleetshift-user-interface` | Path to UI repo (for dev mode) |
| `OIDC_ISSUER_URL` | — | Required when `AUTH=external` |
| `OIDC_CONSOLE_CLIENT_SECRET` | — | Required when `AUTH=external` |

To auto-create a personal Keycloak user on startup (`AUTH=local` only), set `DEV_USER_USERNAME`, `DEV_USER_PASSWORD`, `DEV_USER_GITHUB`, and `DEV_USER_ROLES` in `.env`.

### Services

| Service | URL | Description |
|---|---|---|
| FleetShift (UI + API) | http://localhost:8085 | Web interface + HTTP gateway (single port) |
| FleetShift gRPC | localhost:50051 | gRPC (used by fleetctl) |
| Keycloak | http://keycloak:8180 | OIDC provider (requires `/etc/hosts` on macOS) |

### Authentication

Passwords are generated on each `task deploy:up` and printed to the console.

| Username | Role |
|---|---|
| `ops` | Operations — manages clusters |
| `dev` | Developer — manages applications |

Admin console: `http://keycloak:8180/auth/admin` (user: `admin`, password from startup output).

---

## OpenShift Keycloak

Production-like Keycloak on OpenShift for OIDC integration testing and demos. Deploys RHBK (operator-managed), PostgreSQL, TLS via cert-manager, and the FleetShift realm.

Everything runs in the `keycloak-prod` namespace.

### Prerequisites

- OCP cluster with internet-facing ingress
- `oc` CLI, logged in with cluster-admin privileges
- `jq` and `openssl` in PATH

### Deploy

```bash
task kc:deploy ACME_EMAIL=you@example.com
task kc:deploy ACME_EMAIL=you@example.com BASE_DOMAIN=example.com
task kc:deploy ACME_EMAIL=you@example.com FRESH_CERT=true
```

The deploy is idempotent — safe to re-run. On completion, it prints the Keycloak URL, admin credentials, and test user passwords.

### Add Cluster Console Redirect URIs

Before provisioning AWS clusters with OIDC console access, register each cluster's redirect URI. Keycloak does not support wildcard subdomain patterns, so each cluster needs an explicit entry:

```bash
task kc:add-base-domain BASE_DOMAIN=example.com CLUSTER_NAME=my-cluster
```

### Retrieve Console Client Secret

```bash
oc get secret ocp-console-client-secret -n keycloak-prod \
  -o jsonpath='{.data.clientSecret}' | base64 -d
```

Set this as `OIDC_CONSOLE_CLIENT_SECRET` in `.env` for the local podman stack, or `OCP_CONSOLE_CLIENT_SECRET` for the fleetshift server.

### Add Users

```bash
# OpenShift Keycloak (auto-discovers credentials via oc):
task kc:add-user USERNAME=you@example.com PASSWORD=mypass GITHUB=ghuser ROLES=ops,dev

# Local podman Keycloak (pass admin password from deploy:up output):
task kc:add-user USERNAME=you@example.com PASSWORD=mypass GITHUB=ghuser ROLES=ops,dev ADMIN_PASSWORD=<from-output>
```

### Teardown

```bash
task kc:teardown
```

Prompts for confirmation. Optionally uninstalls cert-manager and RHBK operators.

### Accessing Keycloak

Find the URL after deployment:

```bash
oc get route -n keycloak-prod -o jsonpath='{.items[0].spec.host}'
```

- **Admin console:** `https://<host>/admin`
- **Realm account:** `https://<host>/realms/fleetshift/account`
- **OIDC discovery:** `https://<host>/realms/fleetshift/.well-known/openid-configuration`

Admin credentials:

```bash
oc get secret keycloak-initial-admin -n keycloak-prod \
    -o jsonpath='{.data.password}' | base64 -d
```

### Clients

| Client ID | Type | Purpose |
|---|---|---|
| `fleetshift-ui` | Public | UI authentication |
| `fleetshift-cli` | Public | CLI authentication + device auth |
| `fleetshift-signing` | Public | Key enrollment / attestation |
| `ocp-console` | Confidential | OCP console OIDC login |

### Realm Users

| Username | Roles | Password |
|---|---|---|
| `ops` | ops | Generated at deploy time |
| `dev` | dev | Generated at deploy time |
| `admin` | ops, dev | Generated at deploy time |

Test user passwords are printed once during deploy and are not stored in a Kubernetes secret.
