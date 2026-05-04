# OpenShift Keycloak

Production-like Keycloak on OpenShift for OIDC integration testing and demos. Deploys RHBK (operator-managed), PostgreSQL, TLS via cert-manager, and the FleetShift realm.

Everything runs in the `keycloak-prod` namespace.

## Prerequisites

- OCP cluster with internet-facing ingress
- `oc` CLI, logged in with cluster-admin privileges
- `jq` and `openssl` in PATH

## Deploy

```bash
task kc:deploy ACME_EMAIL=you@example.com
task kc:deploy ACME_EMAIL=you@example.com BASE_DOMAIN=example.com
task kc:deploy ACME_EMAIL=you@example.com FRESH_CERT=true
```

The deploy is idempotent — safe to re-run. On completion, it prints the Keycloak URL, admin credentials, and test user passwords.

## Add Cluster Console Redirect URIs

Before provisioning AWS clusters with OIDC console access, register each cluster's redirect URI. Keycloak does not support wildcard subdomain patterns, so each cluster needs an explicit entry:

```bash
task kc:add-base-domain BASE_DOMAIN=example.com CLUSTER_NAME=my-cluster
```

## Retrieve Console Client Secret

```bash
oc get secret ocp-console-client-secret -n keycloak-prod \
  -o jsonpath='{.data.clientSecret}' | base64 -d
```

Set this as `OIDC_CONSOLE_CLIENT_SECRET` in `.env` for the local podman stack, or `OCP_CONSOLE_CLIENT_SECRET` for the fleetshift server.

## Add Users

```bash
# OpenShift Keycloak (auto-discovers credentials via oc):
task kc:add-user USERNAME=you@example.com PASSWORD=mypass GITHUB=ghuser ROLES=ops,dev

# Local podman Keycloak (pass admin password from podman:up output):
task kc:add-user USERNAME=you@example.com PASSWORD=mypass GITHUB=ghuser ROLES=ops,dev ADMIN_PASSWORD=<from-output>
```

## Teardown

```bash
task kc:teardown
```

Prompts for confirmation. Optionally uninstalls cert-manager and RHBK operators.

## Accessing Keycloak

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

## Clients

| Client ID | Type | Purpose |
|---|---|---|
| `fleetshift` | Confidential (no flows) | Resource server — audience target and role container |
| `fleetshift-ui` | Public | UI authentication |
| `fleetshift-cli` | Public | CLI authentication + device auth |
| `fleetshift-signing` | Public | Key enrollment / attestation |
| `ocp-console` | Confidential | OCP console OIDC login |

## Realm Users

| Username | Roles | Password |
|---|---|---|
| `ops` | ops | Generated at deploy time |
| `dev` | dev | Generated at deploy time |
| `admin` | ops, dev | Generated at deploy time |

Test user passwords are printed once during deploy and are not stored in a Kubernetes secret.
