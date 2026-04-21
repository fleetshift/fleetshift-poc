# Keycloak Prod-Like Deployment

Scripted deployment of a production-like Keycloak instance on OpenShift 4.21 for OIDC integration testing and stakeholder demos.

## What Gets Deployed

- **Red Hat Build of Keycloak (RHBK)** — operator-managed, single replica
- **PostgreSQL 16** — StatefulSet with 5Gi persistent storage
- **TLS** — Let's Encrypt certificate via cert-manager (optional, falls back to cluster wildcard)
- **FleetShift realm** — pre-configured with OIDC clients, roles, and test users

Everything runs in the `keycloak-prod` namespace.

## Prerequisites

- OCP 4.21 cluster with internet-facing ingress
- `oc` CLI, logged in with cluster-admin privileges
- `jq` and `openssl` available in PATH

## Deploy

```bash
# With Let's Encrypt TLS (recommended for demos):
./deploy.sh --acme-email you@example.com

# Without Let's Encrypt (uses self-signed cert):
./deploy.sh

# With base domain for OCP console OIDC (required for AWS cluster provisioning):
./deploy.sh --acme-email you@example.com --base-domain aws-acm-cluster-virt.devcluster.openshift.com
```

The script is idempotent — safe to re-run if something fails partway through. Existing secrets are preserved on re-run.

### Adding console redirect URIs for OCP clusters

Before provisioning AWS clusters with OIDC console access, register each cluster's console redirect URI in Keycloak. Keycloak does **not** support wildcard subdomain patterns (e.g. `apps.*.example.com`), so each cluster needs an explicit entry:

```bash
# Add a cluster's console redirect URI (idempotent, can be run anytime after deploy)
./add-base-domain.sh --base-domain aws-acm-cluster-virt.devcluster.openshift.com --cluster-name my-cluster
```

**This is a prerequisite for provisioning AWS clusters.** Without it, the OCP console cannot authenticate users via OIDC (Keycloak rejects the redirect_uri).

### Retrieving the console client secret

The `ocp-console` client secret is generated during deployment and stored in a Kubernetes secret:

```bash
oc get secret ocp-console-client-secret -n keycloak-prod \
  -o jsonpath='{.data.clientSecret}' | base64 -d
```

Set this as `E2E_CONSOLE_CLIENT_SECRET` in your `e2e/.env` file or `OCP_CONSOLE_CLIENT_SECRET` environment variable for the fleetshift server.

On completion, the script prints the Keycloak URL, admin credentials, and test user credentials.

## Accessing Keycloak

### Find the URL

The URL is printed at the end of `deploy.sh`. To find it later:

```bash
oc get route -n keycloak-prod -o jsonpath='{.items[0].spec.host}'
```

The URL follows the pattern: `https://keycloak-keycloak-prod.apps.<cluster-domain>`

### Admin Console

Go to `https://<keycloak-host>/admin` and log in with the admin credentials.

To retrieve the admin password after deployment:

```bash
oc get secret keycloak-initial-admin -n keycloak-prod \
    -o jsonpath='{.data.username}' | base64 -d; echo
oc get secret keycloak-initial-admin -n keycloak-prod \
    -o jsonpath='{.data.password}' | base64 -d; echo
```

### Log In as a Test User

Go to `https://<keycloak-host>/realms/fleetshift/account` to log in as any of the test users (`ops`, `dev`, `admin`).

Test user passwords are generated at deploy time and printed once in the deploy output. They are set via the `KeycloakRealmImport` and are **not stored in a Kubernetes secret** — if you lose them, reset passwords through the admin console.

### Add Your Own User

To add a personal user for dev testing:

```bash
# From the repo root:
deploy/podman/scripts/add-user.sh --username mshort@redhat.com --password mypass --github mshort55 --roles ops,dev
```

This creates (or updates) a user with the specified credentials, GitHub username, and realm roles. The script is idempotent — re-running it updates the existing user's password and attributes. When run without `--admin-password`, it auto-discovers credentials from OpenShift.

All flags can also be set via environment variables:

```bash
KC_NEW_USERNAME=mshort@redhat.com KC_NEW_PASSWORD=mypass KC_NEW_GITHUB=mshort55 KC_NEW_ROLES=ops,dev deploy/podman/scripts/add-user.sh
```

### OIDC Endpoints

For application integration, the OIDC discovery URL is:

```
https://<keycloak-host>/realms/fleetshift/.well-known/openid-configuration
```

Key endpoints:

| Endpoint | URL |
|----------|-----|
| Authorization | `https://<keycloak-host>/realms/fleetshift/protocol/openid-connect/auth` |
| Token | `https://<keycloak-host>/realms/fleetshift/protocol/openid-connect/token` |
| UserInfo | `https://<keycloak-host>/realms/fleetshift/protocol/openid-connect/userinfo` |

## Teardown

```bash
./teardown.sh
```

Prompts for confirmation before deleting anything. Optionally uninstalls the cert-manager and RHBK operators.

## FleetShift Realm

The realm config at `realm/fleetshift-realm.json` is a maintained source file. Edit it directly to change clients, roles, or users, then re-run `deploy.sh` to apply changes.

### Clients

| Client ID | Type | Purpose | PKCE |
|-----------|------|---------|------|
| `fleetshift-ui` | Public | UI authentication (standard flow) | S256 |
| `fleetshift-cli` | Public | CLI authentication (standard flow, device auth) | S256 |
| `fleetshift-signing` | Public | Key enrollment / attestation flow | S256 |
| `ocp-console` | Confidential | OCP web console OIDC login | No (OCP console doesn't support PKCE) |

The `fleetshift-signing` client has a `github_username` attribute mapper that includes the user's GitHub username in tokens. This is used by the attestation flow to resolve the signer's public key from the GitHub SSH key registry.

### Roles

| Role | Description |
|------|-------------|
| `ops` | Operations persona — manages clusters and infrastructure |
| `dev` | Developer persona — manages applications |

### Test Users

| Username | Roles | `github_username` | Password |
|----------|-------|--------------------|----------|
| `ops` | ops | `ops-github` | Generated at deploy time |
| `dev` | dev | `dev-github` | Generated at deploy time |
| `admin` | ops, dev | `admin-github` | Generated at deploy time |

The `github_username` values are placeholders. Update them in `realm/fleetshift-realm.json` to real GitHub usernames before deploying if you need the attestation flow to work end-to-end.

### Redirect URIs

The realm ships with `localhost` redirect URIs for local development. Add your production app URLs to `realm/fleetshift-realm.json` or via the Keycloak admin console when deploying the app on the cluster.

## File Structure

```
prod/
├── deploy.sh                      # Main deploy script
├── teardown.sh                    # Clean removal script
│                                  # (add-user.sh moved to deploy/podman/scripts/)
├── manifests/
│   ├── namespace.yaml             # keycloak-prod namespace
│   ├── cert-manager-sub.yaml      # cert-manager operator (OLM)
│   ├── cluster-issuer.yaml        # Let's Encrypt ClusterIssuer
│   ├── certificate.yaml           # TLS certificate for Keycloak route
│   ├── postgres-statefulset.yaml  # PostgreSQL StatefulSet + Service
│   ├── rhbk-sub.yaml              # RHBK operator (OLM)
│   └── keycloak.yaml              # Keycloak CR
└── realm/
    └── fleetshift-realm.json      # FleetShift realm configuration
```
