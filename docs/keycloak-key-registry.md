# Key Registries

## Overview

FleetShift supports two key registries for signing key enrollment. Each registry stores/resolves a user's ECDSA P-256 public key for signature verification. The admin configures one registry at a time via `fleetctl auth setup`.

- **Keycloak** — stores the public key as a Keycloak user attribute; the server reads it via the Admin API
- **GitHub** — the user uploads their SSH signing key to GitHub; the server fetches it from the public API

## Registry Configuration

A single `RegistrySubjectMapping` on the OIDC auth method binds a `registry_id` to a CEL expression that extracts the registry subject from the user's ID token claims.

```bash
fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui \
  --key-enrollment-client-id fleetshift-ui \
  --registry-id keycloak \
  --registry-subject-expression 'claims.preferred_username'
```

To switch registries, re-run `auth setup` with different `--registry-id` and `--registry-subject-expression`.

---

## Keycloak Registry

```
Enrollment:
  Browser -> POST /account (user's token) -> stores signing_public_key attribute
  Browser -> POST /signerEnrollments -> server derives subject via CEL

Verification:
  Server -> GET /admin/realms/{realm}/users?username={sub} (admin token)
  Server -> verifies ECDSA signature against the fetched public key
```

### Keycloak Configuration Required

**1. User Profile: allow custom attributes**

Keycloak 24+ enforces User Profile validation. `signing_public_key` and `github_username` must be declared, or set `unmanagedAttributePolicy: "ENABLED"` in the realm JSON. Without this, the Account API silently drops custom attributes.

**2. Protocol mapper: github_username claim**

For the GitHub registry, add an `oidc-usermodel-attribute-mapper` to the `fleetshift-ui` client that maps the `github_username` user attribute to an ID token claim. The attribute must be set on the user *before* login.

**3. Users: manage-account client role**

The Account REST API requires the `manage-account` role from the `account` client. Without this, `POST /account` returns 401.

**4. Backend: Admin API credentials**

The server reads user attributes via the Admin REST API using password grant against the `master` realm with `admin-cli`. Credentials are configured via environment variables on the `fleetshift serve` process:

- `KC_ADMIN_USERNAME` — Keycloak admin username
- `KC_ADMIN_PASSWORD` — Keycloak admin password

These are set in `docker-compose.yml` on the `fleetshift` service to match `KC_BOOTSTRAP_ADMIN_USERNAME`/`KC_BOOTSTRAP_ADMIN_PASSWORD` on the Keycloak service.

---

## GitHub Registry

```
Enrollment:
  Browser -> copies SSH pubkey to clipboard
  User -> pastes at github.com/settings/ssh/new as "Signing Key"
  Browser -> POST /signerEnrollments -> server derives subject via CEL

Verification:
  Server -> GET https://api.github.com/users/{username}/ssh_signing_keys
  Server -> verifies ECDSA signature against the fetched public key
```

### Signing Key vs Authentication Key

GitHub has two SSH key types: Authentication Keys (`/users/{u}/keys`) and Signing Keys (`/users/{u}/ssh_signing_keys`). The server fetches from the **signing keys** endpoint. If the user adds the key as an Authentication Key, verification fails. The server detects this and returns a specific error message.

### Prerequisites

- User has a GitHub account
- `github_username` attribute set in Keycloak (before login)
- Key added as **Signing Key** on GitHub

No server-side credentials needed (public API).

---

## Future: Multi-Registry Support

Currently one registry per auth method. To let users pick their registry at enrollment time:

1. Change `OIDCConfig.registry_subject_mapping` to `repeated registry_subject_mappings`
2. Add `registry_id` to `CreateSignerEnrollmentRequest`
3. CLI `auth setup` becomes additive (upsert by registry ID)
4. Trust bundle carries all mappings for agent-side verification

## Future: Addon Architecture

Key registries should be pluggable addons with configurable credentials, not hardcoded in domain.
