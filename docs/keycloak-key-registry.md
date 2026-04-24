# Key Registries

## Overview

FleetShift supports two key registry types for signing key enrollment. Each registry resolves a user's ECDSA P-256 public key for signature verification. The admin configures one registry at a time via `fleetctl auth setup`.

- **OIDC** — the public key is embedded as a claim in the enrollment ID token. Works with any OIDC-capable IdP (Keycloak, Auth0, Ping, etc.).
- **GitHub** — the user uploads their SSH signing key to GitHub; the server fetches it from the public API.

The OIDC registry is preferred because verification requires only the JWKS endpoint (public, cacheable, transferable out of band). The GitHub registry is a fallback for when the IdP cannot store public keys in claims.

## Registry Configuration

The admin configures the registry via `fleetctl auth setup`. For the OIDC registry, a CEL expression (`--public-key-claim-expression`) tells the platform and agent where to find the public key in the ID token claims. For GitHub, a `RegistrySubjectMapping` maps claims to the GitHub username.

```bash
# OIDC registry (default) — public key extracted from ID token claim
fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui \
  --key-enrollment-client-id fleetshift-ui \
  --public-key-claim-expression 'claims.signing_public_key'

# GitHub registry — public key fetched from GitHub API
fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui \
  --key-enrollment-client-id fleetshift-ui \
  --registry-id github.com \
  --registry-subject-expression 'claims.github_username'
```

To switch registries, re-run `auth setup` with the appropriate flags.

---

## OIDC Registry

The OIDC registry extracts the signing public key from the enrollment ID token using the configured CEL expression (e.g. `claims.signing_public_key`). The token is verified against the IdP's JWKS keyset — no Admin API access required.

```
Enrollment:
  Browser -> POST /account (user's token) -> stores signing_public_key attribute
  Browser -> refreshes session to get fresh ID token with signing_public_key claim
  Browser -> POST /signerEnrollments with ID token
  Platform -> verifies ID token via JWKS, validates public key claim is present

Verification (platform-side, at deploy time):
  Platform -> extracts public key from stored enrollment ID token via CEL
  Platform -> verifies deployment signature against the extracted public key

Verification (agent-side, at delivery time):
  Agent -> fetches JWKS from IdP (URI from trust bundle bootstrap)
  Agent -> verifies enrollment ID token against IdP keyset
  Agent -> extracts public key from verified ID token via CEL
  Agent -> verifies deployment signature against the extracted public key
```

The agent verifies independently — the attestation bundle contains only the enrollment ID token, not the IdP keys. This prevents the platform from forging attestations.

### Keycloak Setup

If using Keycloak as your OIDC IdP:

**1. User Profile: allow custom attributes**

Keycloak 24+ enforces User Profile validation. `signing_public_key` must be declared in the realm's user profile config, or set `unmanagedAttributePolicy: "ENABLED"`.

**2. Protocol mapper: signing_public_key claim**

Add an `oidc-usermodel-attribute-mapper` to your OIDC client(s) that maps the `signing_public_key` user attribute to an ID token claim. The key must be stored in the user's profile before the enrollment token is issued.

**3. Users: manage-account client role**

The Account REST API (used by the browser to store the public key) requires the `manage-account` role from the `account` client. Without this, `POST /account` returns 401.

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

The GitHub registry only makes sense if your IdP cannot store the public key in a claim directly.

### Signing Key vs Authentication Key

GitHub has two SSH key types: Authentication Keys (`/users/{u}/keys`) and Signing Keys (`/users/{u}/ssh_signing_keys`). The server fetches from the **signing keys** endpoint. If the user adds the key as an Authentication Key, verification fails. The server detects this and returns a specific error message.

### Prerequisites

- User has a GitHub account
- `github_username` claim available in the ID token (requires a protocol mapper if using Keycloak)
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
