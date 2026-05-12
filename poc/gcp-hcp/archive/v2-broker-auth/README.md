# GCP HCP API Auth Validation POC — Broker SA Flow

Tests the validated broker service account auth flow for OME -> GCP HCP API integration:

```
Keycloak -> Workforce STS -> generateIdToken (broker SA) -> gateway + X-User-Email
```

**Design spec:** `docs/superpowers/specs/2026-05-06-gcp-hcp-broker-poc-scripts-design.md` (in the UbuntuSync repo)
**Auth direction:** `docs/fleetshift/gcphcp/2026-05-06-validated-gcp-hcp-auth-direction.md`

## Prerequisites

- `gcloud` CLI — installed and authenticated (`gcloud auth login`)
- `curl`, `jq`, `python3` — standard tools
- GCP Workforce Identity Pool with Keycloak provider (see `00-setup-gcp.sh`)
- Broker service account with `roles/iam.serviceAccountOpenIdTokenCreator` binding (see `prerequisites.md` section 6)
- A Keycloak instance with a publicly accessible JWKS endpoint

## Quick Start

```bash
# 1. Configure
cp config.env.example config.env
# Edit config.env with your values

# 2. Set up GCP (one-time)
./00-setup-gcp.sh

# 3. Run the auth flow
./01-get-keycloak-token.sh        # Get Keycloak JWT via browser login
./02-workforce-sts-exchange.sh    # Exchange for Google access token
./03-generate-broker-idtoken.sh   # Mint broker SA ID token (Google-signed JWT)
./04-test-gateway.sh              # Test gateway with broker JWT + X-User-Email

# 4. If step 4 passes — full lifecycle
./05-crud-lifecycle.sh            # Create/list/get/status/delete cluster
./06-check-identity.sh            # Verify created_by = Keycloak email

# 5. Clean up
./teardown.sh
```

## Decision Tree

```
01: Keycloak Token
├── Success -> 02: Workforce STS Exchange
│   ├── Success (opaque ya29... token) -> 03: Generate Broker ID Token
│   │   ├── Success (Google-signed JWT) -> 04: Test Gateway
│   │   │   ├── JWT-only test -> AUTH_REQUIRED (expected)
│   │   │   └── JWT + X-User-Email -> 200 -> 05: CRUD Lifecycle -> 06: Identity Check
│   │   └── Failure (403) -> Fix IAM binding on broker SA
│   └── Failure -> Fix Workforce pool/provider config
└── Failure -> Fix Keycloak config
```

## What Each Script Does

| Script | Purpose |
|--------|---------|
| `00-setup-gcp.sh` | Creates GCP Workforce Identity Pool + Keycloak OIDC provider |
| `01-get-keycloak-token.sh` | Fetches Keycloak JWT via auth code + PKCE (browser login) |
| `02-workforce-sts-exchange.sh` | Exchanges Keycloak JWT for Google access token via Workforce STS |
| `03-generate-broker-idtoken.sh` | Mints a Google-signed JWT for the broker SA via IAM Credentials |
| `04-test-gateway.sh` | Tests gateway with broker JWT + X-User-Email (the make-or-break test) |
| `05-crud-lifecycle.sh` | Full cluster create/list/get/status/delete lifecycle |
| `06-check-identity.sh` | Inspects `created_by` to verify identity propagation |
| `teardown.sh` | Deletes GCP resources created by this POC |

## Token Flow

```
tmp/keycloak_token.jwt         (01 -> 02)    Keycloak access token (JWT)
tmp/user_email.txt             (02 -> 04-06) User email from Keycloak claims
tmp/workforce_access_token.txt (02 -> 03)    Opaque Google access token (ya29...)
tmp/broker_idtoken.jwt         (03 -> 04-06) Google-signed JWT for broker SA
tmp/cluster_id.txt             (05 -> 06)    Cluster ID from CRUD lifecycle
```

## Archived Scripts

The `archive/v1-workforce-direct/` directory contains the original scripts that tested
the now-disproven direct Workforce STS -> gateway path. They are preserved for reference
but are no longer part of the active flow.

## Files

- `config.env.example` — configuration template (copy to `config.env`)
- `lib.sh` — shared helpers (config loader, JWT decoder, broker auth, API call helper)
- `prerequisites.md` — all GCP setup steps to reproduce
- `tmp/` — gitignored directory for tokens and response dumps
