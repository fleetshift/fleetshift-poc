# Testing Signing Key Registries

End-to-end testing for OIDC and GitHub signing key flows.

## Prerequisites

```bash
docker compose up --build
```

Wait for all services to be healthy. The stack starts with **OIDC** registry by default.

## Test 1: OIDC Registry (default)

1. Open http://localhost:3000, log in as `ops` / `test`
2. Navigate to the signing key page
3. **Generate** key, select **OIDC**, click **Enroll**
4. Create a deployment — should be signed and verified (reaches `STATE_ACTIVE`)

## Test 2: Switch to GitHub Registry

### Switch the server

```bash
docker compose exec fleetshift fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-cli \
  --audience fleetshift \
  --key-enrollment-client-id fleetshift-signing \
  --registry-id github.com \
  --registry-subject-expression 'claims.github_username'
```

### Re-enroll

1. Verify `github_username` is set in Keycloak Admin (Users -> ops -> Attributes)
2. **Remove** existing key in the UI
3. **Log out and back in** (need `github_username` claim in token)
4. **Generate** new key, select **GitHub**, click **Enroll**
5. Paste SSH key at https://github.com/settings/ssh/new — select **Signing Key** type
6. Create a deployment — should be signed and verified

## Test 3: Switch back to OIDC

```bash
docker compose exec fleetshift fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-cli \
  --audience fleetshift \
  --key-enrollment-client-id fleetshift-signing \
  --public-key-claim-expression 'claims.signing_public_key'
```

Remove key, generate, pick OIDC, enroll, create deployment.

## Common Errors

| Symptom | Fix |
|---------|-----|
| `no such key: github_username` | Log out/in; verify protocol mapper and attribute exist |
| IdP `POST /account` 401 | Add `manage-account` client role to user |
| GitHub verification fails | Re-add key as **Signing Key**, not Authentication Key |
| `OIDC auth method has no public_key_claim_expression` | Run `fleetctl auth setup` with `--public-key-claim-expression` |
| `identity token has no "signing_public_key" claim` | Key wasn't stored before token was issued; re-enroll (the UI refreshes automatically) |
| `public key claim evaluation failed` | The CEL expression failed to extract a value from the ID token claims |
| `signing key validation failed` | The extracted claim value is not a valid base64-encoded ECDSA P-256 SPKI key |

## Public Key Claim Expression

The platform uses a CEL expression to extract the signing public key from the ID token's claims. The expression receives a `claims` variable (the full JWT claims map) and must produce a string (the base64-encoded SPKI public key).

```bash
fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-cli \
  --audience fleetshift \
  --key-enrollment-client-id fleetshift-signing \
  --public-key-claim-expression 'claims.signing_public_key'
```

For nested claim structures, use dotted paths: `claims.keys.signing`.

The expression is stored in the auth method config, included in the trust bundle delivered to agents, and used at both enrollment validation and attestation verification time.
