# Testing Signing Key Registries

End-to-end testing for Keycloak and GitHub signing key flows.

## Prerequisites

```bash
docker compose up --build
```

Wait for all services to be healthy. The stack starts with **Keycloak** registry by default.

## Test 1: Keycloak Registry (default)

1. Open http://localhost:3000, log in as `ops` / `test`
2. Navigate to the signing key page
3. **Generate** key, select **Keycloak**, click **Enroll**
4. Create a deployment — should be signed and verified (reaches `STATE_ACTIVE`)

## Test 2: Switch to GitHub Registry

### Switch the server

```bash
docker compose exec fleetshift fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui \
  --key-enrollment-client-id fleetshift-ui \
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

## Test 3: Switch back to Keycloak

```bash
docker compose exec fleetshift fleetctl auth setup \
  --server fleetshift:50051 \
  --issuer-url http://keycloak:8180/auth/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui \
  --key-enrollment-client-id fleetshift-ui \
  --registry-id keycloak \
  --registry-subject-expression 'claims.preferred_username'
```

Remove key, generate, pick Keycloak, enroll, create deployment. No re-login needed.

## Common Errors

| Symptom | Fix |
|---------|-----|
| `no such key: github_username` | Log out/in; verify protocol mapper and attribute exist |
| Keycloak `POST /account` 401 | Add `manage-account` client role to user |
| GitHub verification fails | Re-add key as **Signing Key**, not Authentication Key |
| `no registry subject mapping configured` | Run `fleetctl auth setup` with `--registry-id` |
