# OME-330 - Server-Side Authentication Enforcement

**Status**: Design (ready for implementation)  
**Related Issues**: FM-88, OME-312, OME-315  
**Related Documents**: [authentication.md](./authentication.md), [multi_idp_authentication_spike.md](https://github.com/Hyperkid123/fleetshift-poc/blob/57032b55842a17c56e07883e622c852af2973fa3/docs/design/multi_idp_authentication_spike.md)  
**Branch**: `provenance-suite-poc`

---

## Overview

The goal of this effort is to ensure the fleetshift-server always authenticates clients before accepting requests. The design evolves the current `AuthMethod` domain aggregate to `AuthorityConfig`, aligning with the [multi-IdP authentication spike](https://github.com/Hyperkid123/fleetshift-poc/blob/57032b55842a17c56e07883e622c852af2973fa3/docs/design/multi_idp_authentication_spike.md) and the provenance-suite contract.

The implementation is structured in two distinct phases:

- **Phase 1 (Bootstrap)**: Server startup and initial IdP configuration. No HTTP/gRPC listeners yet. IdP must be configured via bootstrap (config file, CLI args, env vars, or UDS admin socket). Once configured, the provider IdP is persisted to DB.
- **Phase 2 (Runtime)**: Listeners are open, IdP is configured in DB. Client authentication proceeds. Adding a second IdP requires manual DB wipe + server restart with new bootstrap config. (Future: APIs for runtime IdP management.)

**Key principles**:

1. **Authentication only**: This effort focuses on *who is making the request*, not *what they're allowed to do*. Authorization (RBAC, permission checks) is out of scope.

2. **Bootstrap-driven configuration**: Phase 1 enforces IdP configuration through a bootstrap action before opening listeners. Configuration is persisted to the database once during first startup.

3. **Multi-IdP support**: Multiple identity providers (Dex, Keycloak, Okta, future SAML, etc.) can be configured, each keyed by `PrincipalAuthority` (scheme + issuer). The server routes incoming requests to the correct authority configuration based on the token's `iss` claim.

4. **Tenant isolation**: Each authority configuration maps identities to a tenant. The authenticated context carries the tenant ID, enabling downstream authorization checks and audit.

5. **Extensible credential methods**: The design prepares for future credential types (SAML, mTLS, etc.) without requiring a complete refactor. Initially, only OIDC is implemented.

---

## Boundary: Authentication vs. Authorization

- **Authentication**: Answers "who is this request from?" Verifies credentials, resolves identity to a tenant, produces an `AuthorizationContext`.
- **Authorization**: Answers "is this identity allowed to perform this operation?" Applies RBAC, permission lookups, policy checks.

This effort addresses authentication. Authorization is handled separately by the application services layer (out of scope here).

The one gray area is `DeliveryPolicy` in the provenance contract's `AuthorityConfig`. That rule about what evidence is required for delivery crosses into policy but is tied to the authority's trust relationship. It belongs in `AuthorityConfig` because it's part of the authority's authentication contract, not a separate authorization resource.

---

## Two-Phase Model

### Phase 1: Bootstrap (No Listeners Yet)

The server starts and must obtain IdP configuration before opening any HTTP/gRPC listener:

```
Server starts (no listeners)
  → DB has AuthorityConfig(s)?
      YES → if bootstrap args also provided, warn:
            "DB already has authority configs; ignoring bootstrap config"
           → load from DB
           → register JWKS key sets
           → proceed to Phase 2
      NO  → bootstrap config provided? (file, CLI, env, or UDS)
          YES → parse and validate bootstrap config
               → run OIDC discovery (resolve JWKS, endpoints)
               → persist AuthorityConfig to DB
               → trigger ProvisionIdP workflow (creates trust-bundle deployment)
               → register JWKS key sets
               → proceed to Phase 2
          NO  → is AIO mode enabled? (FLEETSHIFT_AIO=true env var)
              YES → configure embedded Dex as provider IdP
                   → persist to DB
                   → trigger ProvisionIdP workflow
                   → register JWKS key sets
                   → proceed to Phase 2
              NO  → refuse to start with clear error:
                    "no identity provider configured: provide --bootstrap-provider-tenant
                     or set FLEETSHIFT_AIO=true or use the AIO image"
```

### Phase 2: Runtime (Listeners Open)

The server opens HTTP/gRPC listeners. Client authentication is enforced. The provider IdP is fixed for this deployment until manual DB manipulation + server restart.

**Future**: APIs will allow runtime management of additional IdPs and tenant IdPs, but that is out of scope for this phase.

---

## User Scenarios

### Scenario 1: Laptop Try-Out (AIO with Embedded Dex)

**User**: Developer trying out FleetShift on a laptop.

**Phase 1 (Bootstrap)**:
1. Developer runs `docker run fleetshift-aio`.
2. AIO container startup (entrypoint):
   - Sets `FLEETSHIFT_AIO=true`
   - Starts embedded Dex internally
3. FleetShift server starts:
   - DB is empty (fresh container)
   - No `--bootstrap-provider-tenant` flag provided
   - Sees `FLEETSHIFT_AIO=true`
   - Auto-generates Dex IdP config (hardcoded defaults for embedded Dex)
   - Persists to DB
   - Triggers `ProvisionIdP` workflow (creates trust-bundle deployment)

**Phase 2 (Runtime)**:
4. Developer navigates to OME UI. UI redirects to Dex login. Developer authenticates.
5. CLI: Developer runs `fleetctl auth login`. CLI discovers the configured Dex provider and initiates PKCE flow.
6. Subsequent restarts: Server loads Dex config from DB, skips bootstrap.

**Key point**: Zero manual IdP configuration. The developer never sees a setup wizard or bootstrap prompt. From first interaction, authentication is enforced.

---

### Scenario 2: Enterprise Deployment with Company Keycloak

**User**: Ops admin at Acme Corp. Non-AIO deployment. Company has corporate Keycloak.

**Phase 1 (Bootstrap)**:
1. Admin creates a bootstrap config file (`provider-idp.yaml`):
   ```yaml
   idp:
     issuerUrl: https://keycloak.acme.com/realms/engineering
     audiences: acme-fleetshift-api,acme-fleetshift-cli
     clientId: fleetshift-ui
     tenantId: acme
     keyEnrollmentAudience: acme-fleetshift-signing
   ```
2. Admin starts server with bootstrap config:
   ```bash
   fleetshift-server serve --bootstrap-provider-tenant=/etc/fleetshift/provider-idp.yaml
   ```
3. Server:
   - Reads and parses `provider-idp.yaml`
   - Runs OIDC discovery against Keycloak
   - Persists AuthorityConfig to DB
   - Triggers `ProvisionIdP` workflow
   - Opens listeners with auth enforced

**Phase 2 (Runtime)**:
4. Team members authenticate via Keycloak. Server resolves them to tenant `acme`.
5. Admin later wants to add a second IdP (contractor Okta):
   - Manually wipe the authority config from DB
   - Restart server with new bootstrap config pointing to Okta
   - Server creates new AuthorityConfig, persists, opens listeners

**Key point**: Configuration via file; operators control the IdP choice. Restart required to change IdPs (future APIs will allow runtime changes).

---

### Scenario 3: Contractor Access (Future Runtime API)

**Note**: This scenario describes the *intent* but is not implemented in Phase 2. It informs the design of the `AuthorityConfig` model and APIs (T9 in the task list), which exist but are not the focus of this work item.

**User**: Acme ops admin wants to grant contractor developers access.

**Future flow (Phase 3+)**:
1. Admin calls `fleetctl auth tenant-idp create --from-file contractor-okta.yaml` (future API).
2. Server creates a second AuthorityConfig for contractor Okta (separate from the provider IdP).
3. Contractor developers authenticate via Okta, are mapped to a `contractor` tenant or the same `acme` tenant (depending on `TenantMapping`).
4. RBAC determines what they can access within their tenant.

For this work item, adding a contractor IdP requires: DB wipe + restart with new bootstrap config (using the new IdP).

---

## Bootstrap Configuration

### Input Methods

The bootstrap action is invoked via one of three input channels:

1. **Configuration file** (primary, recommended):
   ```bash
   fleetshift-server serve --bootstrap-provider-tenant=/etc/fleetshift/provider-idp.yaml
   ```

2. **Environment variable**:
   ```bash
   FLEETSHIFT_BOOTSTRAP_PROVIDER_TENANT=/etc/fleetshift/provider-idp.yaml fleetshift-server serve
   ```

3. **Direct CLI flags** (for simple cases):
   ```bash
   fleetshift-server serve \
     --bootstrap-provider-tenant-idp-issuer-url=https://keycloak.acme.com/realms/eng \
     --bootstrap-provider-tenant-idp-audiences=acme-fleetshift-api
   ```

4. **UDS admin socket** (future, for programmatic bootstrap):
   - Not implemented in this phase.
   - Design: admin-only gRPC endpoint over a Unix domain socket (e.g., `/var/run/fleetshift/admin.sock`).
   - Allows bootstrap after server is running (for orchestration platforms).

### File Format

**YAML** is the canonical format (loaded via koanf):

```yaml
# /etc/fleetshift/provider-idp.yaml
idp:
  issuerUrl: https://keycloak.acme.com/realms/engineering
  audiences: acme-fleetshift-api,acme-fleetshift-cli
  clientId: fleetshift-ui                          # defaults to "fleetshift-ui"
  tenantId: acme                                    # defaults to "default"
  keyEnrollmentAudience: acme-fleetshift-signing
  caFile: /etc/ssl/certs/idp-ca.pem                # optional
```

**Minimal file** (only required fields):

```yaml
idp:
  issuerUrl: https://keycloak.acme.com/realms/engineering
  audiences: acme-fleetshift-api
```

**Fields**:

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `issuerUrl` | string | YES | — | OIDC issuer URL (e.g., `https://keycloak.acme.com/realms/engineering`) |
| `audiences` | string | YES | — | Comma-separated list of accepted audience claims (e.g., `api,cli`) |
| `clientId` | string | NO | `fleetshift-ui` | OIDC client ID for the UI |
| `tenantId` | string | NO | `default` | FleetShift tenant ID (resolved by tenant mapping) |
| `keyEnrollmentAudience` | string | NO | — | Purpose-scoped audience for signing key enrollment tokens |
| `caFile` | string | NO | — | Path to PEM CA certificate for self-signed IdP certs |

### koanf Integration

koanf (https://github.com/knadh/koanf) is used for flexible configuration management:

- Load file: `file.Provider(path, yaml.Parser())`
- Overlay env vars: `env.Provider("FLEETSHIFT_BOOTSTRAP_PROVIDER_TENANT__", "__", keyMapper)`
- Overlay CLI flags: `posflag.Provider(flags, ".", k)`
- Unmarshal into `BootstrapProviderTenantConfig` struct

**Precedence** (highest to lowest): CLI flags > env vars > file > defaults

Example:

```go
// serve.go startup logic
bootstrapFile := f.bootstrapProviderTenant  // from --bootstrap-provider-tenant flag

if bootstrapFile != "" {
    bk := koanf.New(".")
    
    // Load file
    if err := bk.Load(file.Provider(bootstrapFile), yaml.Parser()); err != nil {
        return fmt.Errorf("parse bootstrap config %s: %w", bootstrapFile, err)
    }
    
    // Overlay env vars (optional overrides)
    bk.Load(env.Provider("FLEETSHIFT_BOOTSTRAP_PROVIDER_TENANT__", "__", func(s string) string {
        return strings.ToLower(strings.ReplaceAll(s, "__", "."))
    }), nil)
    
    // Unmarshal
    var bootstrapCfg BootstrapProviderTenantConfig
    if err := bk.Unmarshal("idp", &bootstrapCfg); err != nil {
        return fmt.Errorf("invalid bootstrap config: %w", err)
    }
    
    // Proceed with bootstrap action
    if err := authConfigService.BootstrapProviderTenant(ctx, bootstrapCfg); err != nil {
        return fmt.Errorf("bootstrap provider tenant: %w", err)
    }
}
```

---

## Design

### Domain Model

#### `AuthorityConfig` (renamed from `AuthMethod`)

```go
type AuthorityConfig struct {
    id                 AuthorityConfigID
    principalAuthority PrincipalAuthority         // (scheme, authority) — key for lookup
    credentialMethods  []CredentialMethod         // extensible; initially OIDC only
    tenantMapping      TenantMapping              // how to resolve tenant from verified claims
    allowedClients     []ClientID                 // optional azp (OAuth client) restriction
}

type PrincipalAuthority struct {
    Scheme    IdentityScheme  // "oidc", future: "saml", "mTLS"
    Authority Authority       // https://keycloak.acme.com/realms/engineering, etc.
}

type IdentityScheme string
const IdentitySchemeOIDCSubV1 IdentityScheme = "oidc-sub/v1"

type Authority string  // canonical issuer URL or trust domain

type CredentialMethod struct {
    Type CredentialMethodType  // "oidc", future: "saml"
    OIDC *OIDCCredentialConfig // non-nil when Type == "oidc"
}

type CredentialMethodType string
const CredentialMethodTypeOIDC CredentialMethodType = "oidc"

type OIDCCredentialConfig struct {
    IssuerURL                IssuerURL
    Audiences                []Audience              // multiple audiences
    JWKSURI                  EndpointURL             // resolved from discovery
    AuthorizationEndpoint    EndpointURL             // resolved from discovery
    TokenEndpoint            EndpointURL             // resolved from discovery
    KeyEnrollmentAudience    Audience                // purpose-scoped enrollment token audience
    PublicKeyClaimExpression string                  // CEL expression for key extraction
    RegistrySubjectMapping   *RegistrySubjectMapping // for external key registries
}

type TenantMapping struct {
    // Initially: static 1:1 mapping.
    // Future: claim-based mapping via CEL expression.
    StaticTenant TenantID
}

type TenantID string
type Audience string
type ClientID string
```

#### Enriched `AuthorizationContext`

```go
type AuthorizationContext struct {
    Subject              *domain.SubjectClaims    // nil if anonymous
    Client               *application.ClientClaims // nil if no azp claim
    Audience             []domain.Audience        // token aud claim
    Token                domain.RawToken          // verified JWT
    Request              application.RequestClaims
    
    // NEW: enriched on authentication
    TenantID             TenantID                 // resolved from TenantMapping
    PrincipalAuthority   PrincipalAuthority       // the authority that authenticated this request
    AuthorityConfigID    AuthorityConfigID        // which AuthorityConfig was used
}
```

#### Repository Interface

```go
type AuthorityConfigRepository interface {
    Save(ctx context.Context, config AuthorityConfig) error
    Get(ctx context.Context, id AuthorityConfigID) (AuthorityConfig, error)
    List(ctx context.Context) ([]AuthorityConfig, error)
    // Indexed lookup for authentication routing
    FindByAuthority(ctx context.Context, pa PrincipalAuthority) (AuthorityConfig, error)
    Delete(ctx context.Context, id AuthorityConfigID) error
}
```

This interface is designed for future DB persistence (SQLite, Postgres). Phase 2 implements it in-memory. Phase 3+ can swap in a DB implementation without changing authentication logic.

#### Shared `Authenticator`

```go
type Authenticator struct {
    configs   AuthorityConfigRepository
    verifier  OIDCTokenVerifier
    observer  AuthnObserver
    cache     *methodCache  // principal-authority → config (with TTL)
}

func (a *Authenticator) Authenticate(ctx context.Context, rawToken string) (*AuthorizationContext, error) {
    // No token + configs exist = error (auth enforced)
    // No token + no configs = anonymous (setup mode, unlikely in Phase 2)
    // Token present:
    //   1. Extract issuer from JWT header (base64-decode, no verification)
    //   2. Look up AuthorityConfig by PrincipalAuthority{Scheme: "oidc", Authority: issuer}
    //   3. Verify token signature against JWKS
    //   4. Validate audience (any-of intersection with config's audiences)
    //   5. Validate azp (OAuth client) if restricted in config
    //   6. Resolve tenant via TenantMapping
    //   7. Return enriched AuthorizationContext
}
```

### Transport Layer

Both gRPC (`AuthnInterceptor`) and HTTP (`AuthnMiddleware`) delegate to the shared `Authenticator`:

```go
// gRPC interceptor
func (a *AuthnInterceptor) authenticate(ctx context.Context, ...) (context.Context, error) {
    token := extractBearerToken(ctx)
    authCtx, err := a.authenticator.Authenticate(ctx, token)
    if err != nil {
        return ctx, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
    }
    return application.ContextWithAuth(ctx, authCtx), nil
}

// HTTP middleware
func (a *AuthnMiddleware) Wrap(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        token := extractBearer(r)
        authCtx, err := a.authenticator.Authenticate(r.Context(), token)
        if err != nil {
            w.Header().Set("WWW-Authenticate", "Bearer")
            writeJSON(w, http.StatusUnauthorized, ...)
            return
        }
        ctx := application.ContextWithAuth(r.Context(), authCtx)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

---

## Implementation Tasks

### Phase 1: Domain Model (T1-T2)

**T1: `AuthorityConfig` domain aggregate**
- Rename `AuthMethod` → `AuthorityConfig` in `domain/authn.go`
- Define `PrincipalAuthority`, `CredentialMethod`, `OIDCCredentialConfig`, `TenantMapping`
- Add multi-audience support (`[]Audience`)
- Add allowed-clients restriction (`[]ClientID`)
- `AuthorityConfigRepository` interface with `Save`, `Get`, `List`, `FindByAuthority`, `Delete`
- Validation, snapshot serialization, tests

**T2: SQLite and Postgres persistence**
- Rename `AuthMethodRepo` → `AuthorityConfigRepo` in both SQLite and Postgres
- Schema migration: add columns for principal authority, audiences, tenant mapping, allowed clients
- Implement `FindByAuthority` with index
- Repository contract tests

### Phase 2: Unified Authentication (T3-T6)

**T3: Shared `Authenticator`**
- Issuer-based routing via `FindByAuthority`
- Multi-audience validation (any-of intersection)
- `azp` client restriction
- Tenant resolution via `TenantMapping`
- Enriched `AuthorizationContext` with `TenantID`, `PrincipalAuthority`, `AuthorityConfigID`
- Unit tests with fake repository and verifier

**T4: Refactor `AuthnInterceptor`**
- Delegate to shared `Authenticator`
- Update all existing tests
- Ensure backward compatibility with existing gRPC endpoints

**T5: Refactor `AuthnMiddleware`**
- Delegate to shared `Authenticator`
- Update all existing tests
- Ensure backward compatibility with existing HTTP endpoints

**T6: Consolidate `VerifySignHandler`**
- Use shared authenticator or enrollment-scoped verification helper
- Remove duplicated method-loading logic

### Phase 3: Bootstrap (T7-T8)

**T7: Bootstrap configuration schema and parsing**
- `BootstrapProviderTenantConfig` struct with `IDP` sub-struct
- YAML file parsing via koanf
- Validation: `issuerUrl` and `audiences` required; defaults for `clientId` (`"fleetshift-ui"`), `tenantId` (`"default"`)
- Comma-separated audience parsing
- Error handling and logging
- Tests for parsing, validation, defaults, error cases

**T8: Wire bootstrap into `serve.go`**
- Add `--bootstrap-provider-tenant` flag (file path)
- Add `FLEETSHIFT_BOOTSTRAP_PROVIDER_TENANT` env var
- Add `FLEETSHIFT_AIO` env var (AIO mode detection)
- Phase 1 startup logic:
  1. Check DB for existing AuthorityConfigs
  2. If DB non-empty: load from DB, warn if bootstrap also provided, skip to Phase 2
  3. If DB empty + bootstrap file: parse, discover, persist, trigger `ProvisionIdP` workflow
  4. If DB empty + no bootstrap + AIO: auto-configure Dex, persist, trigger workflow
  5. If DB empty + no bootstrap + not AIO: refuse to start with error
- Adapt `ProvisionIdP` workflow context (called during bootstrap, not from API)
- Leave `--oidc-ui-authority` flag as-is (out of scope for now)

### Phase 4: API & Testing (T9-T10)

**T9: Update `AuthMethodService` proto and transport to `AuthorityConfig` naming**
- Rename proto messages: `AuthMethod` → `AuthorityConfig`, `AuthMethodService` → `AuthorityConfigService`
- Update gRPC service: `CreateAuthMethod` → `CreateAuthorityConfig`, etc.
- Update transport server: `AuthMethodServer` → `AuthorityConfigServer`
- Mechanical renaming; logic unchanged from this work item's perspective
- Compile and test to ensure no regressions

**T10: Integration test — bootstrap → enforced auth**
- Use `oidctest.Provider` as real OIDC IdP
- Write bootstrap YAML file pointing to it
- Test: start server with `--bootstrap-provider-tenant=<file>`
- Verify: unauthenticated gRPC request → `Unauthenticated`
- Verify: authenticated gRPC request with valid token → succeeds
- Verify: HTTP authenticated request → succeeds
- Verify: subsequent restart without bootstrap flag → loads from DB, still enforced
- Verify: non-AIO without bootstrap and empty DB → server refuses to start with clear error

---

## Open Questions for Future Work

1. **Claim-based tenant mapping**: Should `TenantMapping` support extracting tenant from a JWT claim (e.g., `org_id`) for multi-tenant IdPs?

2. **SAML support**: Design and implement SAML 2.0 as an alternative credential method type.

3. **mTLS/certificate authentication**: Design and implement certificate-based credential methods.

4. **Runtime IdP management APIs**: Expose CRUD APIs for managing multiple IdPs and tenant-specific IdPs (Phase 3+).

5. **UDS admin socket bootstrap**: Allow programmatic bootstrap via a Unix domain socket endpoint (for orchestration platforms).

6. **Credential revocation**: How to proactively revoke a specific user's active tokens (currently only issuer-level removal via DB is supported)?

7. **WebSocket authentication**: Browser WebSocket API cannot set custom headers. Should we support JWT as a query parameter (with appropriate security mitigations) or require a separate authentication handshake?

---

## References

- [Multi-IdP Authentication Spike](https://github.com/Hyperkid123/fleetshift-poc/blob/57032b55842a17c56e07883e622c852af2973fa3/docs/design/multi_idp_authentication_spike.md)
- [Authentication Design](./authentication.md)
- [Provenance Suite Contract](../../poc/provenance-suites/protocol/contract.go)
- [Trust Model v3](./trust_model_v3.md)
- [Security Design](./security.md)
- [koanf Configuration Management](https://github.com/knadh/koanf)
