# Multi-IdP Authentication Spike

This document records the backend direction being explored for OME-315. It is an implementation note, not a final product design.

## Goal

Allow OME to authenticate tokens from multiple OIDC providers without relying on provider ordering or a hardcoded provider.

The core request flow is:

```text
token
  -> read issuer as an untrusted routing hint
  -> find configured auth method by issuer
  -> verify signature and claims with that method
  -> attach method and identity to request context
```

## TrustPolicy

`TrustPolicy` is the authoritative backend resource for authentication trust. It replaces scattered startup OIDC values as the long-term source of truth.

Conceptually:

```text
TrustPolicy
  -> AuthMethods
  -> TenantBindings
```

An `AuthMethod` describes who can authenticate:

```yaml
id: corporate-keycloak
issuer: https://keycloak.example.com/realms/company
audiences:
  - fleetshift
clientId: fleetshift-ui
scopes:
  - openid
  - profile
  - email
```

A binding describes where an authenticated identity belongs:

```yaml
authMethodId: corporate-keycloak
emailDomains:
  - redhat.com
```

Keeping bindings separate supports one tenant with multiple IdPs and one IdP serving multiple tenants.

## Resource Operations

The backend resource should support normal resource operations:

```text
Get   /v1/trustPolicies/{id}
List  /v1/trustPolicies
Post  /v1/trustPolicies
```

Update and disable/delete behavior can follow the normal resource lifecycle. Existing `fleetctl auth setup` can become the client workflow for these APIs; it does not require a separate `AuthSetup` backend resource.

## Bootstrap

Startup configuration is a conditional first create:

```text
server starts
  -> load bootstrap config
  -> check whether TrustPolicy exists
  -> if absent, internally create initial TrustPolicy
  -> initialize authentication from persisted policy
  -> open authenticated API
```

If a policy already exists, startup inputs are ignored. They must not overwrite or reconcile persisted configuration. Bootstrap input may contain one initial IdP; additional IdPs are added through TrustPolicy operations.

Bootstrap should use the same validation and persistence path as the normal create operation, even if the public API is not available yet.

The IdP lifecycle is outside TrustPolicy. Packaging or deployment selects and starts the initial provider before OME bootstrap runs:

```text
no external IdP configured
  -> packaging starts sandbox Dex
  -> bootstrap creates TrustPolicy for Dex

external IdP configured
  -> packaging does not start Dex
  -> bootstrap creates TrustPolicy for external IdP
```

TrustPolicy records and activates trust for the selected issuer; it does not start or manage the IdP process itself.

## UI Provider Discovery

`GET /api/ui/config` returns all configured OIDC providers. Each entry contains provider-specific login data:

```json
{
  "oidc": [
    {
      "authority": "https://idp.example.com/realms/main",
      "clientId": "fleetshift-ui",
      "scope": "openid profile email",
      "authorizationEndpoint": "https://idp.example.com/auth",
      "emailDomain": "@example.com"
    }
  ]
}
```

The UI chooses a provider before login. The backend still determines and validates the provider from the token issuer; the UI selection is not trusted as authentication proof.

The CLI should use the same TrustPolicy discovery rather than maintaining a separate provider list. `fleetctl auth login` can:

```text
load TrustPolicy providers
  -> if one provider is available, use it
  -> if several providers are available, prompt for provider
  -> persist selected auth-method ID in the local CLI context
  -> authenticate against selected provider
```

An explicit provider option can bypass the prompt, for example `--auth-method=corporate-keycloak`. The selected method is a login hint; the server still resolves and validates the method from the token issuer. A TrustPolicy ID is only needed if users can select between multiple policies; for choosing a provider within one policy, the auth-method ID is the relevant identifier.

The CLI can persist the selected auth-method ID in its local context, similar to how `oc` persists a selected project:

```text
CLI context
  -> OME endpoint
  -> selected auth-method ID
  -> credentials
```

This is a convenience for subsequent logins, not an authority decision. If the method is no longer present or the server rejects the token issuer, the CLI must discard the stale selection and prompt again.

## Domain Mapping

Email domains are discovery hints. They are not identity proof and must not be used instead of issuer and subject verification.

After token verification, identity is based on:

```text
(issuer, subject)
```

The verified identity can then be resolved through the applicable tenant binding. If required, the verified email claim can be checked against the binding's allowed domains.

## Audience

Audience validates token purpose after issuer selects the auth method.

```text
JWT issuer -> select AuthMethod
JWT audience -> validate API/resource use
```

The initial simple model can require `fleetshift` as the API audience. A more flexible TrustPolicy may allow a list of audiences per auth method so existing customer clients can be used when their tokens already contain an accepted audience.

This covers two related cases:

1. One auth method accepts tokens issued for several configured audiences.
2. One token contains several audience values, one of which must match the configured list.

Example:

```yaml
id: corporate-keycloak
audiences:
  - fleetshift
  - existing-management-api
signingEnrollmentAudience: fleetshift-signing
```

The verifier should accept the token only when its `aud` claim intersects the configured audience list. An arbitrary audience should never be accepted just because the issuer is trusted.

Audience values are provider-specific. TrustPolicy may explicitly accept an existing customer audience to reduce IdP setup work:

```yaml
id: customer-keycloak
issuer: https://keycloak.customer.example/realms/main
audiences:
  - existing-customer-api
```

This allows OME to consume an existing client/realm configuration when its tokens are otherwise suitable. A dedicated `fleetshift` audience is preferred because it clearly identifies OME as the target API. Existing customer audiences are supported as a fallback, but each accepted value must be explicitly listed in TrustPolicy. OME must never accept arbitrary audiences from a trusted issuer.

An auth method may also opt into client-level restriction:

```yaml
allowedClients:
  - fleetshift-ui
  - fleetshift-cli
```

When configured, `azp` must identify one of these OIDC client registrations. When omitted, issuer and audience validation are sufficient. `azp` identifies the OIDC client that obtained the token; it does not identify the browser, CLI, or network connection that sent the HTTP request.

## UI and CLI Tokens

For this spike, UI and CLI are clients of the same OME API. The transport used to submit a request is not an authorization boundary: a user can perform the same operation through a browser, CLI, or browser terminal. These clients can therefore use tokens accepted for the same API audience.

Authorization for sensitive operations belongs in normal endpoint and resource RBAC. UI configuration endpoints may have separate access rules because they are configuration resources, not because the request came from a UI.

Separate token types or audiences remain possible if a concrete protocol or security requirement appears, but they are not required merely to distinguish UI from CLI.

Audience rules should be aligned with OME's planned authentication changes before finalizing verifier behavior. UI client audiences and API resource audiences may not be interchangeable.

### Signing enrollment

Signing enrollment may retain a separate purpose-specific audience:

```text
normal API token          -> aud = fleetshift
signing enrollment token  -> aud = fleetshift-signing
```

The expected signing audience belongs in the auth-method configuration within TrustPolicy. TrustPolicy stores the expected value, not the IdP signing keys; token keys continue to come from OIDC discovery and JWKS.

When signing enrollment is enabled, the customer IdP must be configured to issue tokens with the signing audience. This can use a dedicated enrollment client or an audience mapper on an existing client. Missing signing-audience configuration should disable or reject enrollment rather than silently reusing the normal API audience.

Token exchange could provide a separate signing token:

```text
API access token
  -> OME forwards token and provenance
  -> fleetlet/agent verifies or exchanges it with customer IdP
  -> signing token with signing audience
  -> agent verifies signing token
```

This is not universally available. OIDC does not require RFC 8693 token exchange, and customer IdPs may not support the required exchange endpoint, client permissions, delegation, or audience restrictions.

Therefore token exchange must not be a baseline requirement. The compatible baseline is one API access-token audience combined with signing-specific RBAC, provenance, and operation authorization. If stronger separation is needed, the fleetlet/agent can use an internal short-lived signed capability bound to the specific signing operation; OME must remain a courier and must not mint or assert the signing identity.

## Runtime Authentication

HTTP and gRPC should use one shared authenticator and provider registry.

The authentication result should include:

```text
authMethodId
issuer
subject
claims
validated audience
raw token
```

The runtime registry should maintain an issuer index and isolated JWKS state for each provider. Provider updates should replace the live registry atomically rather than mutating it partially.

Code must not select the first OIDC method. This applies to request authentication, signer enrollment, provenance verification, and trust-bundle handling.

For cluster-hosted extension backends, authentication may terminate at the fleetlet or direct cluster proxy. The agent can validate the customer token, perform a supported token exchange, and pass a protected identity context to the extension backend. That context requires a trusted channel such as mTLS, a signed envelope, or an equivalent cluster-local trust mechanism. Plain identity headers are not sufficient.

This is one channel mode, not a requirement for every operation. OME also has platform-authenticated API and addon paths where OME validates the request and passes pre-authenticated tenant context downstream. Courier mode applies where the target or fleetlet must remain the trust boundary, especially for delivery, factory targets, and provenance-protected operations.

## Configuration Plumbing

Environment variables, process arguments, and JSON/YAML files should feed one normalized configuration model. `koanf` is a candidate for this plumbing.

Hot reload is not required for the first implementation. Initial work should establish predictable precedence and safe startup loading; reload can be added after provider-registry replacement and JWKS lifecycle behavior are defined.

## AIO Spike Boundary

Sandbox Dex plus external Keycloak is only packaging behavior for this spike. The backend should consume generic provider definitions and should not contain Dex-specific selection logic.

Production behavior may use sandbox Dex when no external provider is configured and disable Dex when an external provider is configured.

## Open Questions

- Exact `TrustPolicy` resource shape and tenant binding representation.
- Whether one policy exists per platform, tenant, or trust domain.
- Accepted audience representation and endpoint-specific rules.
- Bootstrap config file format and precedence with flags and environment.
- Runtime API operations for provider replacement and disablement.
