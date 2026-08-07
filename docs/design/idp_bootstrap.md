# Initial platform IdP bootstrap

## What this doc covers

How the platform learns its first provider-tenant IdP before it can authenticate anyone, and how that differs from later IdP changes.

It does not cover delivery-authorization trust at targets, signing-key enrollment, or multi-tenant IdP discovery. Those live in [authentication.md](authentication.md) and related tenancy work.

## The problem

The API cannot safely accept unauthenticated calls to install the first IdP. Once an IdP exists, authenticated admins can change trust through the normal API. Before that, there is nothing to authenticate against.

Today the server can start an HTTP listener and leave Day One auth setup open. That is convenient for the prototype and unsafe as a lasting model: anyone who can reach the listener can shape the first trust root.

So the first IdP must come from **authority over the runtime**, not from an open network call.

## Two paths, one model

From the platform's point of view, every IdP is external. Dex in the same image, Keycloak next door, or a corporate issuer are the same: an OIDC authority the server discovers and trusts. The server should not embed Dex-specific logic or treat "sandbox IdP" as a second auth system.

There are two ways to set that trust:

1. **Bootstrap** — install the first provider-tenant IdP from runtime authority.
2. **Update** — change IdP configuration through the authenticated API once a trust root exists.

Neither path is fully in place yet. Bootstrap via config is the one to build first. Updates and any out-of-band recovery path come after.

### Bootstrap (runtime authority)

Supply the first IdP when the process starts, or through a later local control channel that proves process ownership. Examples:

- Config / environment on `fleetshift serve` (issuer, audiences, clients as needed)
- A local socket, managed through the CLI (e.g. `podman exec … fleetctl bootstrap --issuer …)` (not required for the first cut)

Until bootstrap succeeds, do not open the public HTTP API. After bootstrap, start the listener and authenticate against that IdP.

This is the sole bootstrap path. A sandbox image is just packaging that already knows what to put in config.

### Update (authenticated API)

Once the platform trusts an IdP, further trust changes are ordinary authenticated admin operations. The admin's credential chains back to the current anchor.

We do not yet have a clean supported update path (today some code paths effectively take the first auth method). Design that as the one model — "current provider IdP, and the procedure to change it." Multi-tenant / multi-IdP discovery is separate and still open; see login-discovery notes under Open questions.

## Sandbox is packaging, not a server mode

The all-in-one sandbox should look like production as much as possible:

- Same bootstrap-from-config path
- Same auth-method model
- Same update rules when we have them

What makes it a sandbox is the image and entrypoint: a peer Dex (or similar), static clients, and config that registers that issuer on start. Users skip a separate `fleetctl bootstrap` because bootstrap already did that work.

Avoid a distinct `--sandbox` auth mode that freezes the IdP, owns Dex inside the server, or blocks later change. Extra modes cost code and diverge from production. If someone wants the sandbox binary pointed at their own Keycloak, that should work through the same model.

> NOTE: Local Keycloak as a dedicated auth mode can go away. A small OIDC provider is enough for trials; the trust architecture will soon no longer need advanced OAuth features for the demo path.

### Audience and clients

Prefer a client per CLI and UI, as well as an additional OIDC client just for signature enrollment. Use a resource-server-specific audience check where the IdP supports them (Dex: cross-client trust / `audience:server:client_id:…`), otherwise we can relax that on the server side (e.g. support multiple audiences, matching the different clients–this is somewhat a misapplication of OAuth, but can be acceptable as we are trying to lower the barrier to entry).

For federating claims, we do not need complex claim mappings. A "Groups" claim of some kind is the most we should need, to integrate group membership.

> NOTE: Signing enrollment against embedded Dex is not a priority while the attestation model is being reworked. External registries (for example GitHub) can remain fine for sandbox demos if supporting it does not add much complexity (e.g. username is same in Dex as in GitHub...)

## What the UI and CLI should assume

Clients stay IdP-agnostic. They learn issuer and client settings from the backend (similar to kubectl contexts), not from baked-in Dex knowledge.

Consequences:

- No UI control to pick "embedded vs external" IdP as a special product mode.
- After bootstrap, first-run UI can skip OIDC issuer entry and go straight to sign-in (and later setup steps such as addons).
- IdP will still be configurable in the clients, but only after the initial bootstrap

## Deferred


| Item                                     | Notes                                                                                                                           |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Local-socket / CLI bootstrap             | Useful for production when IdP is not trivially known; not required to ship config-based bootstrap. Likely its own control API. |
| IdP update / replace after Day One       | Needed eventually; singular model, not sandbox-specific.                                                                        |
| Multi-tenant IdP discovery / login page  | Needed for multiple issuers; single-tenant can keep a static redirect.                                                          |
| Pivot from sandbox Dex to another issuer | Same as IdP update once that path exists                                                                                        |




## Open questions

- Exact config surface for bootstrap (issuer only vs full auth-method fields, default auth method id, audience set).
- Shape of the eventual recovery channel when the configured IdP is wrong or unavailable (ties to the "root user / out-of-band trust" note in [authentication.md](authentication.md#open-challenges)).
- How provider-tenant bootstrap composes with later consumer-tenant IdPs under multitenancy ([OME-53](https://redhat.atlassian.net/browse/OME-53)).



## Related

- [authentication.md](authentication.md) — delivery authorization, IdP trust at targets, open "root user" challenge
- [architecture/platform_hierarchy.md](architecture/platform_hierarchy.md) — platform bootstrap/pivot (instance lifecycle, not IdP trust)
- [architecture/tenancy_and_permissions.md](architecture/tenancy_and_permissions.md) — provider/tenant model that IdP binding must eventually serve
- OME-206 — sandbox console without a prerequisite external IdP
- OME-7 — Day 1 trust setup (user-facing ceremony once an IdP exists)
- OME-53 — multiple OIDC endpoints for multitenancy

