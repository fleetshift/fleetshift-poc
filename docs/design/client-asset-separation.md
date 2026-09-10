# Client Asset Separation

Status: Proposed for OME-310

## TL;DR

- Shared code does not automatically require an NPM package or public SDK.
- Classify overlap as source-only client sharing, internal package reuse, or future public SDK.
- Keep shared setup kernel minimal: version, server endpoint, and typed runtime configuration.
- Compose runtime-specific behavior instead of using unrestricted overrides.
- Web receives setup from API/bootstrap; CLI composes setup from defaults, profile, environment, and arguments.
- Share plugin capabilities, search semantics, API contracts, error classification, localization contracts, and pure signing primitives only when runtime-neutral.
- Keep Web routing, CLI/TUI routing, authentication, key storage, transport details, and rendering runtime-specific.
- Use `google.rpc.Status` and `google.rpc.BadRequest` for REST API errors exposed through gRPC-Gateway.
- Use stable error codes for i18n; human-readable server messages remain fallback text.
- Validate real CLI/Web overlap before making a model part of shared code or SDK.

## Decision Summary

CLI and Web UI may share source without sharing an NPM package.

Shared client code lives under `client/shared/src`. CLI and Web UI bundle this source directly through Rspack aliases. The directory is an internal source boundary, not a published SDK and not an independently built package.

This separates two decisions that are often conflated:

- Code can be shared between FleetShift clients.
- Code is stable and intentionally exposed to external plugin or SDK users.

The first does not imply the second.

## Proposed Layout

```text
client/
  shared/
    src/
      config/
      profiles/
      validation/
  cli/
  web/
```

The exact subdirectories are illustrative. Shared code should be organized by client-domain capability, not by bundler or runtime.

## Configuration Model Boundary

OME-310 requires a shared configuration and profile model, but it does not require every runtime to populate that model in the same way.

The shared model should describe client configuration that is currently established by the product. It should not invent provider, tenant, workspace, or RBAC fields before those concepts and identifiers are defined elsewhere.

The issue description mentions tenant and provider context as intended scope. Those fields remain unresolved until the RBAC and tenancy design establishes their shape. They should be added through a deliberate schema change, not represented by guessed IDs or names.

Likewise, profile selection is a runtime concern. The CLI may select a named profile from local configuration. The Web UI may receive already-resolved configuration from its API and never expose profile selection. Both runtimes can still consume the same profile data shape where applicable.

## Example Shared Types

The shared object is a root-level client setup or context. It describes which FleetShift instance the client uses and carries one runtime-specific payload. It is not an authentication configuration and does not assume provider, tenant, workspace, or RBAC fields.

```ts
export interface ClientSetup<TRuntimeConfig> {
  version: 1;
  server: {
    endpoint: string;
  };
  runtimeConfig: TRuntimeConfig;
}

export interface WebRuntimeConfig {
  plugins: Record<string, {
    enabled: boolean;
    routes?: WebRoute[];
  }>;
}

export interface CliRuntimeConfig {
  plugins: Record<string, {
    enabled: boolean;
    routes?: CliRoute[];
    commands?: string[];
    tui?: {
      views?: string[];
    };
  }>;
}

export interface WebRoute {
  path: string;
  module: string;
}

export interface CliRoute {
  path: string;
  screen: string;
}

export type WebSetup = ClientSetup<WebRuntimeConfig>;
export type CliSetup = ClientSetup<CliRuntimeConfig>;
```

The generic keeps the shared envelope small and prevents it from accumulating empty `web` and `cli` option fields. Web plugins may contribute browser routing configuration. CLI plugins may also contribute routing configuration, but Ink routing uses an in-memory router such as React Router's `MemoryRouter`. Web routes represent browser URLs; CLI routes represent terminal screen states and navigation history.

`TRuntimeConfig` is a compile-time contract. API responses and local files still require runtime validation before being treated as `ClientSetup<TRuntimeConfig>`.

A named profile can provide a setup:

```ts
export interface ClientProfile<TRuntimeConfig> {
  name: string;
  setup: ClientSetup<TRuntimeConfig>;
}
```

The CLI can load a `ClientProfile<CliRuntimeConfig>` from local files and select one during startup. The Web UI can receive a `WebSetup` directly from its API. Profile selection does not need to be part of the Web UI or part of the resolved setup.

These types are a design example, not a final plugin API. Exact plugin routing, command, and TUI fields require separate plugin-contract decisions.

## Shared Capabilities And Runtime Adapters

The shared model can describe plugin capabilities consumed by multiple clients. Search declarations are one example: a plugin can provide field metadata, operators, dynamic hints, and result information that Web search and a CLI TUI can both consume. The interaction model and presentation remain runtime-specific.

Navigation requires more care. A generic ordered tree of plugin capabilities may transfer between Web and CLI, but the current Web navigation model is not automatically portable. `NavLayoutEntry` includes Web-oriented concepts such as PatternFly icons, page IDs, and browser route associations. Those fields should not be treated as a shared CLI contract without first separating generic navigation semantics from Web rendering metadata.

The transferable part may be an ordered hierarchy of entries with stable capability identifiers, labels, and optional descriptions. Web can map those entries to URLs and icons. CLI can map them to Ink screens, commands, and `MemoryRouter` paths. Whether one navigation model is useful should be validated against real CLI flows rather than assumed from code reuse in `sdk/common`.

Signing follows the same pattern. Canonical payload construction, encoding, and the signing operation contract may be shared. Key storage and access must remain runtime-specific:

```ts
export interface Signer {
  sign(payload: Uint8Array): Promise<Uint8Array>;
}

export interface SigningContext {
  algorithm: string;
  keyId?: string;
}
```

The Web UI may implement `Signer` with browser cryptography and a browser-managed key flow. The CLI may implement it with an operating-system keychain, local agent, or another secure key provider. Shared configuration must never contain private keys or assume how keys are stored.

The general rule is:

```text
shared contract and pure semantics -> client/shared
Web implementation and presentation -> client/web
CLI implementation and presentation -> client/cli
```

This allows shared plugin configuration, search semantics, signing contracts, and canonical data handling without forcing browser and CLI runtimes to use the same storage, cryptography, navigation, or presentation mechanism.

## Sharing Destinations

Overlap does not determine one implementation location. Each candidate should be classified by its intended audience and stability:

- `client/shared`: internal source-only code bundled directly into CLI and Web UI, with no package or public compatibility promise.
- Internal package such as `sdk/common`: reusable built code for FleetShift applications when a package boundary or independent testing is useful, but without community API guarantees.
- Public SDK: stable contracts intentionally consumed by community plugin developers, requiring versioning, compatibility policy, and documentation.

Likely `client/shared` candidates include setup composition, runtime adapters, localization integration, and client-only validation. Likely internal package candidates include shared REST client behavior, API types, error parsing, search contracts, and pure signing primitives. Public SDK candidates require a separate review; plugin contracts, API types, search metadata, and signing interfaces may qualify later, but current code reuse does not make them public APIs.

## What Belongs In Shared Source

Shared code must be runtime-neutral and usable by both clients. Initial OME-310 candidates include:

- Client configuration and profile types for established concepts.
- Configuration defaults and precedence rules.
- Pure configuration validation.
- Serialization and normalization of shared configuration values.
- Runtime-independent helpers used by both clients.

Shared code must not require browser or Node.js globals.

## What Does Not Belong In Shared Source

Runtime-specific behavior remains in its client:

- CLI argument parsing, environment variables, and filesystem persistence.
- Web UI state, browser storage, and user-interface rendering.
- CLI or Web UI transport adapters.
- Authentication flows and IdP discovery.
- Runtime-specific error presentation.

Authentication and IdP discovery remain outside OME-310's shared model boundary.

## Runtime Configuration Loading

Configuration loading is intentionally runtime-specific:

```text
CLI flags/files/env  -> CLI loader      -> shared resolved config
Web API/bootstrap    -> Web loader      -> shared resolved config
```

The CLI may need an explicitly configured server endpoint and authentication method because it cannot assume an API bootstrap call. The Web UI may receive OIDC or other bootstrap settings from its server. These different loading paths do not require different shared configuration types, but authentication material and discovery behavior remain outside the shared model.

The shared model represents the result and semantics of configuration, not the mechanism used to obtain it.

## Precedence And Defaults

For CLI setup, values are composed in this order from lowest to highest priority:

```text
built-in defaults -> selected profile -> environment variables -> command-line arguments
```

The highest-priority value wins for fields that can be supplied by multiple sources. Command-line arguments therefore take priority over environment variables, environment variables take priority over the selected profile, and the selected profile takes priority over built-in defaults.

The CLI may use a default profile when no profile is selected. If no default or selected profile supplies a required endpoint, setup fails with an actionable error. Authentication configuration follows its own runtime-specific flow and is not part of this precedence model.

For Web setup, the API or bootstrap response is the authoritative source for the resolved setup. CLI profile files, CLI environment variables, and CLI arguments do not participate in Web setup. Web-specific composition may add runtime capabilities, but it must not silently merge unrelated CLI configuration.

This precedence describes typed composition by each runtime. It is not an unrestricted override map.

## Configuration Validation Scope

Configuration validation applies to the client setup and profile values before application code consumes them. It does not define validation for every Web form, command payload, or server API resource.

Shared validation covers the common envelope and established fields:

- `version` is supported.
- `server.endpoint` is present and is a valid URL.
- Profile name is present and unambiguous when profiles are used.
- Plugin identifiers are valid and do not collide after normalization.
- Required shared plugin fields are present.

Each runtime validates its own `runtimeConfig` payload. Web validates Web plugin configuration such as route declarations. CLI validates CLI plugin configuration such as command and TUI declarations. The shared validator should not understand fields belonging only to one runtime.

The Web API or bootstrap response is untrusted configuration input and must be validated before becoming Web context. CLI files, environment variables, and command-line arguments are normalized into the same setup shape and then validated. API request payloads and authentication configuration have separate validation boundaries.

## Error Contract

The repository already uses the standard gRPC error envelope [`google.rpc.Status`](https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto). The gRPC-Gateway serializes this envelope as JSON, and the shared `ResourceApiError` currently exposes it as `RpcStatus` with `code`, `message`, and `details`.

The standard envelope is:

```proto
message Status {
  int32 code = 1;
  string message = 2;
  repeated Any details = 3;
}
```

The numeric `code` uses [canonical gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html) such as `INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`, and `INTERNAL`. The existing server commonly returns `InvalidArgument` with only formatted text, so field-level structured details are not implemented consistently yet.

For validation errors, the standard [`google.rpc.BadRequest`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.BadRequest.html) detail can carry field violations:

```proto
message BadRequest {
  repeated FieldViolation field_violations = 1;
}

message FieldViolation {
  string field = 1;
  string description = 2;
  string reason = 3;
}
```

The shared REST client should preserve the complete `Status` envelope and decode known detail types where useful. CLI and Web UI should branch on canonical status codes and stable detail reasons, not human-readable messages.

Local CLI or Web setup validation can use an equivalent internal issue shape before an HTTP request exists. When the server returns validation errors, clients should use `google.rpc.BadRequest` rather than inventing a second API error format. The CLI can print field violations and the Web UI can associate them with fields, show a toast, or show a full-screen error.

Validation should collect all independent field violations in one response rather than stop at the first error. Other standard error details may be added when applicable, including [`google.rpc.ErrorInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.ErrorInfo.html) for machine-readable reasons, [`google.rpc.RetryInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.RetryInfo.html) for retry delays, [`google.rpc.ResourceInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.ResourceInfo.html) for resource identity, and [`google.rpc.Help`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.Help.html) for documentation links.

## Error Presentation And Localization

The error payload contains enough information for clients to choose presentation when it includes a canonical status code, stable reason or code, and field paths. The Web UI can map `/server/endpoint` to a form control, show multiple field violations together, or use a toast or full-screen error when no field mapping exists. The CLI can print the same paths and messages, select human-readable or JSON output, and return a non-zero exit status.

Human-readable `message` and `description` values are useful fallback text but must not be the client branching or translation key. Future i18n should translate stable codes or reasons owned by the client, with parameters supplied separately where needed.

For example, a validation detail can use a stable reason and metadata alongside `BadRequest`:

```text
Status.code: INVALID_ARGUMENT
ErrorInfo.reason: INVALID_ENDPOINT
BadRequest.field_violations[0].field: server.endpoint
BadRequest.field_violations[0].description: Endpoint must be an absolute URL.
ErrorInfo.metadata: scheme=http, allowed_schemes=https
```

The client can map `INVALID_ENDPOINT` to a localized message and use metadata as interpolation or validation data. Server text remains a fallback for unknown codes, diagnostics, and clients without a translation catalog. [`google.rpc.LocalizedMessage`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.LocalizedMessage.html) may support server-selected localization later, but it should not replace stable machine-readable reasons or force the server to choose the client's locale.

## Localization Boundary

Localization is a good candidate for shared client code when it contains message identifiers, interpolation rules, fallback behavior, and locale-independent error classification. The shared layer should provide a translator contract or pure translation function, not require React context.

```ts
export interface Translator {
  readonly locale: string;
  translate(messageId: string, values?: Record<string, string | number>): string;
}
```

The Web UI can provide `Translator` through React context. Ink also uses React and can provide the same contract through its component tree. Classic CLI commands can call the translator directly without rendering through React. This keeps localization available to table output, JSON-adjacent human output, help text, and non-TUI error paths.

Locale selection remains runtime-specific. Web may use an account preference, browser preference, or server-provided setting. CLI may use an explicit locale argument, environment locale, or profile setting. Both should fall back to a default locale when no supported locale is selected.

Core error message identifiers and catalogs may be shared. Plugin-specific messages should be supplied by each plugin and loaded by the runtime that uses them. Clients must retain the server message as a fallback when no local translation exists, while continuing to branch on stable error codes.

## Candidate Localization Engine

The existing client workspace does not currently include an i18n engine. [`i18next`](https://www.i18next.com/) is a candidate for the runtime-neutral engine and can run in both Node.js and browser environments. [`react-i18next`](https://react.i18next.com/) provides React context, hooks, and components around that engine.

The dependency boundary could be:

```text
client/shared -> message IDs, catalog shape, translator-facing types
i18next      -> catalog loading, locale selection, interpolation, fallback
Web UI       -> react-i18next provider and React hooks
Ink TUI      -> react-i18next provider and React hooks
classic CLI  -> i18next.t() or a shared Translator directly
```

Ink is a React renderer, so `react-i18next` context can work in TUI components as long as components use terminal-safe output. The current CLI already renders command output through Ink's `render()` call and an application-level React provider, including classic table output. Therefore classic CLI output can use the same localization context. Non-component code can use the core `i18next` instance or a shared translator directly.

Ink rendering is not ReactDOM server-side rendering. The main context concern is provider scope: create or select the locale before calling Ink `render()`, then place the i18n provider around the rendered command or TUI tree. A per-run i18n instance is preferable to mutable process-global locale state so tests and future context switching do not leak locale between renders.

Shared code should not import `react-i18next`. This keeps localization usable by classic CLI code and non-React shared helpers while allowing Web and Ink to use their normal context integration.

## Composition Over Overrides

Runtime-specific behavior should be composed around the shared model rather than applied as an unrestricted override layer. Generic overrides make it easy to merge settings intended for one runtime into another and make configuration ownership unclear.

The shared layer should contain the maximum useful overlap. Each runtime then adds an explicit wrapper or adapter for its requirements:

```ts
interface SharedClientContext {
  endpoint: string;
  plugins: Record<string, SharedPluginSetup>;
}

interface SharedPluginSetup {
  enabled: boolean;
}

interface WebClientContext extends SharedClientContext {
  web: WebRuntimeConfig;
}

interface CliClientContext extends SharedClientContext {
  cli: CliRuntimeConfig;
}
```

In this design, the OME-310 requirement for runtime-specific overrides means runtime-specific composition. It does not mean arbitrary field replacement or a shared bag of unknown options. Web-only values cannot leak into CLI setup, and CLI-only values cannot leak into Web setup.

## Runtime Context Switching

The TUI may eventually allow switching profiles or client contexts while running. This is an extension point, not a requirement for the first OME-310 implementation.

A context switch must replace dependent state as one operation. That includes the API client, authentication state, plugin configuration, search state, and TUI routes. Partially replacing context could leave data, credentials, or navigation from the previous context active.

The initial design should therefore allow context replacement without requiring it. It should not make context immutable in a way that prevents future TUI switching, but implementation should begin with context selection during startup.

## Bundling

Each application resolves the internal alias to source:

```text
@fleetshift/client-shared/* -> client/shared/src/*
```

Rspack then transpiles and bundles imported files into the consuming artifact. No shared package build is required, and no runtime package is loaded by the browser or CLI.

Each artifact contains its own bundled copy. This is intentional: source sharing provides code reuse and consistent behavior, not a runtime dependency or cross-application module registry.

Rspack watch mode tracks shared files reached through the alias. A change to a used file under `client/shared/src` causes the consuming application to rebuild.

## TypeScript Resolution

Each client TypeScript configuration mirrors the Rspack alias with `paths`:

```json
{
  "paths": {
    "@fleetshift/client-shared/*": ["../../client/shared/src/*"]
  }
}
```

`baseUrl` is intentionally not used. TypeScript 6 deprecates it; path targets include their explicit relative prefix instead.

Because shared source is outside each application's `src` directory, the client `rootDir` must include the complete `client` directory. This affects TypeScript project configuration only; Rspack remains responsible for emit.

## Nx Build Inputs

Client build inputs must include shared source:

```text
client/shared/**/*
```

Otherwise an Nx-cached non-watch build may remain valid after a shared source change. Watch mode dependency tracking and Nx cache invalidation are separate concerns and both must be handled.

## SDK Boundary

`client/shared` is internal implementation. Its exports are not automatically available to community plugin developers and must not be treated as a public compatibility promise.

A future SDK may expose selected configuration or profile types, but that is a separate design decision requiring:

- A stable public API shape.
- Versioning and compatibility policy.
- Runtime and environment support guarantees.
- Documentation for external consumers.

Until that decision exists, shared client source may change with the internal CLI and Web UI implementation.

## Validation

The source-only approach has been validated with a shared helper imported by Web UI code. Rspack resolved the aliased TypeScript source, included it in the Web bundle, and watch-mode rebuild behavior was confirmed after correcting the test path so the helper was executed.

## Follow-Up Decisions

- Identify the OME-310 configuration and profile types that form the first shared model.
- Define configuration precedence and default values.
- Define validation error structure shared by both clients.
- Decide which, if any, shared types later become SDK contracts.
- Add explicit Nx inputs for `client/shared/**/*`.
