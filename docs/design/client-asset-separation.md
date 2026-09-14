# Client Asset Separation

Status: Proposed for OME-310

## TL;DR

- CLI and Web UI can share source without creating an NPM package or public SDK.
- Source-only code lives in `client/shared/src` and is bundled directly into each client by Rspack.
- Share contracts and pure behavior; compose each runtime around them instead of using unrestricted overrides.
- Web receives configuration from API/bootstrap. CLI composes configuration from its own sources.
- REST client behavior and API contracts should be usable by both clients; packaging/public SDK status remains a separate decision.
- Search metadata, plugin capability metadata, error parsing, localization contracts, and signing contracts may be shared when runtime-neutral.
- Browser/Ink presentation, routing, authentication, transport, and key storage remain runtime-specific.
- REST errors use existing `google.rpc.Status` JSON from gRPC-Gateway. Future field validation uses `google.rpc.BadRequest`.

## Source Sharing

```text
client/
  shared/
    src/
  cli/
  web/
```

`client/shared` is an internal source boundary. It is neither an independently built package nor an external compatibility promise.

This separates two different decisions:

- Code is shared by FleetShift clients.
- Code is stable and intentionally exposed to community plugin or SDK users.

The first does not require the second.

Each Rspack configuration resolves `@fleetshift/client-shared/*` to `client/shared/src/*`. Rspack transpiles and bundles imported code into each consuming artifact. It watches imported shared files in watch mode.

TypeScript mirrors this alias with `paths`. Since shared files sit outside an application's `src` directory, each client `rootDir` must include the `client` directory. TypeScript 6 does not require `baseUrl` for `paths`; do not use it.

Nx cache inputs must include `client/shared/**/*`. Rspack watch tracking and Nx cache invalidation are separate concerns.

The approach was validated by importing a shared helper into Web UI code. Rspack resolved, bundled, and rebuilt that source.

## Shared Configuration

OME-310 defines a shared configuration model, not one configuration loader. Web and CLI fill the model differently:

```text
Web API/bootstrap -> Web composition -> resolved Web context
CLI sources       -> CLI composition -> resolved CLI context
```

Web and CLI are runtimes. TUI is a CLI presentation mode.

The shared model describes established cross-runtime concepts only. Do not add provider, tenant, workspace, RBAC, authentication, key-storage, or other fields before their domain contracts exist.

`ClientSetup<T>` illustrates intended shape, not approved final schema:

```ts
interface ClientSetup<TRuntimeConfig> {
  version: number;
  server: {
    endpoint: string;
  };
  runtimeConfig: TRuntimeConfig;
}
```

The generic allows one common envelope (can be extended as implementation moves) while preserving typed runtime specific configuration. API/bootstrap responses and CLI input still require runtime validation before use.

Runtime behavior is composition, not generic overrides. The shared layer carries maximum genuine overlap; each runtime adds an explicit wrapper or adapter. Web-only configuration must not leak into CLI setup, and CLI-only configuration must not leak into Web setup.

## Sharing Candidates

Review each candidate for runtime neutrality and intended audience before choosing its location.

| Candidate | Shared part | Runtime-specific part |
| --- | --- | --- |
| REST API client | request/response types, paths, error parsing | browser/Undici transport, auth injection, CA handling |
| Plugins | identity and capability metadata | Scalprum/assets, Web routes, CLI commands, Ink screens |
| Search | fields, operators, dynamic hints, result metadata | browser indexing and UI/TUI interaction |
| Navigation | stable capability IDs, labels, hierarchy if proven useful | PatternFly icons, browser URLs, Ink screens and `MemoryRouter` paths |
| Signing | canonical payload construction and signer contract | browser cryptography, keychain/local-agent access, key storage |
| Localization | message IDs, catalog shape, interpolation, fallback rules | locale source, catalog loading, React/Ink presentation |

Current `sdk/common` already contains reusable REST and API types alongside React-oriented exports. Reuse of the REST client does not mean every current export is a public SDK. Move only confirmed, runtime-neutral behavior into `client/shared`; package/public-SDK decisions require separate stability and versioning work.

## Configuration Sources, Precedence, And Defaults

CLI may receive configuration from files, environment variables, and command-line arguments. Web setup comes from API/bootstrap. CLI inputs do not participate in Web setup.

For values provided by more than one CLI source, command-line arguments take priority over environment variables and configuration files. Exact configuration-file rules and all defaults remain open because clients must be configuration-driven: production clients must not embed backend defaults for IdP, plugins, navigation, or other server-owned settings.

Deployment or AIO/sandbox configuration may provide development defaults, such as a local IdP. Those defaults belong to deployment/bootstrap configuration, not general client behavior. Server-provided configuration can define defaults for core plugins or user experience where appropriate.

Authentication and IdP discovery remain runtime-specific and outside this shared configuration model. The CLI may require explicit setup; Web may receive OIDC configuration from its API.

## Validation And Errors

Configuration validation applies to resolved client setup, not to every Web form, command payload, or server resource. The exact schema and validation library remain open. Generated API types describe protobuf/API data but do not replace runtime validation of untrusted JSON; a runtime schema library such as Zod is a possible implementation choice.

The repository already exposes the standard [`google.rpc.Status`](https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto) envelope as JSON through gRPC-Gateway. `sdk/common` currently represents it as `RpcStatus` with `code`, `message`, and `details`.

```proto
message Status {
  int32 code = 1;
  string message = 2;
  repeated Any details = 3;
}
```

Use [canonical gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html) for broad classification. For API field validation, use [`google.rpc.BadRequest`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.BadRequest.html) details. The server currently often returns text-only `InvalidArgument` errors, so structured field details are future work.

```proto
message FieldViolation {
  string field = 1;
  string description = 2;
  string reason = 3;
}
```

Clients branch on status codes and stable reasons, not human-readable text. Collect independent field violations together. Other standard details remain available when needed: [`ErrorInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.ErrorInfo.html), [`RetryInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.RetryInfo.html), [`ResourceInfo`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.ResourceInfo.html), and [`Help`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.Help.html).

Web maps known field violations to controls or selects toast/full-screen presentation. CLI prints human-readable errors or JSON and returns a non-zero status. Both consume same error contract.

## Localization

Localization is future work, but shared contracts must not prevent it. Share message IDs, catalog shape, interpolation, and fallback behavior. Do not branch or translate using server English text; use stable error reasons with server text as fallback.

[`i18next`](https://www.i18next.com/) is a candidate cross-runtime engine. [`react-i18next`](https://react.i18next.com/) can supply it through React context in both Web UI and Ink. Current CLI output already renders through Ink, including classic table output. Non-component code can use the translation engine directly.

Create/select locale before Ink `render()` and place provider around rendered command tree. Prefer per-run instances over mutable process-global locale state. [`google.rpc.LocalizedMessage`](https://googleapis.dev/nodejs/spanner/latest/google.rpc.LocalizedMessage.html) may later provide server-selected text, but does not replace stable client-facing reason codes.

## Deferred Decisions

- Final shared configuration schema and runtime schemas.
- Configuration-file format, source precedence details, and deployment versus server defaults.
- Shared plugin, search, and navigation contracts.
- Runtime validation schema and validation lifecycle.
- Stable validation reasons and field-path conventions.
- Localization catalogs, namespaces, locale selection, and fallback locale.
- Internal package versus public SDK compatibility and versioning policy.
- TUI runtime context switching.
