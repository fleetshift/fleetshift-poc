# Extension Validation and Failure Handling

Extension validation aims to catch incompatible contributions before consumers use them; downloaded JavaScript can bypass the supplied build tools, so build checks alone cannot enforce runtime contracts. The console currently uses build-time metadata checks, SDK CodeRef resolution, and consumer-specific runtime guards with different coverage; host-owned runtime contract validation remains future work, and current checks do not establish a universal readiness, authorization, or crash-isolation guarantee for plugin code.

See [Feature Contracts](feature-contracts.md) for declarations and the [feature-contract diagram](diagrams/feature-contract/feature-contract.c4) for build and runtime boundaries.

## Build-time checks

`FleetPlugin` calls `validateExtensionSet` in its constructor before delegating to the Dynamic Plugin SDK. Typed builders also validate their inputs. Invalid metadata fails construction rather than silently passing through the wrapper.

| Check | Current coverage |
|-------|------------------|
| Base identity | Required nonempty `id` and `label`; ID pattern `/^[a-z][a-z0-9-]*$/` |
| CodeRef syntax | Checked for required known fields and selected optional fields |
| Exposed module references | Direct property CodeRefs must reference keys in the same plugin's `exposedModules` |
| Duplicate IDs | Rejected within the extension set, with grouped route IDs keyed by group plus ID |
| Route groups | String-valued route group references must exist in the same extension set; duplicate route IDs within a group rejected |
| Type-specific metadata | Known builders validate their required fields |
| Custom extension types | Default validator applies shared base-property checks |

Examples of type-specific requirements:

- Routes require component and icon CodeRefs.
- Cluster providers require a description and icon, card, and wizard CodeRefs.
- Setup steps require path, component, a dependency-ID array, and boolean `requiresAuth`.
- Search renderers require a resource type and resolver CodeRef.
- Cluster-detail tabs have their own component and tab-metadata checks.

```typescript
createClusterProvider({
  id: "kind",
  label: "Kind",
  description: "Create a local Kind cluster for development and testing.",
  icon: { $codeRef: "KindProviderCard.KindIcon" },
  card: { $codeRef: "KindProviderCard.default" },
  wizard: { $codeRef: "CreateClusterWizard.default" },
});
```

Sources: [FleetPlugin](../../sdk/build-utils/src/extensions/FleetPlugin.ts), [validators](../../sdk/build-utils/src/extensions/validate.ts), [types](../../sdk/build-utils/src/extensions/types.ts), [search-renderer validator](../../sdk/build-utils/src/extensions/searchResultRenderer.ts), [tab validator](../../sdk/build-utils/src/extensions/clusterDetailTab.ts).

### What the build does not prove

The set-level CodeRef scan examines direct property values, not arbitrary nested objects. The route validator does not recursively validate `extensionPoints`. The known-field CodeRef checks require an encoded reference matching `ModuleName.exportName`, but the exposed-module check only verifies the module key. It does not load the module or inspect the named export.

Optional description/keyword types, navigation targets, route suffixes, backend health, and general component renderability are not comprehensively validated by this layer. TypeScript declarations are authoring aids, not runtime checks of downloaded JavaScript.

Grouped route deduplication is also weaker than end-to-end page identity: the server uses `{pluginKey}.{id}` without the group. Distinct groups can therefore pass build checks while producing duplicate page IDs. See [Route Groups](route-groups.md).

## SDK CodeRef resolution

The current application depends on Dynamic Plugin SDK 9.1. The SDK decodes encoded CodeRefs recursively, including references inside nested property objects. A decoded CodeRef loads the exposed module from its Module Federation container and reads the named export.

Consumers use the SDK in two steps:

```typescript
const candidates = useExtensions(isClusterProviderExtension);
const [extensions, resolved, errors] = useResolvedExtensions(candidates);
```

`useResolvedExtensions` recursively resolves the candidate extensions' CodeRefs. By default, extensions with CodeRef resolution errors are excluded from the returned list. The hook returns an error collection and logs resolution errors. A failed property is set to `undefined` during resolution; consumers opting to include errored extensions would need to handle that explicitly.

This catches failures such as missing modules or named exports. Successful resolution does not prove that an export is a React component, that a function returns the expected value, or that its later execution succeeds. Resolving a validation-shaped function would not automatically call it.

Nested CodeRefs do not require a generated `_validators` property. The application has no two-pass self/provider validation hook, mandatory `isValidExtension`, extension-point `validate` contract, or shared validation-success cache.

Sources: [SDK dependency](../../client/web/package.json), [route consumer](../../client/web/src/components/AppNav/AppNav.tsx), [provider consumer](../../extensions/core/client/src/plugins/core-plugin/CreateClusterModal.tsx). SDK resolution behavior is implemented by `decodeCodeRefs`, `resolveCodeRefValues`, and `useResolvedExtensions` in `@openshift/dynamic-plugin-sdk`.

## Type guards and consumers

Application guards such as `isRouteExtension`, `isClusterProviderExtension`, and `isSearchResultRendererExtension` test the extension type string. Narrowing to `RouteExtension` or another extension type does not structurally validate properties or test backend readiness.

Provider and cluster-detail components consume resolved exports, often through type assertions. The provider modal sorts candidates, handles a missing selected provider, and renders its card or wizard. It does not call a provider-supplied structural validator before rendering.

Sources: [route guard](../../client/web/src/extensions/isRouteExtension.ts), [provider guard](../../client/web/src/extensions/isClusterProviderExtension.ts), [renderer guard](../../client/web/src/extensions/isSearchResultRendererExtension.ts), [provider modal](../../extensions/core/client/src/plugins/core-plugin/CreateClusterModal.tsx), [cluster-detail tabs](../../extensions/core/client/src/plugins/core-plugin/ClusterDetailPage.tsx).

## Consumer-specific safeguards

| Consumer | Current behavior |
|----------|------------------|
| Plugin page | Checks scope availability and browser-local enablement; shows loading, unavailable, state-error, or load-error UI |
| Setup routing | Excludes missing/cyclic dependency chains; warns about duplicate IDs and keeps the first dependency-map definition |
| Inventory search | Catches resolver-call exceptions and uses a disabled fallback result |
| Plugin loader | Attempts all configured plugin loads; catches aggregate load failure before ending initial loading |
| Search index | Resolves routes/providers through the SDK; has no additional universal readiness gate |

These checks are scoped to their call sites. For example, a successful resolver call does not protect later rendering of its description or icon, and page availability does not imply permission to perform an operation.

Sources: [PluginPage](../../client/web/src/pages/PluginPage.tsx), [setup dependency resolution](../../client/web/src/utils/setupExtensions.ts), [inventory search](../../client/web/src/hooks/useInventorySearch.tsx), [PluginLoader](../../client/web/src/components/Root/PluginLoader.tsx), [SearchProvider](../../client/web/src/components/Search/SearchProvider.tsx).

## Diagnostics and interpretation

Build errors identify invalid metadata and missing exposed-module keys. SDK console errors identify CodeRef resolution failures. Setup dependency warnings and inventory resolver exceptions come from their respective consumers.

The browser route `/app/debug` shows plugin registry entries, Scalprum configuration, plugin pages, and backend navigation layout. It has no shared extension-validation results panel.

Do not interpret a visible search entry, registered route, enabled preference, or resolved CodeRef as an authorization decision or continuous health check. Search can expose pages whose local enablement is off, and the page then presents its enablement UI. Backend operation authorization remains a separate boundary.

Sources: [DebugPage](../../client/web/src/pages/DebugPage.tsx), [page resolver](../../client/web/src/components/Root/ScalprumShell.tsx), [feature contracts](feature-contracts.md).

## Future work: host-owned runtime contract validation

**Proposal only; not implemented.** The interfaces, aliases, declarations, hook, cache, and examples below describe future contracts, not current exports from `@ocm-fleet/common` or `@ocm-fleet/build-utils`. They preserve the original proposal's `isValidExtension`, extension-point `validate`, and `useValidatedExtensions` design, with the ownership and caching revisions called out below. They are design examples, not a runnable implementation.

### Ownership and acceptance boundary (proposed)

The host must validate contributions before exposing them to consumers: manifest and extension metadata, required fields, property shapes, supported extension-point compatibility, and applicable resolved export shapes. JavaScript plugins may bypass the provided build tools; neither `FleetPlugin` nor an extension's own predicate can be the enforcement boundary.

- **Host-owned mandatory checks:** validate the envelope and supported contract, then enforce the accepted shape at consumption. The host owns built-in contracts and controls which contract owners may register extension points. Unknown or incompatible contracts are rejected at that boundary.
- **Contract-owner validation:** the route declaring an extension point supplies `validate`. Here “provider validation” means the provider of the extension point, not the incoming cluster-provider plugin. Its callback implements the accepted shape and can reject a contribution; the contributor cannot select or replace it. Host checks still apply even if that callback returns success.
- **Optional self-validation:** a contribution may supply `isValidExtension` for supplemental prerequisites or dynamic availability. Absence is allowed; presence requires a resolvable callable with a valid result. It can further restrict exposure, never certify structural safety or override rejection.

All consumed extension types need an accepted structural contract, including types without a route-owned extension point. Those use a host-owned contract. An arbitrary plugin-supplied function returning `true` is not evidence that JavaScript is safe. Structural validation does not sandbox code, prove component behavior, prevent every crash, or authorize backend operations.

### Declaration and callback contracts (proposed)

Retain the original base-properties and non-overridable-extras design, but make self-validation optional. These local aliases illustrate proposed replacements for the current build-utils declarations; the imported current base type preserves existing metadata fields.

```typescript
import type {
  BaseExtensionProperties as CurrentBaseExtensionProperties,
  EncodedCodeRef,
} from "@ocm-fleet/build-utils";

type ExtensionValidationResult = { valid: boolean; reason?: string };
type SelfValidation = () => Promise<ExtensionValidationResult>;
type ResolvedContribution = {
  type: string;
  properties: Record<string, unknown>;
};
type ProviderValidation = (
  extension: ResolvedContribution,
) => ExtensionValidationResult | Promise<ExtensionValidationResult>;

type BaseExtensionProperties = CurrentBaseExtensionProperties & {
  isValidExtension?: EncodedCodeRef;
};
type ExtensionProperties<TExtras extends object = Record<never, never>> =
  BaseExtensionProperties & Omit<TExtras, keyof BaseExtensionProperties>;
type FleetExtension<TType extends string, TExtras extends object> = {
  type: TType;
  properties: ExtensionProperties<TExtras>;
};
type ExtensionPointDeclaration = {
  description: string;
  type: string;
  validate: EncodedCodeRef;
};
```

`SelfValidation` retains the original no-argument, asynchronous signature. `ProviderValidation` receives the full resolved extension, not encoded CodeRefs or only its properties. Revision from the original synchronous boolean predicate: it may be asynchronous and returns structured reasons, so the infrastructure can report failures without relying on callback-local console messages. It is not an SDK `ExtensionPredicate`, which selects candidate types synchronously.

Both callbacks return `{ valid: true }` or `{ valid: false, reason }`. Thrown exceptions, rejected promises, malformed results, and non-callable declared validators reject the contribution for that check and produce diagnostics. An absent required owner validator rejects that extension point; an absent optional self-validator does not. Pending checks must not expose their candidate. Cancellation and timeout policy remain to be designed; one slow contribution should not block unrelated accepted contributions.

### Example 1: declarations on both sides (proposed)

These are future manifest-shaped declarations using the aliases above, not calls to today's builders. A future builder would accept the same fields after its types and checks are updated. The route belongs to the contract-owner plugin; the provider belongs to a separate contributor plugin.

```typescript
type RouteExtras = {
  component: EncodedCodeRef;
  icon: EncodedCodeRef;
  extensionPoints?: Record<string, ExtensionPointDeclaration>;
};
type ClusterProviderExtras = {
  description: string;
  icon: EncodedCodeRef;
  card: EncodedCodeRef;
  wizard: EncodedCodeRef;
};

const clustersRoute: FleetExtension<"ocm-fleet.route", RouteExtras> = {
  type: "ocm-fleet.route",
  properties: {
    id: "clusters",
    label: "Clusters",
    component: { $codeRef: "ClustersPage.default" },
    icon: { $codeRef: "ClustersPage.ClustersIcon" },
    extensionPoints: {
      providers: {
        description: "Cluster creation providers",
        type: "ocm-fleet.cluster-provider",
        validate: { $codeRef: "ClusterProviderValidator.validate" },
      },
    },
  },
};

const exampleProvider: FleetExtension<
  "ocm-fleet.cluster-provider", ClusterProviderExtras
> = {
  type: "ocm-fleet.cluster-provider",
  properties: {
    id: "example",
    label: "Example Kubernetes Provider",
    description: "Create a Kubernetes cluster using an example provider.",
    icon: { $codeRef: "ExampleProvider.Icon" },
    card: { $codeRef: "ExampleProvider.Card" },
    wizard: { $codeRef: "ExampleProvider.Wizard" },
    isValidExtension: { $codeRef: "ExampleValidation.isValid" }, // Optional.
  },
};
```

The owner exposes `ClustersPage` and `ClusterProviderValidator`; the contributor exposes `ExampleProvider` and `ExampleValidation` in their respective `exposedModules`. References resolve within the declaring plugin, not the other plugin. Future build checks should validate supplied self-validation CodeRefs and required extension-point validators, including nested references against exposed-module keys. Today's direct-property scan does not already do that.

The SDK already recursively resolves `extensionPoints.providers.validate`. The proposed runtime should consume that resolved callback; no `_validators` hoisting or manual CodeRef parsing is needed. No backend registry change is established here as a prerequisite for nested resolution.

### Example 2: owner acceptance and supplemental availability (proposed)

Illustrative callbacks using the proposed types above. This owner contract deliberately accepts function exports for `icon`, `card`, and `wizard`. A production contract must explicitly define other accepted React export forms, such as memo/forwardRef objects; `typeof value === "function"` does not prove renderability or prop compatibility.

```typescript
// Owner plugin: ClusterProviderValidator.validate
export const validate: ProviderValidation = (extension) => {
  if (extension.type !== "ocm-fleet.cluster-provider") {
    return { valid: false, reason: "Unexpected extension type" };
  }
  for (const field of ["id", "label", "description"]) {
    const value = extension.properties[field];
    if (typeof value !== "string" || value.length === 0) {
      return { valid: false, reason: `Missing nonempty ${field}` };
    }
  }
  for (const field of ["icon", "card", "wizard"]) {
    if (typeof extension.properties[field] !== "function") {
      return { valid: false, reason: `Expected resolved function: ${field}` };
    }
  }
  return { valid: true };
};

// Contributor plugin: ExampleValidation.isValid
// Illustrative endpoint, not an existing OCM Fleet API contract.
export const isValid: SelfValidation = async () => {
  try {
    const response = await fetch("/api/v1/integrations/example/status");
    return response.ok
      ? { valid: true }
      : { valid: false, reason: `Example backend returned ${response.status}` };
  } catch {
    return { valid: false, reason: "Example backend unreachable" };
  }
};
```

The first callback checks shape without invoking the contributed components. The second reports availability at one instant. A successful status request is neither permission to create a cluster nor a lasting readiness assertion; later operations still handle backend errors and authorization.

### Consumption and execution flow (proposed)

Retain the original tuple-style hook and automatic owner-validator lookup. Add an explicit owner/slot binding so two routes declaring the same type do not silently compete. The host verifies that binding against its accepted contract registry; it is not a caller-supplied validator or an authority established by a type guard.

```typescript
import type {
  CodeRef, Extension, ExtensionPredicate, LoadedAndResolvedExtension,
} from "@openshift/dynamic-plugin-sdk";
import type {
  ClusterProviderCardProps, ClusterProviderWizardProps,
} from "@ocm-fleet/common";
import type { ComponentType } from "react";

type ExtensionPointBinding = {
  pluginName: string;
  routeId: string;
  slot: string;
};
declare function useValidatedExtensions<T extends Extension>(
  predicate: ExtensionPredicate<T>,
  owner: ExtensionPointBinding,
): [
  extensions: LoadedAndResolvedExtension<T>[],
  loaded: boolean,
  rejections: ValidationRejection[],
];

type ClusterProviderExtension = Extension<"ocm-fleet.cluster-provider", {
  id: string;
  label: string;
  description: string;
  icon: CodeRef<ComponentType>;
  card: CodeRef<ComponentType<ClusterProviderCardProps>>;
  wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  isValidExtension?: CodeRef<SelfValidation>;
}>;
const isClusterProviderExtension: ExtensionPredicate<ClusterProviderExtension> =
  (extension): extension is ClusterProviderExtension =>
    extension.type === "ocm-fleet.cluster-provider";

// Called inside a future React consumer hook or component.
function useClusterProviders() {
  const [providers, loaded, rejections] = useValidatedExtensions(
    isClusterProviderExtension,
    { pluginName: "core-plugin", routeId: "clusters", slot: "providers" },
  );
  // providers[n].properties.card is now a component, not a CodeRef loader.
  return { providers, loaded, rejections };
}
```

`ValidationRejection` is defined below. The SDK's decoded `CodeRef<T>` is a loader returning `Promise<T>`; `LoadedAndResolvedExtension<T>` replaces loaders with their exported values. Encoded manifest objects in Example 1 are a separate representation. The generic describes the host-registered contract: a type-string predicate or arbitrary `T` cannot establish that contract's validity by itself.

Proposed execution order:

1. Host-check metadata and contract binding before treating input as a supported contribution. Preserve identities for malformed entries too.
2. On demand, select candidates and resolve their CodeRefs and the bound owner's nested `validate` through the SDK. Collect SDK errors rather than silently losing excluded candidates.
3. Enforce host resolved-export checks and invoke the bound owner's `validate(extension)`. Missing, throwing, malformed, or rejecting validators filter that contribution before consumer exposure.
4. For structural survivors, call optional `isValidExtension()`. Reject declared but invalid callbacks and unsuccessful outcomes; skip this step when absent.
5. Return only accepted contributions, completion state, and rejections. Share diagnostic outcomes across consumers; discard stale async completions after replacement or invalidation.

On-demand resolution preserves laziness; it is not a global cold-start sweep. The host must enforce structural checks at its supported contribution exposure paths, not rely solely on every developer remembering this hook. Navigation, search, setup, and provider consumers would need an explicit migration. The original metadata-only search proposal is not current behavior: today's search resolves route/provider extensions as described above. Metadata indexing and later code consumption must retain their distinct validation boundaries.

### Outcomes, debug diagnostics, and caching (proposed)

Retain source-attributed rejections and a shared results store for a future `/app/debug` validation panel. Expand sources beyond the original `self`/`provider` pair so host contract and SDK resolution failures are distinguishable. This is a proposed diagnostic shape, not today's error collection.

```typescript
type ValidationRejectionSource = "host" | "resolution" | "provider" | "self";
type ValidationIdentity = {
  pluginName: string;
  pluginVersion: string;
  artifactDigest: string;
  extensionId: string;
  extensionType: string;
  contentDigest: string;
  hostContractRevision: string;
  owner: ExtensionPointBinding;
  ownerArtifactDigest: string;
  consumerContractRevision: string;
  reloadGeneration: number;
};
type ValidationRejection = {
  identity: ValidationIdentity;
  source: ValidationRejectionSource;
  code: "contract" | "resolution" | "rejected" | "exception" | "invalid-result";
  reason: string;
  propertyPath?: string;
  timestamp: number;
};
type StructuralValidationCache = Map<
  string, // Canonical serialization/hash of the complete ValidationIdentity.
  { valid: true; timestamp: number }
>;
```

For example, a provider rejection would carry `source: "provider"`, `code: "rejected"`, `propertyPath: "properties.wizard"`, and `reason: "Expected resolved function: wizard"`. SDK import failures instead use `source: "resolution"`; thrown self-checks use `source: "self"` and `code: "exception"`. Pre-identity metadata failures need an ingestion identity based on artifact and entry location rather than trusting missing or malformed `id` fields.

The proposed panel would show pending/accepted/rejected counts, plugin and contribution identity, reason, source, and time. A shell-exposed Scalprum shared store and proposed `useValidationResults` accessor would make results available across Module Federation boundaries. Reasons are developer-facing diagnostics, not sensitive payloads or backend authorization decisions. User-facing unavailable states remain a separate UX decision.

Cache successful **structural** results only for the exact plugin artifact and version, extension/content identity, owner artifact and slot, host contract revision, consumer contract revision, and reload generation. A version string or extension ID alone is insufficient. Owner callback caching requires a structural, deterministic contract; callbacks depending on live state must be re-evaluated instead.

Invalidate affected results and in-flight work on plugin replacement/removal, artifact or manifest changes, owner/contract changes, and reload, even when the version string stays the same. Registry/orchestration notifications may trigger invalidation but cannot be its only source. Session-local entries should not survive a page reload without fresh identity verification.

This replaces the original `SelfValidationCache` keyed only by extension ID. Dynamic readiness, credentials, feature flags, and permissions must not be cached indefinitely after success. Re-evaluate on relevant state changes and subsequent consumption, with explicit expiry/retry policy if short-lived caching is introduced; failed self-checks must be recoverable. Bounded retries and pending-check handling remain open design work. No permanent readiness guarantee follows from structural cache success.
