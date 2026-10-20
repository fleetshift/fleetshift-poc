# OCM Fleet Console

The console is the browser application for OCM Fleet. A React 19 shell provides authentication integration, routing, navigation, and search. Domain pages and contributions are loaded through Scalprum and Module Federation from plugin manifests.

This documentation describes verified implementation and explicitly identified production design direction. A working prototype or an implemented UI control does not establish production readiness of its data source, authorization, or failure handling. See the [architecture glossary](../design/glossary.md) for project and component terminology.

## Current architecture

Start with [Plugin System](plugin-system.md) to understand how domain plugins extend a generic shell, including packages, build output, and bootstrap. The resource manager currently reads a build-generated local registry and serves the packaged frontend under `/app`. Cluster discovery does not currently select the plugin set.

| Reference | Why / read for |
| --- | --- |
| [Plugin discovery and asset distribution](plugin-discovery.md) | Identify available contributions; distinguish registry metadata, asset sources, target signals, and planned distribution |
| [Feature contracts](feature-contracts.md) | Make independent contributions predictable; read for extension metadata, composition, routing, and feature search |
| [Routes and route groups](route-groups.md) | Make new pages discoverable by default; read for generated links, grouping, visibility, ordering, and identity limits |
| [Extension validation](extension-validation.md) | Catch incompatible contributions; distinguish current build/SDK/consumer checks from proposed host-owned runtime validation |
| [Navigation customization](nav-customization.md) | Organize frequent tasks without changing URLs; read for browser-local overrides, reconciliation, reset behavior, and limits |
| [Search-result renderers](search-result-renderer-contract.md) | Separate inventory retrieval from domain display and links; read for renderer contracts, query policy, and fallbacks |
| [Advanced CEL search](cel-advanced-search.md) | Express precise fleet filters; read for editing, execution, history, and future inventory-driven discovery/cache |
| [API access](api-requirements.md) | Query fleet inventory without browser fan-out; distinguish current resource APIs from intended live-target access |
| [Cross-plugin navigation](cross-plugin-navigation.md) | Decouple links from destination URLs and handle missing optional pages; read for scope/module resolution and helper limits |
| [State management](state-management.md) | Avoid accidental coupling and shared-cache assumptions; read for consumer-local state and explicitly shared stores/subscriptions |
| [Dashboard and navigation customization](customization.md) | Tailor existing views to user tasks; read for bounded widget/layout changes and local persistence |

Source-linked references live directly in `docs/ui/` and document implemented behavior and important gaps, with future sections explicitly marked. Remaining files under `spikes/` are outside the current UI reference set: CLI proposals or retired link stubs. They are not active UI implementation documentation.

## Production design direction

Frontend asset distribution will be separated from the resource manager. The [addon UI architecture](../design/addon-ui-architecture.md) describes runtime addon metadata and independently packaged, versioned UI assets. This remains target design; current local asset serving does not implement it.

[Client Asset Separation](../design/client-asset-separation.md) is a **draft** refinement covering an independently trusted shell, same-origin plugin delivery, integrity, and deployment-owned policy. Its Gateway/Operator arrangement differs from the older direct-fetch model. [Plugin discovery](plugin-discovery.md#draft-asset-trust-refinement) records that unresolved distinction. Proposed routes and components are not current APIs or deployed services.

## Diagrams

- [Feature contract](diagrams/feature-contract/feature-contract.c4): current build/bootstrap flow and a separate planned distribution view.
- [Route groups](diagrams/route-groups/route-groups.c4): canonical pages, default navigation, and browser-local presentation.
- [Search](diagrams/search/search.c4): feature indexing, inventory queries, and CEL search.

Each diagram directory is an independent LikeC4 project. Diagrams supplement the source-linked references; they do not add runtime guarantees.
