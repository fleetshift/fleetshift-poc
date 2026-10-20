# Console Customization

Dashboard and navigation customization let users arrange existing widgets and pages around their tasks while retaining plugin-owned views and canonical routes. The console supports these bounded changes; arbitrary user-authored canvas pages were explored and rejected and are not the current page model.

## Navigation

The backend generates a default navigation layout from the packaged plugin registry. It also provides page metadata, including the routes and remote modules to render.

The browser can override the presentation of those pages:

- Reorder pages and groups.
- Create and edit custom groups.
- Move pages between groups.
- Hide items in a collapsed More section and restore them.
- Select icon overrides.

These changes affect presentation, not page identity or routing. Moving a page into another group does not give it a new URL. Hiding a page is not an authorization decision.

Sources:

- [Backend navigation generation](../../server/internal/transport/http/uiconfig.go)
- [Navigation editor](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/NavLayoutEditor.tsx)
- [Group editing](../../extensions/core/client/src/plugins/settings-plugin/GroupFormModal.tsx)
- [Sidebar rendering](../../client/web/src/components/AppNav/AppNav.tsx)

## Navigation persistence

`useNavLayout` reads a versioned override from IndexedDB. It also reads legacy flat ordering and exposes it when no new-format override exists. Clearing the override removes the full-layout customization; legacy ordering remains stored and can still affect the sidebar after reload. See [reset behavior](nav-customization.md#reset-behavior).

The sidebar merges the override with backend entries so removed pages can be dropped and newly available pages incorporated. Plugins can use the shell-owned extension store to share the same subscriptions across Module Federation boundaries.

This is browser-local persistence, not server-side per-user synchronization. The current user-config endpoint generates default navigation rather than saving individual layouts. The local override does not establish account isolation or cross-device synchronization.

Sources: [layout hook](../../sdk/common/src/useNavLayout.ts), [layout merge](../../sdk/common/src/navLayout.ts), [shell-owned store](../../client/web/src/components/Root/ScalprumShell.tsx).

## Dashboard

The overview plugin uses PatternFly's widgetized dashboard with a fixed, code-defined `widgetMapping`. Users can rearrange and resize the configured widgets. The layout is saved in localStorage with a layout version; incompatible stored versions fall back to the default template.

The widget mapping belongs to the overview plugin, not to a generic shell canvas. The current implementation does not discover arbitrary dashboard widget contributions from plugin manifests or load server-provided persona layouts.

Layout customization also does not imply every widget uses live production data; data integration is a separate concern for each widget.

Sources:

- [Widget mapping and layout component](../../extensions/core/client/src/plugins/overview-plugin/OverviewDashboard.tsx)
- [Default template and localStorage persistence](../../extensions/core/client/src/plugins/overview-plugin/dashboardLayout.ts)
- [Overview data composition](../../extensions/core/client/src/plugins/overview-plugin/useFleetData.ts)

## Related references

- [Extension types](../../sdk/build-utils/src/extensions/types.ts): plugin-defined grouping contracts.
- [Cross-plugin navigation](cross-plugin-navigation.md): page resolution independent of custom layout.
- [State ownership](state-management.md): shared store and subscription boundaries.
