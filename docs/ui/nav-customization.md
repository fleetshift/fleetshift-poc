# Navigation Customization

Navigation customization lets users organize the sidebar around frequent tasks without changing canonical page URLs. The Navigation settings page edits a browser-local layout: users can reorder pages and groups, move pages between groups, create custom groups, choose icons, and hide or restore entries; these preferences also affect feature-search grouping.

No customization is needed for a new route to appear. [Routes and Route Groups](route-groups.md#new-standalone-route-to-default-link) explains declaration → `pluginPages` → default `navLayout` → visible link, including Main/Bottom ordering, disabled plugins, and configuration-fetch timing. Customization starts from that generated default.

The current UI is [NavLayoutEditor](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/NavLayoutEditor.tsx), mounted by [SettingsPage](../../extensions/core/client/src/plugins/settings-plugin/SettingsPage.tsx). It uses shared tree utilities and custom pointer/keyboard drag handling with Motion.

## Layout and persistence

The canonical [layout types](../../sdk/common/src/navLayout.ts) include page, group, section, and More entries. Groups contain page children; More can contain layout entries. The editor writes a versioned override:

```typescript
interface NavLayoutOverride {
  version: 1;
  layout: NavLayoutEntry[];
}
```

An example stored layout is:

```json
{
  "version": 1,
  "layout": [
    {
      "type": "group",
      "groupId": "user-my-tools",
      "pluginKey": "",
      "label": "My Tools",
      "description": "Frequently used fleet pages",
      "children": [{ "type": "page", "pageId": "core.clusters" }]
    },
    { "type": "more", "children": [{ "type": "page", "pageId": "overview.overview" }] }
  ]
}
```

IndexedDB database `ocm-fleet-extensions` stores the override in `nav-layout` under key `layout`. Legacy flat ordering remains in `nav-order` under key `order`. A new override takes precedence. Storage is local to the browser origin and is not keyed by authenticated user or synchronized with the server.

The hook accepts the shell-owned extension store so the settings plugin and shell can share subscriptions across Module Federation boundaries. The stored-shape check verifies version and layout-array presence, not every nested entry invariant.

Sources: [storage](../../sdk/common/src/extensionInstall.ts), [useNavLayout](../../sdk/common/src/useNavLayout.ts), [shape check](../../sdk/common/src/navLayout.ts).

## Reconciliation with backend navigation

`mergeLayout(backend, override)` compares page-ID sets:

1. Without an override, it returns the backend layout.
2. Pages absent from the backend are removed from the override, including inside More.
3. Newly available pages enter the active layout in their backend-defined group or section. Missing containers are created when needed.
4. New ungrouped pages are appended alphabetically by page ID.
5. Existing pages retain the user's arrangement. Empty group containers remain for editing.
6. More is extracted before adding pages and appended afterward, so new pages are never automatically hidden.

This is page-ID reconciliation, not full metadata synchronization. Existing group metadata is retained from the override. Empty groups are hidden by the sidebar but remain available in the editor.

`AppNav` honors legacy flat ordering when there is no full override. The current editor and search provider do not apply that legacy order, so their initial view can differ from the sidebar until a full override is saved.

Sources: [mergeLayout](../../sdk/common/src/navLayout.ts), [AppNav](../../client/web/src/components/AppNav/AppNav.tsx), [SearchProvider](../../client/web/src/components/Search/SearchProvider.tsx).

## Editing groups and pages

Custom groups use `user-` plus a slug of the name. The form requires a nonempty name producing a usable slug and a description; keywords and icon are optional. Creating or renaming a group checks for an existing group ID. Renaming changes its generated ID.

Only custom groups expose Edit and Delete actions. Deleting one promotes its children to top-level pages at the group's position after confirmation. Plugin-defined groups keep their manifest-owned identity and label, but their children can be rearranged and their icon can be selected in the editor.

Main and Bottom are separate drag lists. Their split is determined by plugin-scope `CORE_EXTENSION_META`; a plugin-defined group inherits the section inferred from its plugin key, while a custom group defaults to Main. Cross-section dragging is not provided. Groups move with their children, and pages can be nested or unnested within the supported tree depth.

Sources: [group form](../../extensions/core/client/src/plugins/settings-plugin/GroupFormModal.tsx), [editor helpers](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/navLayoutHelpers.ts), [row actions](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/TreeItem.tsx), [drag handling](../../extensions/core/client/src/plugins/settings-plugin/useDragTree.ts).

## Hidden items and More

Dragging an entry below the Hidden divider hides it; row menus also provide Hide and Restore. An entire group can be hidden, or an individual group child can become an ungrouped hidden page. Hidden rows are visually muted. The editor's Bottom list does not expose the same hide/restore controls as Main.

More uses the fixed ID `_more` and renders at the end of the main navigation list, before the separate bottom list. It is omitted when it has no known visible children. Otherwise it expands when one of its pages is active, using each page's canonical route. Hidden pages remain registered, directly addressable, and searchable.

Sources: [editor](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/NavLayoutEditor.tsx), [More rendering](../../client/web/src/components/AppNav/AppNavMore.tsx), [sidebar placement](../../client/web/src/components/AppNav/AppNav.tsx).

## Icon selection and current limits

The icon gallery reads a generated flat list of PascalCase PF names, derives search words from those names, and uses a cached Orama index across modal openings. It displays at most 200 results. IntersectionObserver loads icon components near the viewport; the grid is capped rather than fully DOM-virtualized. A shared loader caches loaded components and in-flight imports.

Pages store `iconOverride`; groups store `icon`. Sidebar page overrides take priority over plugin icons when enabled, while disabled pages show a puzzle icon. Group icon behavior is less complete:

- Custom groups attempt to render an icon in the sidebar title and use it in search.
- The sidebar converts custom group icons as kebab-case slugs, although the gallery stores PascalCase names. This can produce an invalid doubled `Icon` suffix and leave the icon missing.
- Plugin-defined group icons can be saved but are not used by the current sidebar or group-search icon selection.
- Tree rows show labels and action menus, not a preview of each selected icon.

Sources: [gallery](../../extensions/core/client/src/plugins/settings-plugin/IconGalleryModal.tsx), [icon loader](../../sdk/common/src/pfIconLoader.ts), [page icons](../../client/web/src/components/AppNav/AppNavItem.tsx), [group icons](../../client/web/src/components/AppNav/AppNavGroup.tsx), [search icons](../../client/web/src/components/Search/SearchProvider.tsx).

## Reset behavior

Reset position is available for active page rows after an override exists. After confirmation, the editor removes that page from the saved arrangement and reconciles it back into its backend container. It does not restore an exact original sibling index; insertion appends within an existing container and clears that page's saved icon override.

Reset all clears the new layout override after confirmation. It does not clear legacy flat ordering. If legacy data exists, the sidebar can therefore return to that saved order rather than a completely fresh default.

Source: [reset handlers](../../extensions/core/client/src/plugins/settings-plugin/nav-layout/NavLayoutEditor.tsx).

## Routing and search

Moving `/settings/navigation` into a custom group leaves it at `/settings/navigation` (browser URL `/app/settings/navigation`). The router and cross-plugin resolver use backend `pluginPages`; canonical plugin-group redirects use backend `navLayout`. Custom groups have no route.

Feature search rebuilds from the merged layout and includes hidden entries. It reflects group membership, not sidebar ordering: Orama relevance determines matches. Group search links currently use the first child's first path segment, which can be incorrect for custom or rearranged groups. Sidebar group auto-expansion checks the group's URL prefix rather than its actual children, so it can also fail after cosmetic moves.

Sources: [routing](../../client/web/src/routes/ConsoleRoutes.tsx), [resolver](../../client/web/src/components/Root/ScalprumShell.tsx), [search indexing](../../client/web/src/components/Search/SearchProvider.tsx), [group active state](../../client/web/src/components/AppNav/AppNavGroup.tsx). See [Route Groups](route-groups.md) and the [search diagram](diagrams/search/search.c4).
