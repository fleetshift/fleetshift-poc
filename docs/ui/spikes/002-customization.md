# SPIKE: UI Customization

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

As the number of clusters and plugins grows, the UI gets cluttered. Different users care about different things — an ops engineer managing upgrades does not need to see pipelines front and center, and a developer debugging a deployment does not care about storage volumes. The UI needs to give users some control over what they see and how it is organized, without making the experience overwhelming.

## What was explored

### Fully customizable canvas pages (rejected)

The most extreme approach was explored: let users create entire pages from scratch. Users would drag and drop components onto a "canvas", arrange them freely, save the page with its own route, and place it in the navigation. Essentially every page would be user-composed.

This was found to be problematic:

- **Too complex technically.** Managing arbitrary page layouts, persisting them, syncing them across sessions, and making them work with the plugin system adds a lot of moving parts.
- **Too much cognitive load for users.** Most users do not want to build their own pages. They want something that works out of the box with the option to tweak it. A blank canvas is intimidating and slows people down.

This approach was dropped.

### Customizable dashboards with widgets (viable)

A middle ground that works well: dashboards are customizable, but regular pages are not. Plugins contribute widget extensions — small, focused components that show a summary or key metric. Users can then arrange widgets on their dashboard to suit their needs.

This builds on top of the PatternFly widgetized dashboard work (https://github.com/patternfly/widgetized-dashboard) which is being developed as a PF extension. A working version already exists in the Hybrid Cloud Console.

How it fits with the plugin system:
- Plugins already declare extensions. A new extension type for dashboard widgets lets plugins contribute widgets alongside their pages and tabs.
- The shell renders a widget grid that the user can rearrange, add to, or remove from.
- Widget layout is persisted per user.
- Default layouts can be provided per persona (ops gets cluster health and node stats, dev gets deployment status and pipeline results).

### Navigation customization (viable)

With many plugins enabled across multiple clusters, the nav sidebar gets long. Letting users customize it helps them focus:

- **Show/hide pages.** Users can turn off nav items they do not use.
- **Reorder and group.** Users can drag pages into the order that makes sense for their workflow and organize them into sections.
- **Per-user persistence.** Nav layout is saved on the server and loaded on login.

This was implemented and works. The nav layout is a server-driven config — a list of pages and sections that the shell renders. Users can modify it and the changes are synced back.

## Summary

- **Canvas pages**: explored and rejected. Too complex, too much cognitive load.
- **Customizable dashboards**: viable. Builds on PF widgetized dashboard and the existing extension system. Plugins contribute widgets, users arrange them.
- **Navigation customization**: viable and implemented. Users can show/hide, reorder, and group nav items. Layout is persisted per user.

## Key files

- Nav layout tree (drag and drop): `packages/gui/src/components/NavLayoutTree/`
- Nav layout utilities: `packages/gui/src/components/NavLayoutTree/utilities.ts`
- Dashboard page: `packages/gui/src/pages/Dashboard.tsx`
- User config/preferences persistence: `packages/mock-servers/src/routes/users.ts`
