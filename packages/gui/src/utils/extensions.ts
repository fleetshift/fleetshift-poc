/** Derive the plugin key from a plugin name, e.g. "core-plugin" → "core" */
export function pluginKeyFromName(pluginName: string): string {
  return pluginName.replace(/-plugin$/, "");
}

// --- Canvas composition types ---

/** Reference to a plugin module for ScalprumComponent rendering */
export interface ModuleRef {
  scope: string; // plugin name, e.g. "core-plugin"
  module: string; // exposed module, e.g. "./PodList"
  label: string; // display name for palette
}

/** A module placed on the canvas grid */
export interface CanvasModule {
  i: string; // unique instance ID
  x: number;
  y: number;
  w: number;
  h: number;
  moduleRef: ModuleRef;
}

/** A composed page */
export interface CanvasPage {
  id: string;
  title: string;
  path: string; // user-chosen slug, e.g. "pods"
  modules: CanvasModule[];
}

// --- Nav Layout types ---

export interface NavLayoutPage {
  type: "page";
  pageId: string;
}

export interface NavLayoutGroup {
  type: "group";
  groupId: string;
  pluginKey: string;
  label: string;
  children: NavLayoutPage[];
}

export interface NavLayoutSection {
  type: "section";
  id: string;
  label: string;
  children: { pageId: string }[];
}

export type NavLayoutEntry = NavLayoutPage | NavLayoutGroup | NavLayoutSection;

/** Check whether a page exists anywhere in the layout (top-level or inside a section) */
export function isPageInLayout(
  layout: NavLayoutEntry[],
  pageId: string,
): boolean {
  return layout.some(
    (entry) =>
      (entry.type === "page" && entry.pageId === pageId) ||
      (entry.type === "group" &&
        entry.children.some((child) => child.pageId === pageId)) ||
      (entry.type === "section" &&
        entry.children.some((child) => child.pageId === pageId)),
  );
}
