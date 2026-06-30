// --- Nav layout types (canonical source) ---

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

/** Persisted override shape stored in IndexedDB (version-tagged). */
export interface NavLayoutOverride {
  version: 1;
  layout: NavLayoutEntry[];
}

/**
 * Data that can be found in IndexedDB:
 * - `NavLayoutOverride` (new format)
 * - `string[]` (legacy flat ordering)
 * - `null` (nothing stored)
 */
export type StoredNavLayout = NavLayoutOverride | string[] | null;

// --- tree utilities (used by NavLayoutEditor + gui NavLayoutTree) ---

export interface FlatNode {
  id: string;
  kind: "page" | "group" | "section";
  depth: number;
  parentId: string | null;
  pageId?: string;
  label?: string;
  groupMeta?: NavLayoutGroup;
}

export const INDENTATION = 36;

/** Flatten a layout into a list of nodes suitable for dnd-kit rendering. */
export function flattenLayout(layout: NavLayoutEntry[]): FlatNode[] {
  const result: FlatNode[] = [];
  for (const entry of layout) {
    if (entry.type === "page") {
      result.push({
        id: entry.pageId,
        kind: "page",
        depth: 0,
        parentId: null,
        pageId: entry.pageId,
      });
    } else if (entry.type === "group") {
      result.push({
        id: entry.groupId,
        kind: "group",
        depth: 0,
        parentId: null,
        label: entry.label,
        groupMeta: entry,
      });
      for (const child of entry.children) {
        result.push({
          id: child.pageId,
          kind: "page",
          depth: 1,
          parentId: entry.groupId,
          pageId: child.pageId,
        });
      }
    } else if (entry.type === "section") {
      result.push({
        id: entry.id,
        kind: "section",
        depth: 0,
        parentId: null,
        label: entry.label,
      });
      for (const child of entry.children) {
        result.push({
          id: child.pageId,
          kind: "page",
          depth: 1,
          parentId: entry.id,
          pageId: child.pageId,
        });
      }
    }
  }
  return result;
}

/** Reconstruct a NavLayoutEntry[] from a flat node list.
 *
 * Children are associated with their parent via `parentId` rather than
 * positional adjacency.  This ensures groups/sections cannot be
 * "interrupted" by a top-level item placed between their children
 * during drag-and-drop — the children always stay with their container.
 */
export function buildLayout(nodes: FlatNode[]): NavLayoutEntry[] {
  // IDs of top-level nodes that can act as parents.
  const containerIds = new Set(
    nodes.filter((n) => !n.parentId).map((n) => n.id),
  );
  const hasValidParent = (node: FlatNode) =>
    node.parentId !== null && containerIds.has(node.parentId);
  const safePageId = (node: FlatNode): string => {
    if (!node.pageId) {
      throw new Error(
        `Invalid child node "${node.id}" in nav layout — missing pageId`,
      );
    }
    return node.pageId;
  };

  // Pre-collect children per parentId (preserving relative order).
  // Nodes whose parentId points to a missing container are treated as
  // top-level rather than silently dropped.
  const childrenByParent = new Map<string, FlatNode[]>();
  for (const node of nodes) {
    if (hasValidParent(node)) {
      let list = childrenByParent.get(node.parentId!);
      if (!list) {
        list = [];
        childrenByParent.set(node.parentId!, list);
      }
      list.push(node);
    }
  }

  const result: NavLayoutEntry[] = [];
  for (const node of nodes) {
    // Skip children with valid parents — emitted with their container.
    if (hasValidParent(node)) continue;

    if (node.kind === "group" && node.groupMeta) {
      const children = (childrenByParent.get(node.id) ?? []).map((c) => ({
        type: "page" as const,
        pageId: safePageId(c),
      }));
      result.push({ ...node.groupMeta, children });
    } else if (node.kind === "section") {
      const children = (childrenByParent.get(node.id) ?? []).map((c) => ({
        pageId: safePageId(c),
      }));
      result.push({
        type: "section",
        id: node.id,
        label: node.label || "Untitled",
        children,
      });
    } else {
      result.push({ type: "page", pageId: safePageId(node) });
    }
  }
  return result;
}

/** Get IDs of all direct children of a container. */
export function getDescendantIds(
  nodes: FlatNode[],
  parentId: string,
): string[] {
  return nodes.filter((n) => n.parentId === parentId).map((n) => n.id);
}

/** Move an element within an array (immutable). */
export function arrayMove<T>(array: T[], from: number, to: number): T[] {
  const result = [...array];
  const [item] = result.splice(from, 1);
  result.splice(to, 0, item);
  return result;
}

/**
 * Move a group/section and its contiguous children as a single block.
 *
 * After `normalizeOrder`, children are always contiguous (immediately
 * following their parent). This function extracts the parent + children
 * as a block, removes it, adjusts the target index, and inserts the
 * block at its new position.
 *
 * For non-container nodes, falls back to plain `arrayMove`.
 */
export function arrayMoveBlock(
  nodes: FlatNode[],
  from: number,
  to: number,
): FlatNode[] {
  const source = nodes[from];
  if (source.kind !== "group" && source.kind !== "section") {
    return arrayMove(nodes, from, to);
  }

  let blockEnd = from + 1;
  while (blockEnd < nodes.length && nodes[blockEnd].parentId === source.id) {
    blockEnd++;
  }
  const blockLen = blockEnd - from;

  const result = [...nodes];
  const block = result.splice(from, blockLen);

  const adjustedTo = to > from ? to - blockLen : to;
  const insertAt = Math.max(0, Math.min(result.length, adjustedTo));
  result.splice(insertAt, 0, ...block);
  return result;
}

/**
 * Normalize a flat node list so children immediately follow their parent.
 *
 * After a drag operation, children may be scattered in the flat array
 * (e.g. when a group is moved but its children stay at their old indices).
 * This function re-collects children under their parent while preserving
 * relative order within each container.
 *
 * Call after every drag-end to keep the visual tree consistent with the
 * data model.
 */
export function normalizeOrder(nodes: FlatNode[]): FlatNode[] {
  // Only treat nodes whose parent actually exists as children.
  // Orphaned nodes (stale parentId) become top-level instead of being dropped.
  const containerIds = new Set(
    nodes.filter((n) => !n.parentId).map((n) => n.id),
  );
  const hasValidParent = (node: FlatNode) =>
    node.parentId !== null && containerIds.has(node.parentId);

  const childrenByParent = new Map<string, FlatNode[]>();
  for (const node of nodes) {
    if (hasValidParent(node)) {
      let list = childrenByParent.get(node.parentId!);
      if (!list) {
        list = [];
        childrenByParent.set(node.parentId!, list);
      }
      list.push(node);
    }
  }

  const result: FlatNode[] = [];
  for (const node of nodes) {
    if (hasValidParent(node)) continue;
    // Orphaned children get promoted to top-level
    result.push(node.parentId ? { ...node, depth: 0, parentId: null } : node);
    const children = childrenByParent.get(node.id);
    if (children) {
      result.push(...children);
    }
  }
  return result;
}

/**
 * Project the depth + parent for a dragged item based on horizontal offset.
 * Groups/sections stay at depth 0; pages can nest to depth 1 under groups/sections.
 */
export function getProjection(
  items: FlatNode[],
  activeId: string,
  dragOffsetX: number,
  initialDepth: number,
): { depth: number; parentId: string | null } {
  const activeIndex = items.findIndex((i) => i.id === activeId);
  const activeItem = activeIndex !== -1 ? items[activeIndex] : null;

  if (
    !activeItem ||
    activeItem.kind === "group" ||
    activeItem.kind === "section"
  ) {
    return { depth: 0, parentId: null };
  }

  const prev = activeIndex > 0 ? items[activeIndex - 1] : null;
  const dragDepth = Math.round(dragOffsetX / INDENTATION);
  const projectedDepth = Math.max(0, Math.min(1, initialDepth + dragDepth));

  let maxDepth = 0;
  let parentId: string | null = null;

  if (prev) {
    if (prev.kind === "group" || prev.kind === "section") {
      maxDepth = 1;
      parentId = prev.id;
    } else if (prev.depth === 1 && prev.parentId) {
      maxDepth = 1;
      parentId = prev.parentId;
    }
  }

  const depth = Math.min(projectedDepth, maxDepth);
  return {
    depth,
    parentId: depth === 1 ? parentId : null,
  };
}

// --- helpers ---

/** Collect every page ID referenced anywhere in a layout. */
export function collectPageIds(layout: NavLayoutEntry[]): Set<string> {
  const ids = new Set<string>();
  for (const entry of layout) {
    if (entry.type === "page") {
      ids.add(entry.pageId);
    } else if (entry.type === "group") {
      for (const child of entry.children) {
        ids.add(child.pageId);
      }
    } else if (entry.type === "section") {
      for (const child of entry.children) {
        ids.add(child.pageId);
      }
    }
  }
  return ids;
}

/** Check whether stored data is the new NavLayoutOverride format. */
export function isNavLayoutOverride(data: unknown): data is NavLayoutOverride {
  return (
    data !== null &&
    !Array.isArray(data) &&
    typeof data === "object" &&
    "version" in data &&
    data.version === 1 &&
    "layout" in data &&
    Array.isArray(data.layout)
  );
}

/**
 * Discriminated container describing where a page lives in the backend layout.
 */
type BackendContainer =
  | { kind: "top" }
  | { kind: "group"; group: NavLayoutGroup }
  | { kind: "section"; section: NavLayoutSection };

/**
 * Build a lookup: pageId -> the backend container it belongs to.
 * Top-level pages map to `{ kind: "top" }`, group children to their group,
 * and section children to their section.
 */
function buildBackendGroupMap(
  backend: NavLayoutEntry[],
): Map<string, BackendContainer> {
  const map = new Map<string, BackendContainer>();
  for (const entry of backend) {
    if (entry.type === "page") {
      map.set(entry.pageId, { kind: "top" });
    } else if (entry.type === "group") {
      for (const child of entry.children) {
        map.set(child.pageId, { kind: "group", group: entry });
      }
    } else if (entry.type === "section") {
      for (const child of entry.children) {
        map.set(child.pageId, { kind: "section", section: entry });
      }
    }
  }
  return map;
}

/**
 * Remove entries whose page IDs are in `removedIds`.
 * For groups/sections, filter children and keep the container
 * (even if empty — editor shows empty groups).
 */
function dropRemoved(
  layout: NavLayoutEntry[],
  removedIds: Set<string>,
): NavLayoutEntry[] {
  const result: NavLayoutEntry[] = [];
  for (const entry of layout) {
    if (entry.type === "page") {
      if (!removedIds.has(entry.pageId)) {
        result.push(entry);
      }
    } else if (entry.type === "group") {
      const filtered = entry.children.filter((c) => !removedIds.has(c.pageId));
      result.push({ ...entry, children: filtered });
    } else if (entry.type === "section") {
      const filtered = entry.children.filter((c) => !removedIds.has(c.pageId));
      result.push({ ...entry, children: filtered });
    }
  }
  return result;
}

/**
 * Insert newly-added pages into the layout.
 *
 * Rules from the design doc:
 * - If the backend places the page in a group, insert it into that group
 *   in the override (create the group entry if it doesn't exist).
 * - If the page is ungrouped in the backend, append to the end of the
 *   layout alphabetically by pageId.
 */
function insertAdded(
  layout: NavLayoutEntry[],
  addedIds: Set<string>,
  backendGroupMap: Map<string, BackendContainer>,
): NavLayoutEntry[] {
  if (addedIds.size === 0) return layout;

  const result = layout.map((entry) => {
    if (entry.type === "group") {
      // Check if any added pages belong to this group
      const newChildren: NavLayoutPage[] = [];
      for (const pageId of addedIds) {
        const container = backendGroupMap.get(pageId);
        if (
          container?.kind === "group" &&
          container.group.groupId === entry.groupId
        ) {
          newChildren.push({ type: "page", pageId });
        }
      }
      if (newChildren.length === 0) return entry;
      return {
        ...entry,
        children: [...entry.children, ...newChildren],
      };
    }

    if (entry.type === "section") {
      // Check if any added pages belong to this section
      const newChildren: { pageId: string }[] = [];
      for (const pageId of addedIds) {
        const container = backendGroupMap.get(pageId);
        if (
          container?.kind === "section" &&
          container.section.id === entry.id
        ) {
          newChildren.push({ pageId });
        }
      }
      if (newChildren.length === 0) return entry;
      return {
        ...entry,
        children: [...entry.children, ...newChildren],
      };
    }

    return entry;
  });

  // Collect which added pages were placed into existing groups/sections
  const placed = new Set<string>();
  for (const entry of result) {
    if (entry.type === "group") {
      for (const child of entry.children) {
        if (addedIds.has(child.pageId)) placed.add(child.pageId);
      }
    } else if (entry.type === "section") {
      for (const child of entry.children) {
        if (addedIds.has(child.pageId)) placed.add(child.pageId);
      }
    }
  }

  // Remaining added pages: check if they belong to a backend container not in override
  const ungrouped: NavLayoutPage[] = [];
  const newGroups = new Map<string, NavLayoutGroup>();
  const newSections = new Map<string, NavLayoutSection>();

  for (const pageId of addedIds) {
    if (placed.has(pageId)) continue;
    const container = backendGroupMap.get(pageId);
    if (container?.kind === "group") {
      // Group exists in backend but not in override — create it
      const existing = newGroups.get(container.group.groupId);
      if (existing) {
        existing.children.push({ type: "page", pageId });
      } else {
        newGroups.set(container.group.groupId, {
          ...container.group,
          children: [{ type: "page", pageId }],
        });
      }
    } else if (container?.kind === "section") {
      // Section exists in backend but not in override — create it
      const existing = newSections.get(container.section.id);
      if (existing) {
        existing.children.push({ pageId });
      } else {
        newSections.set(container.section.id, {
          ...container.section,
          children: [{ pageId }],
        });
      }
    } else {
      ungrouped.push({ type: "page", pageId });
    }
  }

  // Sort ungrouped alphabetically by pageId
  ungrouped.sort((a, b) => a.pageId.localeCompare(b.pageId));

  // Append new groups, new sections, then ungrouped pages
  const groupEntries = [...newGroups.values()] as NavLayoutEntry[];
  const sectionEntries = [...newSections.values()] as NavLayoutEntry[];
  return [...result, ...groupEntries, ...sectionEntries, ...ungrouped];
}

/**
 * Merge a backend layout with a user override.
 *
 * Reconciliation is a set diff on page IDs:
 * - **Added** (in backend, not in override): inserted per backend grouping
 *   or appended ungrouped alphabetically.
 * - **Removed** (in override, not in backend): silently dropped.
 * - Everything else keeps the user's arrangement.
 *
 * If `override` is `null`, the backend layout is returned as-is.
 */
export function mergeLayout(
  backend: NavLayoutEntry[],
  override: NavLayoutOverride | null,
): NavLayoutEntry[] {
  if (!override) return backend;

  const backendIds = collectPageIds(backend);
  const overrideIds = collectPageIds(override.layout);

  // Pages in backend but not in override → added
  const addedIds = new Set<string>();
  for (const id of backendIds) {
    if (!overrideIds.has(id)) addedIds.add(id);
  }

  // Pages in override but not in backend → removed
  const removedIds = new Set<string>();
  for (const id of overrideIds) {
    if (!backendIds.has(id)) removedIds.add(id);
  }

  // Step 1: drop removed pages from override
  let merged = dropRemoved(override.layout, removedIds);

  // Step 2: insert added pages
  const backendGroupMap = buildBackendGroupMap(backend);
  merged = insertAdded(merged, addedIds, backendGroupMap);

  return merged;
}
