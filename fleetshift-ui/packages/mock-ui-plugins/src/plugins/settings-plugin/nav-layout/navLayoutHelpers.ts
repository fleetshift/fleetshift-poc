import type {
  FlatNode,
  NavLayoutEntry,
  NavLayoutGroup,
} from "@fleetshift/common";
import { CORE_EXTENSION_META, NodeKind } from "@fleetshift/common";

import type { DragState } from "../useDragTree";

export function resolveLabel(
  pageId: string,
  pageMap: Map<string, { title: string }>,
): string {
  return pageMap.get(pageId)?.title ?? pageId;
}

export function splitNodes(
  nodes: FlatNode[],
  pageMap: Map<string, { scope: string }>,
): { main: FlatNode[]; bottom: FlatNode[] } {
  const main: FlatNode[] = [];
  const bottom: FlatNode[] = [];

  const bottomContainerIds = new Set<string>();
  for (const node of nodes) {
    if (node.depth !== 0) continue;
    if (node.kind === NodeKind.Page && node.pageId) {
      const scope = pageMap.get(node.pageId)?.scope;
      if (scope && CORE_EXTENSION_META[scope]?.navSection === "bottom") {
        bottomContainerIds.add(node.id);
      }
    } else if (node.kind === NodeKind.Group && node.groupMeta) {
      const scope = `${node.groupMeta.pluginKey}-plugin`;
      if (CORE_EXTENSION_META[scope]?.navSection === "bottom") {
        bottomContainerIds.add(node.id);
      }
    }
  }

  for (const node of nodes) {
    const isBottomNode =
      bottomContainerIds.has(node.id) ||
      (node.parentId !== null && bottomContainerIds.has(node.parentId));
    (isBottomNode ? bottom : main).push(node);
  }

  return { main, bottom };
}

export function computeDisplacement(
  topIdx: number,
  dragState: DragState | null,
): number {
  if (!dragState) return 0;
  const S = dragState.sourceTopIndex;
  const D = dragState.dropIndex;
  const h = dragState.blockHeight;

  if (D < S && topIdx >= D && topIdx < S) return h;
  if (D > S + 1 && topIdx >= S + 1 && topIdx < D) return -h;
  return 0;
}

/** Find a NavLayoutGroup in a layout by groupId. */
export function findGroup(
  layout: NavLayoutEntry[],
  groupId: string,
): NavLayoutGroup | undefined {
  for (const entry of layout) {
    if (entry.type === "group" && entry.groupId === groupId) return entry;
  }
  return undefined;
}

/** Collect all group IDs present in a layout. */
export function collectGroupIds(layout: NavLayoutEntry[]): Set<string> {
  const ids = new Set<string>();
  for (const entry of layout) {
    if (entry.type === "group") ids.add(entry.groupId);
  }
  return ids;
}

/**
 * Delete a custom group, promoting its children to top-level pages
 * at the group's position in the layout.
 */
export function deleteGroupFromLayout(
  layout: NavLayoutEntry[],
  groupId: string,
): NavLayoutEntry[] {
  const result: NavLayoutEntry[] = [];
  for (const entry of layout) {
    if (entry.type === "group" && entry.groupId === groupId) {
      // Promote children to top-level pages at this position
      for (const child of entry.children) {
        result.push({ type: "page", pageId: child.pageId });
      }
    } else {
      result.push(entry);
    }
  }
  return result;
}
