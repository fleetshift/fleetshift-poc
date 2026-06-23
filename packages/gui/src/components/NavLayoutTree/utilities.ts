import type {
  NavLayoutEntry,
  NavLayoutGroup,
  NavLayoutSection,
} from "../../utils/extensions";

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

export function buildLayout(nodes: FlatNode[]): NavLayoutEntry[] {
  const result: NavLayoutEntry[] = [];
  let currentGroup: NavLayoutGroup | null = null;
  let currentSection: NavLayoutSection | null = null;

  for (const node of nodes) {
    if (node.kind === "group" && node.groupMeta) {
      currentGroup = { ...node.groupMeta, children: [] };
      currentSection = null;
      result.push(currentGroup);
    } else if (node.kind === "section") {
      currentSection = {
        type: "section",
        id: node.id,
        label: node.label || "Untitled",
        children: [],
      };
      currentGroup = null;
      result.push(currentSection);
    } else if (node.depth === 1 && currentGroup) {
      currentGroup.children.push({ type: "page", pageId: node.pageId! });
    } else if (node.depth === 1 && currentSection) {
      currentSection.children.push({ pageId: node.pageId! });
    } else {
      currentGroup = null;
      currentSection = null;
      result.push({ type: "page", pageId: node.pageId! });
    }
  }
  return result;
}

export function getDescendantIds(
  nodes: FlatNode[],
  parentId: string,
): string[] {
  return nodes.filter((n) => n.parentId === parentId).map((n) => n.id);
}

export function arrayMove<T>(array: T[], from: number, to: number): T[] {
  const result = [...array];
  const [item] = result.splice(from, 1);
  result.splice(to, 0, item);
  return result;
}

export function getProjection(
  items: FlatNode[],
  activeId: string,
  dragOffsetX: number,
  initialDepth: number,
): { depth: number; parentId: string | null } {
  const activeIndex = items.findIndex((i) => i.id === activeId);
  const activeItem = activeIndex !== -1 ? items[activeIndex] : null;

  // Groups and sections always stay at depth 0
  if (
    !activeItem ||
    activeItem.kind === "group" ||
    activeItem.kind === "section"
  ) {
    return { depth: 0, parentId: null };
  }

  // Item directly above the active item in its current (reordered) position
  const prev = activeIndex > 0 ? items[activeIndex - 1] : null;

  // Desired depth from horizontal offset
  const dragDepth = Math.round(dragOffsetX / INDENTATION);
  const projectedDepth = Math.max(0, Math.min(1, initialDepth + dragDepth));

  // Nesting is only allowed under groups/sections (not under other items)
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
