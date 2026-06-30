import { describe, expect, it } from "vitest";

import type { FlatNode, NavLayoutEntry } from "../navLayout";
import {
  arrayMove,
  arrayMoveBlock,
  buildLayout,
  flattenLayout,
  getDescendantIds,
  getProjection,
  normalizeOrder,
} from "../navLayout";

const sampleLayout: NavLayoutEntry[] = [
  { type: "page", pageId: "overview" },
  {
    type: "group",
    groupId: "core-group",
    pluginKey: "core",
    label: "Core",
    children: [
      { type: "page", pageId: "clusters" },
      { type: "page", pageId: "nodes" },
    ],
  },
  {
    type: "section",
    id: "sec-1",
    label: "Admin",
    children: [{ pageId: "settings" }],
  },
];

describe("flattenLayout", () => {
  it("flattens pages, groups, and sections into FlatNode[]", () => {
    const nodes = flattenLayout(sampleLayout);
    expect(nodes).toHaveLength(6);

    expect(nodes[0]).toMatchObject({
      id: "overview",
      kind: "page",
      depth: 0,
      parentId: null,
    });
    expect(nodes[1]).toMatchObject({
      id: "core-group",
      kind: "group",
      depth: 0,
      parentId: null,
      label: "Core",
    });
    expect(nodes[2]).toMatchObject({
      id: "clusters",
      kind: "page",
      depth: 1,
      parentId: "core-group",
    });
    expect(nodes[3]).toMatchObject({
      id: "nodes",
      kind: "page",
      depth: 1,
      parentId: "core-group",
    });
    expect(nodes[4]).toMatchObject({
      id: "sec-1",
      kind: "section",
      depth: 0,
      parentId: null,
      label: "Admin",
    });
    expect(nodes[5]).toMatchObject({
      id: "settings",
      kind: "page",
      depth: 1,
      parentId: "sec-1",
    });
  });

  it("returns empty array for empty layout", () => {
    expect(flattenLayout([])).toEqual([]);
  });
});

describe("buildLayout", () => {
  it("round-trips through flattenLayout → buildLayout", () => {
    const nodes = flattenLayout(sampleLayout);
    const rebuilt = buildLayout(nodes);
    expect(rebuilt).toEqual(sampleLayout);
  });

  it("handles top-level pages without containers", () => {
    const nodes: FlatNode[] = [
      { id: "a", kind: "page", depth: 0, parentId: null, pageId: "a" },
      { id: "b", kind: "page", depth: 0, parentId: null, pageId: "b" },
    ];
    const layout = buildLayout(nodes);
    expect(layout).toEqual([
      { type: "page", pageId: "a" },
      { type: "page", pageId: "b" },
    ]);
  });

  it("keeps group children together when a top-level item interrupts them", () => {
    const groupMeta = {
      type: "group" as const,
      groupId: "core-group",
      pluginKey: "core",
      label: "Core",
      children: [],
    };
    // Simulate a top-level page dropped between group children
    const nodes: FlatNode[] = [
      {
        id: "core-group",
        kind: "group",
        depth: 0,
        parentId: null,
        groupMeta,
      },
      {
        id: "clusters",
        kind: "page",
        depth: 1,
        parentId: "core-group",
        pageId: "clusters",
      },
      // A top-level page interrupting the group:
      {
        id: "overview",
        kind: "page",
        depth: 0,
        parentId: null,
        pageId: "overview",
      },
      {
        id: "nodes",
        kind: "page",
        depth: 1,
        parentId: "core-group",
        pageId: "nodes",
      },
    ];
    const layout = buildLayout(nodes);
    expect(layout).toEqual([
      {
        ...groupMeta,
        children: [
          { type: "page", pageId: "clusters" },
          { type: "page", pageId: "nodes" },
        ],
      },
      { type: "page", pageId: "overview" },
    ]);
  });

  it("preserves child order after reorder within a group", () => {
    const groupMeta = {
      type: "group" as const,
      groupId: "signing",
      pluginKey: "signing",
      label: "Signing",
      children: [],
    };
    // Children in reordered position (B before A)
    const nodes: FlatNode[] = [
      {
        id: "signing",
        kind: "group",
        depth: 0,
        parentId: null,
        groupMeta,
      },
      {
        id: "signing-policies",
        kind: "page",
        depth: 1,
        parentId: "signing",
        pageId: "signing-policies",
      },
      {
        id: "signing-keys",
        kind: "page",
        depth: 1,
        parentId: "signing",
        pageId: "signing-keys",
      },
    ];
    const layout = buildLayout(nodes);
    expect(layout).toEqual([
      {
        ...groupMeta,
        children: [
          { type: "page", pageId: "signing-policies" },
          { type: "page", pageId: "signing-keys" },
        ],
      },
    ]);
  });
});

describe("getDescendantIds", () => {
  it("returns child IDs for a group", () => {
    const nodes = flattenLayout(sampleLayout);
    const ids = getDescendantIds(nodes, "core-group");
    expect(ids).toEqual(["clusters", "nodes"]);
  });

  it("returns child IDs for a section", () => {
    const nodes = flattenLayout(sampleLayout);
    const ids = getDescendantIds(nodes, "sec-1");
    expect(ids).toEqual(["settings"]);
  });

  it("returns empty for a page node", () => {
    const nodes = flattenLayout(sampleLayout);
    const ids = getDescendantIds(nodes, "overview");
    expect(ids).toEqual([]);
  });
});

describe("arrayMove", () => {
  it("moves element from one position to another", () => {
    expect(arrayMove(["a", "b", "c", "d"], 0, 2)).toEqual(["b", "c", "a", "d"]);
  });

  it("does not mutate the original array", () => {
    const original = ["a", "b", "c"];
    arrayMove(original, 0, 2);
    expect(original).toEqual(["a", "b", "c"]);
  });
});

describe("getProjection", () => {
  it("keeps groups at depth 0 regardless of offset", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = getProjection(nodes, "core-group", 100, 0);
    expect(result).toEqual({ depth: 0, parentId: null });
  });

  it("keeps sections at depth 0 regardless of offset", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = getProjection(nodes, "sec-1", 100, 0);
    expect(result).toEqual({ depth: 0, parentId: null });
  });

  it("allows page nesting under a group", () => {
    // After moving overview below core-group:
    // [core-group, overview, clusters, ...]
    const nodes = flattenLayout(sampleLayout);
    const reordered = [nodes[1], nodes[0], ...nodes.slice(2)];
    // overview is now at index 1, right after core-group
    const result = getProjection(reordered, "overview", 50, 0);
    expect(result).toEqual({ depth: 1, parentId: "core-group" });
  });

  it("keeps page at depth 0 with no offset", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = getProjection(nodes, "overview", 0, 0);
    expect(result).toEqual({ depth: 0, parentId: null });
  });
});

describe("normalizeOrder", () => {
  it("returns already-normalized list unchanged", () => {
    const nodes = flattenLayout(sampleLayout);
    expect(normalizeOrder(nodes)).toEqual(nodes);
  });

  it("re-collects children after a group is moved past other items", () => {
    // Simulate dragging a group (core-group at index 1) to after sec-1 (index 4):
    // arrayMove moves only the group header, leaving children behind.
    const nodes = flattenLayout(sampleLayout);
    // Original: [overview, core-group, clusters, nodes, sec-1, settings]
    const scattered = arrayMove(nodes, 1, 4);
    // After arrayMove: [overview, clusters, nodes, sec-1, core-group, settings]
    // clusters/nodes are orphaned from their parent visually.
    const normalized = normalizeOrder(scattered);
    expect(normalized.map((n) => n.id)).toEqual([
      "overview",
      "sec-1",
      "settings",
      "core-group",
      "clusters",
      "nodes",
    ]);
  });

  it("preserves child order within a container", () => {
    const groupMeta = {
      type: "group" as const,
      groupId: "g",
      pluginKey: "test",
      label: "G",
      children: [],
    };
    const nodes: FlatNode[] = [
      {
        id: "page-x",
        kind: "page",
        depth: 0,
        parentId: null,
        pageId: "page-x",
      },
      {
        id: "child-b",
        kind: "page",
        depth: 1,
        parentId: "g",
        pageId: "child-b",
      },
      { id: "g", kind: "group", depth: 0, parentId: null, groupMeta },
      {
        id: "child-a",
        kind: "page",
        depth: 1,
        parentId: "g",
        pageId: "child-a",
      },
    ];
    const normalized = normalizeOrder(nodes);
    expect(normalized.map((n) => n.id)).toEqual([
      "page-x",
      "g",
      "child-b",
      "child-a",
    ]);
  });

  it("handles top-level-only lists", () => {
    const nodes: FlatNode[] = [
      { id: "a", kind: "page", depth: 0, parentId: null, pageId: "a" },
      { id: "b", kind: "page", depth: 0, parentId: null, pageId: "b" },
    ];
    expect(normalizeOrder(nodes)).toEqual(nodes);
  });
});

describe("orphaned children (stale parentId)", () => {
  it("buildLayout promotes orphaned children to top-level pages", () => {
    // Child points to a parent that doesn't exist in the node list
    const nodes: FlatNode[] = [
      { id: "a", kind: "page", depth: 0, parentId: null, pageId: "a" },
      {
        id: "orphan",
        kind: "page",
        depth: 1,
        parentId: "deleted-group",
        pageId: "orphan",
      },
    ];
    const layout = buildLayout(nodes);
    expect(layout).toEqual([
      { type: "page", pageId: "a" },
      { type: "page", pageId: "orphan" },
    ]);
  });

  it("normalizeOrder promotes orphaned children to depth 0", () => {
    const nodes: FlatNode[] = [
      { id: "a", kind: "page", depth: 0, parentId: null, pageId: "a" },
      {
        id: "orphan",
        kind: "page",
        depth: 1,
        parentId: "deleted-group",
        pageId: "orphan",
      },
    ];
    const result = normalizeOrder(nodes);
    expect(result).toEqual([
      { id: "a", kind: "page", depth: 0, parentId: null, pageId: "a" },
      {
        id: "orphan",
        kind: "page",
        depth: 0,
        parentId: null,
        pageId: "orphan",
      },
    ]);
  });

  it("buildLayout throws on child node without pageId", () => {
    const nodes: FlatNode[] = [
      { id: "bad", kind: "page", depth: 0, parentId: null },
    ];
    expect(() => buildLayout(nodes)).toThrow(/missing pageId/);
  });
});

describe("arrayMoveBlock", () => {
  it("moves a group + children as a block forward", () => {
    const nodes = flattenLayout(sampleLayout);
    // Move core-group (index 1, block of 3: group + 2 children) past everything
    const result = arrayMoveBlock(nodes, 1, nodes.length);
    expect(result.map((n) => n.id)).toEqual([
      "overview",
      "sec-1",
      "settings",
      "core-group",
      "clusters",
      "nodes",
    ]);
  });

  it("moves a group + children as a block backward", () => {
    const nodes = flattenLayout(sampleLayout);
    // Move sec-1 (index 4, block of 2) to before overview (index 0)
    const result = arrayMoveBlock(nodes, 4, 0);
    expect(result.map((n) => n.id)).toEqual([
      "sec-1",
      "settings",
      "overview",
      "core-group",
      "clusters",
      "nodes",
    ]);
  });

  it("falls back to arrayMove for non-container nodes", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = arrayMoveBlock(nodes, 0, 4);
    // overview moved forward — same as arrayMove
    expect(result.map((n) => n.id)).toEqual([
      "core-group",
      "clusters",
      "nodes",
      "sec-1",
      "overview",
      "settings",
    ]);
  });

  it("does not mutate the original array", () => {
    const nodes = flattenLayout(sampleLayout);
    const ids = nodes.map((n) => n.id);
    arrayMoveBlock(nodes, 1, 5);
    expect(nodes.map((n) => n.id)).toEqual(ids);
  });

  it("handles moving group to same position (noop)", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = arrayMoveBlock(nodes, 1, 1);
    expect(result.map((n) => n.id)).toEqual(nodes.map((n) => n.id));
  });

  it("clamps target to array bounds", () => {
    const nodes = flattenLayout(sampleLayout);
    const result = arrayMoveBlock(nodes, 1, 100);
    expect(result.map((n) => n.id)).toEqual([
      "overview",
      "sec-1",
      "settings",
      "core-group",
      "clusters",
      "nodes",
    ]);
  });
});

describe("arrayMoveBlock + buildLayout round-trip", () => {
  it("block-moving a group preserves children in the layout", () => {
    const nodes = flattenLayout(sampleLayout);
    const moved = arrayMoveBlock(nodes, 1, 5);
    const layout = buildLayout(moved);
    expect(layout).toEqual([
      { type: "page", pageId: "overview" },
      {
        type: "section",
        id: "sec-1",
        label: "Admin",
        children: [{ pageId: "settings" }],
      },
      {
        type: "group",
        groupId: "core-group",
        pluginKey: "core",
        label: "Core",
        children: [
          { type: "page", pageId: "clusters" },
          { type: "page", pageId: "nodes" },
        ],
      },
    ]);
  });
});

describe("drag-end integration: group drag + normalizeOrder + buildLayout", () => {
  it("dragging a group produces correct layout with children", () => {
    // Full flow: arrayMove → getProjection → normalizeOrder → buildLayout
    const nodes = flattenLayout(sampleLayout);
    // Drag core-group (index 1) to after sec-1 (index 4)
    let reordered = arrayMove(nodes, 1, 4);
    const projection = getProjection(reordered, "core-group", 0, 0);
    reordered = reordered.map((n) =>
      n.id === "core-group"
        ? { ...n, depth: projection.depth, parentId: projection.parentId }
        : n,
    );
    reordered = normalizeOrder(reordered);
    const layout = buildLayout(reordered);

    expect(layout).toEqual([
      { type: "page", pageId: "overview" },
      {
        type: "section",
        id: "sec-1",
        label: "Admin",
        children: [{ pageId: "settings" }],
      },
      {
        type: "group",
        groupId: "core-group",
        pluginKey: "core",
        label: "Core",
        children: [
          { type: "page", pageId: "clusters" },
          { type: "page", pageId: "nodes" },
        ],
      },
    ]);
  });
});
