import { describe, expect, it } from "vitest";

import type { FlatNode, NavLayoutEntry } from "../navLayout";
import {
  arrayMove,
  buildLayout,
  flattenLayout,
  getDescendantIds,
  getProjection,
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
