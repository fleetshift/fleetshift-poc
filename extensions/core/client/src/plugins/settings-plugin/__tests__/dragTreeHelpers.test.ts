import type { FlatNode } from "@fleetshift/common";
import { NodeKind } from "@fleetshift/common";
import { describe, expect, it } from "vitest";

import { computeKbMove, getBlockLength } from "../dragTreeHelpers";

function page(id: string, depth = 0, parentId: string | null = null): FlatNode {
  return { id, kind: NodeKind.Page, depth, parentId, pageId: id };
}

function group(id: string, children: FlatNode[]): FlatNode[] {
  return [
    { id, kind: NodeKind.Group, depth: 0, parentId: null },
    ...children.map((c) => ({ ...c, depth: 1, parentId: id })),
  ];
}

function section(id: string, children: FlatNode[]): FlatNode[] {
  return [
    { id, kind: NodeKind.Section, depth: 0, parentId: null, label: id },
    ...children.map((c) => ({ ...c, depth: 1, parentId: id })),
  ];
}

// ---------------------------------------------------------------------------
// getBlockLength
// ---------------------------------------------------------------------------

describe("getBlockLength", () => {
  it("returns 1 for a page node", () => {
    const nodes = [page("a"), page("b")];
    expect(getBlockLength(nodes, 0)).toBe(1);
  });

  it("returns header + children for a group", () => {
    const nodes = [...group("g", [page("c1"), page("c2")]), page("x")];
    expect(getBlockLength(nodes, 0)).toBe(3);
  });

  it("returns header + children for a section", () => {
    const nodes = [...section("s", [page("c1")]), page("x")];
    expect(getBlockLength(nodes, 0)).toBe(2);
  });

  it("counts correctly when group is at the end", () => {
    const nodes = [page("x"), ...group("g", [page("c1"), page("c2")])];
    expect(getBlockLength(nodes, 1)).toBe(3);
  });

  it("stops counting at next top-level node", () => {
    const nodes = [...group("g1", [page("a")]), ...group("g2", [page("b")])];
    expect(getBlockLength(nodes, 0)).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// computeKbMove — single item movement
// ---------------------------------------------------------------------------

describe("computeKbMove (single item)", () => {
  it("moves an item down one position", () => {
    const nodes = [page("a"), page("b"), page("c")];
    const result = computeKbMove(nodes, "a", false, 1, 1);
    expect(result!.map((n) => n.id)).toEqual(["b", "a", "c"]);
  });

  it("moves an item up one position", () => {
    const nodes = [page("a"), page("b"), page("c")];
    const result = computeKbMove(nodes, "b", false, 1, -1);
    expect(result!.map((n) => n.id)).toEqual(["b", "a", "c"]);
  });

  it("returns null when moving up from first position", () => {
    const nodes = [page("a"), page("b")];
    expect(computeKbMove(nodes, "a", false, 1, -1)).toBeNull();
  });

  it("returns null when moving down from last position", () => {
    const nodes = [page("a"), page("b")];
    expect(computeKbMove(nodes, "b", false, 1, 1)).toBeNull();
  });

  it("returns null for unknown dragId", () => {
    const nodes = [page("a"), page("b")];
    expect(computeKbMove(nodes, "zzz", false, 1, 1)).toBeNull();
  });

  it("does not mutate the input array", () => {
    const nodes = [page("a"), page("b"), page("c")];
    const copy = nodes.map((n) => ({ ...n }));
    computeKbMove(nodes, "a", false, 1, 1);
    expect(nodes).toEqual(copy);
  });
});

// ---------------------------------------------------------------------------
// computeKbMove — entering groups
// ---------------------------------------------------------------------------

describe("computeKbMove (entering groups)", () => {
  it("moving down onto a group header enters as first child", () => {
    const nodes = [page("a"), ...group("g", [page("c1"), page("c2")])];
    const result = computeKbMove(nodes, "a", false, 1, 1)!;
    expect(result.map((n) => n.id)).toEqual(["g", "a", "c1", "c2"]);
    const moved = result.find((n) => n.id === "a")!;
    expect(moved.depth).toBe(1);
    expect(moved.parentId).toBe("g");
  });

  it("moving down onto a section header enters as first child", () => {
    const nodes = [page("a"), ...section("s", [page("c1")])];
    const result = computeKbMove(nodes, "a", false, 1, 1)!;
    expect(result.map((n) => n.id)).toEqual(["s", "a", "c1"]);
    const moved = result.find((n) => n.id === "a")!;
    expect(moved.depth).toBe(1);
    expect(moved.parentId).toBe("s");
  });

  it("entering an empty group", () => {
    const g: FlatNode = {
      id: "g",
      kind: NodeKind.Group,
      depth: 0,
      parentId: null,
    };
    const nodes = [page("a"), g, page("b")];
    const result = computeKbMove(nodes, "a", false, 1, 1)!;
    expect(result.map((n) => n.id)).toEqual(["g", "a", "b"]);
    expect(result.find((n) => n.id === "a")!.parentId).toBe("g");
  });
});

// ---------------------------------------------------------------------------
// computeKbMove — exiting groups
// ---------------------------------------------------------------------------

describe("computeKbMove (exiting groups)", () => {
  it("moving up past group header promotes to top level", () => {
    const nodes = [...group("g", [page("c1"), page("c2")])];
    // c1 is at index 1, moving up lands on index 0 (the group header)
    const result = computeKbMove(nodes, "c1", false, 1, -1)!;
    expect(result.map((n) => n.id)).toEqual(["c1", "g", "c2"]);
    const moved = result.find((n) => n.id === "c1")!;
    expect(moved.depth).toBe(0);
    expect(moved.parentId).toBeNull();
  });

  it("moving down from last child to a top-level item inherits depth 0", () => {
    const nodes = [...group("g", [page("c1")]), page("x")];
    // c1 is at index 1, x is at index 2 (top-level)
    const result = computeKbMove(nodes, "c1", false, 1, 1)!;
    expect(result.map((n) => n.id)).toEqual(["g", "x", "c1"]);
    const moved = result.find((n) => n.id === "c1")!;
    expect(moved.depth).toBe(0);
    expect(moved.parentId).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// computeKbMove — context inheritance
// ---------------------------------------------------------------------------

describe("computeKbMove (context inheritance)", () => {
  it("swapping children within the same group keeps parentId", () => {
    const nodes = [...group("g", [page("c1"), page("c2")])];
    const result = computeKbMove(nodes, "c1", false, 1, 1)!;
    expect(result.map((n) => n.id)).toEqual(["g", "c2", "c1"]);
    const moved = result.find((n) => n.id === "c1")!;
    expect(moved.depth).toBe(1);
    expect(moved.parentId).toBe("g");
  });

  it("inherits depth from target when swapping top-level items", () => {
    const nodes = [page("a"), page("b"), page("c")];
    const result = computeKbMove(nodes, "a", false, 1, 1)!;
    const moved = result.find((n) => n.id === "a")!;
    expect(moved.depth).toBe(0);
    expect(moved.parentId).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// computeKbMove — block movement
// ---------------------------------------------------------------------------

describe("computeKbMove (block movement)", () => {
  it("moves a group block down past next top-level block", () => {
    const nodes = [...group("g1", [page("a")]), ...group("g2", [page("b")])];
    // g1 block (len 2) moves past g2 block (len 2)
    const result = computeKbMove(nodes, "g1", true, 2, 1)!;
    expect(result.map((n) => n.id)).toEqual(["g2", "b", "g1", "a"]);
  });

  it("moves a group block up past previous top-level item", () => {
    const nodes = [page("x"), ...group("g", [page("a")])];
    const result = computeKbMove(nodes, "g", true, 2, -1)!;
    expect(result.map((n) => n.id)).toEqual(["g", "a", "x"]);
  });

  it("returns null when block is already at the top", () => {
    const nodes = [...group("g", [page("a")]), page("x")];
    expect(computeKbMove(nodes, "g", true, 2, -1)).toBeNull();
  });

  it("returns null when block is already at the bottom", () => {
    const nodes = [page("x"), ...group("g", [page("a")])];
    expect(computeKbMove(nodes, "g", true, 2, 1)).toBeNull();
  });

  it("moves block down past a single top-level page", () => {
    const nodes = [...group("g", [page("a")]), page("x")];
    const result = computeKbMove(nodes, "g", true, 2, 1)!;
    expect(result.map((n) => n.id)).toEqual(["x", "g", "a"]);
  });

  it("does not mutate the input array", () => {
    const nodes = [page("x"), ...group("g", [page("a")])];
    const ids = nodes.map((n) => n.id);
    computeKbMove(nodes, "g", true, 2, -1);
    expect(nodes.map((n) => n.id)).toEqual(ids);
  });
});
