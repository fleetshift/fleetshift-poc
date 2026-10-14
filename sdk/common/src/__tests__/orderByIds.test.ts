import { describe, expect, it } from "vitest";

import { orderByIds } from "../orderByIds";

interface Item {
  id: string;
  title: string;
}

const make = (id: string, title: string): Item => ({ id, title });

describe("orderByIds", () => {
  it("sorts alphabetically when savedOrder is null", () => {
    const items = [
      make("c", "Charlie"),
      make("a", "Alpha"),
      make("b", "Bravo"),
    ];
    const result = orderByIds(items, null, "title");
    expect(result.map((i) => i.id)).toEqual(["a", "b", "c"]);
  });

  it("respects savedOrder for known items", () => {
    const items = [
      make("a", "Alpha"),
      make("b", "Bravo"),
      make("c", "Charlie"),
    ];
    const result = orderByIds(items, ["c", "a", "b"], "title");
    expect(result.map((i) => i.id)).toEqual(["c", "a", "b"]);
  });

  it("appends unknown items alphabetically after known ones", () => {
    const items = [
      make("a", "Alpha"),
      make("b", "Bravo"),
      make("new-z", "Zulu"),
      make("new-d", "Delta"),
    ];
    const result = orderByIds(items, ["b", "a"], "title");
    expect(result.map((i) => i.id)).toEqual(["b", "a", "new-d", "new-z"]);
  });

  it("ignores savedOrder entries that are not in items", () => {
    const items = [make("a", "Alpha"), make("b", "Bravo")];
    const result = orderByIds(
      items,
      ["removed", "a", "also-gone", "b"],
      "title",
    );
    expect(result.map((i) => i.id)).toEqual(["a", "b"]);
  });

  it("handles empty items", () => {
    expect(orderByIds([], ["a", "b"], "title")).toEqual([]);
    expect(orderByIds([], null, "title")).toEqual([]);
  });

  it("handles empty savedOrder", () => {
    const items = [make("b", "Bravo"), make("a", "Alpha")];
    const result = orderByIds(items, [], "title");
    expect(result.map((i) => i.id)).toEqual(["a", "b"]);
  });

  it("does not mutate the input array", () => {
    const items = [make("b", "Bravo"), make("a", "Alpha")];
    const copy = [...items];
    orderByIds(items, null, "title");
    expect(items).toEqual(copy);
  });

  it("works with a different label key", () => {
    const items = [
      { id: "1", name: "Zulu" },
      { id: "2", name: "Alpha" },
    ];
    const result = orderByIds(items, null, "name");
    expect(result.map((i) => i.id)).toEqual(["2", "1"]);
  });

  it("preserves saved order even when items arrive in different order", () => {
    const items = [
      make("d", "D"),
      make("c", "C"),
      make("b", "B"),
      make("a", "A"),
    ];
    const result = orderByIds(items, ["a", "d", "b", "c"], "title");
    expect(result.map((i) => i.id)).toEqual(["a", "d", "b", "c"]);
  });

  it("handles single item", () => {
    const result = orderByIds([make("x", "X")], ["x"], "title");
    expect(result.map((i) => i.id)).toEqual(["x"]);
  });

  it("all items unknown when savedOrder has no matching ids", () => {
    const items = [make("b", "Bravo"), make("a", "Alpha")];
    const result = orderByIds(items, ["x", "y", "z"], "title");
    expect(result.map((i) => i.id)).toEqual(["a", "b"]);
  });
});
