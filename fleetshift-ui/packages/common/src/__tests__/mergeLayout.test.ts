import { describe, expect, it } from "vitest";

import type {
  NavLayoutEntry,
  NavLayoutGroup,
  NavLayoutMore,
  NavLayoutOverride,
  NavLayoutPage,
  NavLayoutSection,
} from "../navLayout";
import {
  buildLayout,
  collectPageIds,
  extractMore,
  flattenLayout,
  isNavLayoutOverride,
  mergeLayout,
} from "../navLayout";

const page = (id: string): NavLayoutPage => ({ type: "page", pageId: id });

const group = (
  groupId: string,
  pluginKey: string,
  label: string,
  children: NavLayoutPage[],
): NavLayoutGroup => ({
  type: "group",
  groupId,
  pluginKey,
  label,
  children,
});

const section = (
  id: string,
  label: string,
  children: { pageId: string }[],
): NavLayoutSection => ({
  type: "section",
  id,
  label,
  children,
});

const more = (children: NavLayoutEntry[]): NavLayoutMore => ({
  type: "more",
  children,
});

const override = (layout: NavLayoutEntry[]): NavLayoutOverride => ({
  version: 1,
  layout,
});

describe("collectPageIds", () => {
  it("collects page IDs from top-level pages", () => {
    const ids = collectPageIds([page("a"), page("b")]);
    expect(ids).toEqual(new Set(["a", "b"]));
  });

  it("collects page IDs from groups", () => {
    const ids = collectPageIds([
      group("g1", "core", "Core", [page("a"), page("b")]),
    ]);
    expect(ids).toEqual(new Set(["a", "b"]));
  });

  it("collects page IDs from sections", () => {
    const ids = collectPageIds([
      {
        type: "section",
        id: "s1",
        label: "Section",
        children: [{ pageId: "x" }, { pageId: "y" }],
      },
    ]);
    expect(ids).toEqual(new Set(["x", "y"]));
  });

  it("collects from mixed layout", () => {
    const ids = collectPageIds([
      page("a"),
      group("g1", "core", "Core", [page("b")]),
      {
        type: "section",
        id: "s1",
        label: "S",
        children: [{ pageId: "c" }],
      },
    ]);
    expect(ids).toEqual(new Set(["a", "b", "c"]));
  });

  it("returns empty set for empty layout", () => {
    expect(collectPageIds([])).toEqual(new Set());
  });
});

describe("isNavLayoutOverride", () => {
  it("returns true for valid override", () => {
    expect(isNavLayoutOverride({ version: 1, layout: [] })).toBe(true);
  });

  it("returns false for null", () => {
    expect(isNavLayoutOverride(null)).toBe(false);
  });

  it("returns false for string array (legacy)", () => {
    expect(isNavLayoutOverride(["a", "b"])).toBe(false);
  });

  it("returns false for wrong version", () => {
    expect(
      isNavLayoutOverride({
        version: 2,
        layout: [],
      } as unknown as NavLayoutOverride),
    ).toBe(false);
  });

  it("returns false for missing layout", () => {
    expect(
      isNavLayoutOverride({ version: 1 } as unknown as NavLayoutOverride),
    ).toBe(false);
  });
});

describe("mergeLayout", () => {
  it("returns backend layout when override is null", () => {
    const backend = [page("a"), page("b")];
    expect(mergeLayout(backend, null)).toEqual(backend);
  });

  it("returns override layout when no changes", () => {
    const backend = [page("a"), page("b")];
    const userOverride = override([page("b"), page("a")]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("b"), page("a")]);
  });

  it("drops removed pages from override", () => {
    const backend = [page("a")]; // "b" removed
    const userOverride = override([page("b"), page("a")]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("a")]);
  });

  it("drops removed pages from groups in override", () => {
    const backend = [group("g1", "core", "Core", [page("a")])]; // "b" removed
    const userOverride = override([
      group("g1", "core", "Core", [page("a"), page("b")]),
    ]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([group("g1", "core", "Core", [page("a")])]);
  });

  it("appends added ungrouped pages alphabetically", () => {
    const backend = [page("a"), page("b"), page("c")]; // "c" is new
    const userOverride = override([page("b"), page("a")]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("b"), page("a"), page("c")]);
  });

  it("inserts added pages into their backend group", () => {
    const backend = [group("g1", "core", "Core", [page("a"), page("b")])]; // "b" is new
    const userOverride = override([group("g1", "core", "Core", [page("a")])]);
    const result = mergeLayout(backend, userOverride);
    const g = result[0] as NavLayoutGroup;
    expect(g.type).toBe("group");
    expect(g.children).toEqual([page("a"), page("b")]);
  });

  it("creates new group in override when backend adds a group", () => {
    const backend = [
      page("a"),
      group("g-new", "mgmt", "Management", [page("x"), page("y")]),
    ];
    const userOverride = override([page("a")]);
    const result = mergeLayout(backend, userOverride);
    // "a" stays, then the new group with its pages is appended
    expect(result.length).toBe(2);
    expect(result[0]).toEqual(page("a"));
    const g = result[1] as NavLayoutGroup;
    expect(g.type).toBe("group");
    expect(g.groupId).toBe("g-new");
    expect(g.children.map((c) => c.pageId)).toEqual(["x", "y"]);
  });

  it("handles simultaneous add and remove", () => {
    const backend = [page("a"), page("c")]; // "b" removed, "c" added
    const userOverride = override([page("b"), page("a")]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("a"), page("c")]);
  });

  it("preserves user arrangement for unchanged pages", () => {
    const backend = [page("a"), page("b"), page("c")];
    const userOverride = override([page("c"), page("a"), page("b")]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("c"), page("a"), page("b")]);
  });

  it("preserves empty groups (visible in editor)", () => {
    const backend: NavLayoutEntry[] = []; // all plugins uninstalled
    const userOverride = override([group("g1", "core", "Core", [page("a")])]);
    // "a" is removed, but the group container stays
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([group("g1", "core", "Core", [])]);
  });

  it("handles empty backend and empty override", () => {
    expect(mergeLayout([], override([]))).toEqual([]);
  });

  it("handles override with pages moved between groups", () => {
    const backend = [
      group("g1", "core", "Core", [page("a")]),
      group("g2", "mgmt", "Mgmt", [page("b")]),
    ];
    // User moved "a" into g2
    const userOverride = override([
      group("g1", "core", "Core", []),
      group("g2", "mgmt", "Mgmt", [page("b"), page("a")]),
    ]);
    const result = mergeLayout(backend, userOverride);
    // User's arrangement preserved — "a" stays in g2
    const g1 = result[0] as NavLayoutGroup;
    const g2 = result[1] as NavLayoutGroup;
    expect(g1.children).toEqual([]);
    expect(g2.children.map((c) => c.pageId)).toEqual(["b", "a"]);
  });

  it("sorts multiple added ungrouped pages alphabetically", () => {
    const backend = [page("a"), page("z"), page("m"), page("b")];
    const userOverride = override([page("a")]);
    // b, m, z are new — appended in alpha order by pageId
    const result = mergeLayout(backend, userOverride);
    expect(result.map((e) => (e as NavLayoutPage).pageId)).toEqual([
      "a",
      "b",
      "m",
      "z",
    ]);
  });

  it("does not duplicate pages already in override group", () => {
    const backend = [group("g1", "core", "Core", [page("a"), page("b")])];
    const userOverride = override([
      group("g1", "core", "Core", [page("a"), page("b")]),
    ]);
    const result = mergeLayout(backend, userOverride);
    const g = result[0] as NavLayoutGroup;
    expect(g.children).toEqual([page("a"), page("b")]);
  });

  it("inserts added pages into their backend section", () => {
    // "b" is new
    const backend = [
      section("s1", "Tools", [{ pageId: "a" }, { pageId: "b" }]),
    ];
    const userOverride = override([section("s1", "Tools", [{ pageId: "a" }])]);
    const result = mergeLayout(backend, userOverride);
    const s = result[0] as NavLayoutSection;
    expect(s.type).toBe("section");
    expect(s.children).toEqual([{ pageId: "a" }, { pageId: "b" }]);
  });

  it("creates new section in override when backend adds a section", () => {
    const backend = [
      page("a"),
      section("s-new", "New Section", [{ pageId: "x" }, { pageId: "y" }]),
    ];
    const userOverride = override([page("a")]);
    const result = mergeLayout(backend, userOverride);
    expect(result.length).toBe(2);
    expect(result[0]).toEqual(page("a"));
    const s = result[1] as NavLayoutSection;
    expect(s.type).toBe("section");
    expect(s.id).toBe("s-new");
    expect(s.children.map((c) => c.pageId)).toEqual(["x", "y"]);
  });

  it("drops removed pages from sections in override", () => {
    const backend = [section("s1", "Tools", [{ pageId: "a" }])]; // "b" removed
    const userOverride = override([
      section("s1", "Tools", [{ pageId: "a" }, { pageId: "b" }]),
    ]);
    const result = mergeLayout(backend, userOverride);
    const s = result[0] as NavLayoutSection;
    expect(s.children).toEqual([{ pageId: "a" }]);
  });

  // --- "More" (hidden items) tests ---

  it("preserves hidden items in more entry", () => {
    const backend = [page("a"), page("b"), page("c")];
    const userOverride = override([page("a"), more([page("b"), page("c")])]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("a"), more([page("b"), page("c")])]);
  });

  it("drops removed pages from more entry", () => {
    const backend = [page("a"), page("b")]; // "c" removed
    const userOverride = override([page("a"), more([page("b"), page("c")])]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("a"), more([page("b")])]);
  });

  it("removes more entry entirely when all hidden pages are removed", () => {
    const backend = [page("a")]; // "b" and "c" removed
    const userOverride = override([page("a"), more([page("b"), page("c")])]);
    const result = mergeLayout(backend, userOverride);
    expect(result).toEqual([page("a")]);
  });

  it("added pages go into active part, never into more", () => {
    const backend = [page("a"), page("b"), page("new")]; // "new" added
    const userOverride = override([page("a"), more([page("b")])]);
    const result = mergeLayout(backend, userOverride);
    // "new" should be appended to the active part, before "more"
    expect(result[0]).toEqual(page("a"));
    expect(result[1]).toEqual(page("new"));
    const moreEntry = result[2] as NavLayoutMore;
    expect(moreEntry.type).toBe("more");
    expect(moreEntry.children).toEqual([page("b")]);
  });

  it("added pages go into backend group, not more", () => {
    const backend = [group("g1", "core", "Core", [page("a"), page("b")])];
    const userOverride = override([
      group("g1", "core", "Core", [page("a")]),
      more([page("x")]),
    ]);
    // "b" is new — should go into g1, not more
    const result = mergeLayout(backend, userOverride);
    const g = result[0] as NavLayoutGroup;
    expect(g.children.map((c) => c.pageId)).toEqual(["a", "b"]);
    // "x" was hidden but is now removed from backend
    expect(result.length).toBe(1); // no more entry since "x" is removed
  });

  it("handles hidden groups inside more", () => {
    const backend = [
      page("a"),
      group("g1", "core", "Core", [page("b"), page("c")]),
    ];
    const userOverride = override([
      page("a"),
      more([group("g1", "core", "Core", [page("b"), page("c")])]),
    ]);
    const result = mergeLayout(backend, userOverride);
    expect(result[0]).toEqual(page("a"));
    const moreEntry = result[1] as NavLayoutMore;
    expect(moreEntry.type).toBe("more");
    const hiddenGroup = moreEntry.children[0] as NavLayoutGroup;
    expect(hiddenGroup.groupId).toBe("g1");
    expect(hiddenGroup.children.map((c) => c.pageId)).toEqual(["b", "c"]);
  });

  it("drops removed pages from groups inside more", () => {
    const backend = [page("a"), page("b")]; // "c" removed
    const userOverride = override([
      page("a"),
      more([group("g1", "core", "Core", [page("b"), page("c")])]),
    ]);
    const result = mergeLayout(backend, userOverride);
    const moreEntry = result[1] as NavLayoutMore;
    const hiddenGroup = moreEntry.children[0] as NavLayoutGroup;
    expect(hiddenGroup.children.map((c) => c.pageId)).toEqual(["b"]);
  });
});

describe("collectPageIds — more", () => {
  it("collects page IDs from more entry", () => {
    const ids = collectPageIds([page("a"), more([page("b"), page("c")])]);
    expect(ids).toEqual(new Set(["a", "b", "c"]));
  });

  it("collects page IDs from groups inside more", () => {
    const ids = collectPageIds([
      more([group("g1", "core", "Core", [page("x"), page("y")])]),
    ]);
    expect(ids).toEqual(new Set(["x", "y"]));
  });
});

describe("flattenLayout / buildLayout — iconOverride roundtrip", () => {
  it("preserves iconOverride on top-level pages", () => {
    const layout: NavLayoutEntry[] = [
      { type: "page", pageId: "a", iconOverride: "CogIcon" },
      page("b"),
    ];
    const flat = flattenLayout(layout);
    expect(flat[0].iconOverride).toBe("CogIcon");
    expect(flat[1].iconOverride).toBeUndefined();
    const rebuilt = buildLayout(flat);
    expect(rebuilt[0]).toEqual({
      type: "page",
      pageId: "a",
      iconOverride: "CogIcon",
    });
    expect(rebuilt[1]).toEqual(page("b"));
  });

  it("preserves iconOverride on group children", () => {
    const layout: NavLayoutEntry[] = [
      group("g1", "core", "Core", [
        { type: "page", pageId: "a", iconOverride: "LockIcon" },
        page("b"),
      ]),
    ];
    const flat = flattenLayout(layout);
    const childA = flat.find((n) => n.pageId === "a");
    expect(childA?.iconOverride).toBe("LockIcon");
    const rebuilt = buildLayout(flat);
    const g = rebuilt[0] as NavLayoutGroup;
    expect(g.children[0]).toEqual({
      type: "page",
      pageId: "a",
      iconOverride: "LockIcon",
    });
    expect(g.children[1]).toEqual(page("b"));
  });
});

describe("extractMore", () => {
  it("separates active entries from more children", () => {
    const layout: NavLayoutEntry[] = [
      page("a"),
      page("b"),
      more([page("c"), page("d")]),
    ];
    const result = extractMore(layout);
    expect(result.active).toEqual([page("a"), page("b")]);
    expect(result.more).toEqual([page("c"), page("d")]);
  });

  it("returns empty more when no more entry exists", () => {
    const layout: NavLayoutEntry[] = [page("a"), page("b")];
    const result = extractMore(layout);
    expect(result.active).toEqual([page("a"), page("b")]);
    expect(result.more).toEqual([]);
  });

  it("handles empty layout", () => {
    const result = extractMore([]);
    expect(result.active).toEqual([]);
    expect(result.more).toEqual([]);
  });

  it("handles groups inside more", () => {
    const hiddenGroup = group("g1", "core", "Core", [page("x")]);
    const layout: NavLayoutEntry[] = [page("a"), more([hiddenGroup])];
    const result = extractMore(layout);
    expect(result.active).toEqual([page("a")]);
    expect(result.more).toEqual([hiddenGroup]);
  });
});
