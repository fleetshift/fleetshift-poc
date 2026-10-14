import { describe, expect, it } from "vitest";

import { getCursorContext } from "../cursorParser";
import { getStaticFields } from "../fieldRegistry";
import type { Suggestion } from "../types";
import {
  applyInsideMacro,
  applyReversedInFilter,
  computeFieldSuggestions,
} from "../useAdvancedSearch";

const fields = getStaticFields();

/**
 * Tests for operator suggestion values — specifically the dot-call
 * `.startsWith()` operator which should NOT insert a space between
 * the field and the operator.
 *
 * Bug: typing `resource.conditions.Applied.status ` then accepting
 * `.startsWith()` produces `resource.conditions.Applied.status .startsWith()`
 * with a spurious space. The replaceRange should eat trailing whitespace
 * before the cursor when inserting a dot-call operator.
 */
describe("dot-call operator insertion (startsWith space bug)", () => {
  /**
   * Mirrors the acceptSuggestion logic from useAdvancedSearch:
   * when suggestion.value starts with ".", trim trailing whitespace from before.
   */
  function simulateAccept(
    expr: string,
    cursorPos: number,
    suggestionValue: string,
  ): string {
    const ctx = getCursorContext(expr, cursorPos, fields);
    const [start, end] = ctx.replaceRange;
    let before = expr.slice(0, start);
    if (suggestionValue.startsWith(".")) {
      before = before.trimEnd();
    }
    const after = expr.slice(end);
    return before + suggestionValue + after;
  }

  it("accepting .startsWith() — no space between field and dot-call", () => {
    const expr = "resource.conditions.Applied.status ";
    const ctx = getCursorContext(expr, 35, fields);
    expect(ctx.kind).toBe("operator");

    const result = simulateAccept(expr, 35, ".startsWith()");
    expect(result).not.toContain(" .startsWith()");
    expect(result).toContain("status.startsWith()");
  });

  it("non-dot operators still get proper spacing", () => {
    const expr = "resource.conditions.Applied.status ";
    const result = simulateAccept(expr, 35, "== ");

    // Should have proper spacing: `status == ` (at least one space)
    expect(result).toMatch(/status\s+==\s/);
    // Should not have double spaces
    expect(result).not.toMatch(/status\s{2,}==/);
  });

  it("dot-call with multiple trailing spaces trims all of them", () => {
    const expr = "resource.conditions.Applied.status   ";
    const ctx = getCursorContext(expr, 37, fields);
    expect(ctx.kind).toBe("operator");

    const result = simulateAccept(expr, 37, ".startsWith()");
    expect(result).not.toContain(" .startsWith()");
    expect(result).toContain("status.startsWith()");
  });
});

/**
 * Tests for arrow-key navigation with operator chip layout.
 */
describe("operator chip keyboard navigation", () => {
  function currentNavHandler(
    key: string,
    prev: number,
    count: number,
  ): number | null {
    if (key === "ArrowDown" || key === "ArrowRight") {
      return prev < count - 1 ? prev + 1 : 0;
    }
    if (key === "ArrowUp" || key === "ArrowLeft") {
      return prev > 0 ? prev - 1 : count - 1;
    }
    return null;
  }

  it("ArrowRight should navigate forward through operator chips", () => {
    expect(currentNavHandler("ArrowRight", 0, 8)).toBe(1);
  });

  it("ArrowLeft should navigate backward through operator chips", () => {
    expect(currentNavHandler("ArrowLeft", 2, 8)).toBe(1);
  });

  it("ArrowLeft at first chip should wrap to last", () => {
    expect(currentNavHandler("ArrowLeft", 0, 8)).toBe(7);
  });

  it("ArrowRight at last chip should wrap to first", () => {
    expect(currentNavHandler("ArrowRight", 7, 8)).toBe(0);
  });
});

describe("computeFieldSuggestions", () => {
  describe("string method suggestions on leaf dot", () => {
    it("returns .startsWith() when dot typed after a string leaf", async () => {
      const result = await computeFieldSuggestions(
        "resource.conditions.Applied.status.",
      );
      expect(result).toHaveLength(1);
      expect(result[0].label).toBe(".startsWith()");
      expect(result[0].value).toBe('.startsWith("")');
      expect(result[0].cursorOffset).toBe(-2);
    });

    it("returns children when dot typed after a branch node", async () => {
      const result = await computeFieldSuggestions("resource.");
      expect(result.length).toBeGreaterThan(1);
      const labels = result.map((s) => s.label);
      expect(labels).toContain("Conditions");
      expect(labels).not.toContain(".startsWith()");
    });

    it("returns empty for dot after a numeric leaf", async () => {
      const result = await computeFieldSuggestions(
        "resource.observation.extracted.replicas.",
      );
      expect(result).toEqual([]);
    });

    it("returns .startsWith() for name. (top-level string leaf)", async () => {
      const result = await computeFieldSuggestions("name.");
      // name is a top-level string leaf — getChildrenAt returns nothing
      // but the node exists and is type "string"
      expect(result).toHaveLength(1);
      expect(result[0].label).toBe(".startsWith()");
    });
  });

  describe("has() in field suggestions", () => {
    it("includes has() in empty-partial top-level suggestions", async () => {
      const result = await computeFieldSuggestions("");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeDefined();
      expect(hasItem!.value).toBe("has(");
      expect(hasItem!.description).toBe("Check if field exists");
    });

    it("includes has() when partial matches 'h'", async () => {
      const result = await computeFieldSuggestions("h");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeDefined();
    });

    it("includes has() when partial matches 'ha'", async () => {
      const result = await computeFieldSuggestions("ha");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeDefined();
    });

    it("includes has() when partial matches 'has'", async () => {
      const result = await computeFieldSuggestions("has");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeDefined();
    });

    it("does NOT include has() when partial does not match", async () => {
      const result = await computeFieldSuggestions("na");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeUndefined();
    });

    it("does NOT include has() in nested path context", async () => {
      const result = await computeFieldSuggestions("resource.observation.");
      const hasItem = result.find((s) => s.label === "has()");
      expect(hasItem).toBeUndefined();
    });
  });
});

describe("applyInsideMacro", () => {
  it("appends ) to leaf path suggestions", () => {
    const input: Suggestion[] = [
      { type: "path", value: "name ", label: "Name" },
      { type: "path", value: "resourceType ", label: "Type" },
    ];
    const result = applyInsideMacro(input);
    expect(result[0].value).toBe("name) ");
    expect(result[1].value).toBe("resourceType) ");
  });

  it("preserves branch suggestions ending with dot", () => {
    const input: Suggestion[] = [
      { type: "path", value: "resource.", label: "Resource" },
      { type: "path", value: "name ", label: "Name" },
    ];
    const result = applyInsideMacro(input);
    expect(result[0].value).toBe("resource.");
    expect(result[1].value).toBe("name) ");
  });

  it("preserves non-path suggestions", () => {
    const input: Suggestion[] = [
      { type: "path", value: "has(", label: "has()" },
      {
        type: "semantic",
        value: 'name == "x"',
        label: "sem",
      },
    ];
    const result = applyInsideMacro(input);
    expect(result[0].value).toBe("has() ");
    expect(result[1].value).toBe('name == "x"');
  });
});

describe("applyReversedInFilter", () => {
  it("keeps branch nodes (value ends with dot)", () => {
    const input: Suggestion[] = [
      { type: "path", value: "resource.", label: "Resource" },
    ];
    const result = applyReversedInFilter(input);
    expect(result).toHaveLength(1);
  });

  it("keeps container-flagged leaf nodes", () => {
    const input: Suggestion[] = [
      {
        type: "path",
        value: "localLabels ",
        label: "Labels",
        celPreview: "resource.localLabels",
      },
    ];
    const result = applyReversedInFilter(input);
    expect(result).toHaveLength(1);
  });

  it("filters out scalar leaf nodes", () => {
    const input: Suggestion[] = [
      { type: "path", value: "name ", label: "Name" },
      { type: "path", value: "resourceType ", label: "Type" },
    ];
    const result = applyReversedInFilter(input);
    expect(result).toHaveLength(0);
  });

  it("preserves non-path suggestions (semantic, operator)", () => {
    const input: Suggestion[] = [
      {
        type: "semantic",
        value: "some expression",
        label: "sem",
      },
      { type: "path", value: "name ", label: "Name" },
    ];
    const result = applyReversedInFilter(input);
    expect(result).toHaveLength(1);
    expect(result[0].type).toBe("semantic");
  });

  it("filters a realistic top-level list correctly", async () => {
    const all = await computeFieldSuggestions("");
    const filtered = applyReversedInFilter(all);

    const labels = filtered.map((s) => s.label);
    // resource is a branch (has children) → kept
    expect(labels).toContain("Resource");
    // name and resourceType are scalar leaves → filtered out
    expect(labels).not.toContain("Resource Name");
    expect(labels).not.toContain("Resource Type");
    // has() is type "path" but value "has(" doesn't end with "." and
    // isn't a real field path → filtered out (expected)
    expect(labels).not.toContain("has()");
  });
});

describe("end-to-end suggestion pipeline", () => {
  it("typing dot after leaf in has() context → startsWith with auto-close", async () => {
    // has(resource.conditions.Applied.status.|
    // 1. computeFieldSuggestions returns .startsWith()
    // 2. insideMacro would NOT auto-close it (it's type "operator", not "path")
    const suggestions = await computeFieldSuggestions(
      "resource.conditions.Applied.status.",
    );
    const withMacro = applyInsideMacro(suggestions);
    // .startsWith() is type "operator", so insideMacro doesn't touch it
    expect(withMacro[0].value).toBe('.startsWith("")');
  });

  it("inside has() with no partial: leaf fields get ) appended", async () => {
    const suggestions = await computeFieldSuggestions("");
    const withMacro = applyInsideMacro(suggestions);

    const nameItem = withMacro.find((s) => s.label === "Resource Name");
    expect(nameItem).toBeDefined();
    expect(nameItem!.value).toBe("name) ");

    const resourceItem = withMacro.find((s) => s.label === "Resource");
    expect(resourceItem).toBeDefined();
    expect(resourceItem!.value).toBe("resource.");
  });

  it("reversed in at top level: only resource branch survives", async () => {
    const suggestions = await computeFieldSuggestions("");
    const filtered = applyReversedInFilter(suggestions);

    const pathLabels = filtered
      .filter((s) => s.type === "path")
      .map((s) => s.label);
    // Only "Resource" branch survives — name and resourceType are scalar leaves
    expect(pathLabels).toEqual(["Resource"]);
  });

  it("reversed in at resource. level: conditions (container) kept, scalar leaves filtered", async () => {
    const suggestions = await computeFieldSuggestions("resource.");
    const filtered = applyReversedInFilter(suggestions);

    const labels = filtered.map((s) => s.label);
    // conditions is a branch with container: true → kept
    expect(labels).toContain("Conditions");
    // observation is a branch → kept
    expect(labels).toContain("Observation");
    // localLabels is container: true → kept
    expect(labels).toContain("Labels");
    // uid, createTime, updateTime are scalar leaves → filtered out
    expect(labels).not.toContain("Resource UID");
    expect(labels).not.toContain("Created");
    expect(labels).not.toContain("Updated");
  });
});
