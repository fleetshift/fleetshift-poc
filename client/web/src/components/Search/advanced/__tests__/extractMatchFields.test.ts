import { describe, expect, it } from "vitest";

import { extractFieldPaths, resolveFieldValue } from "../extractMatchFields";

describe("extractFieldPaths", () => {
  it("returns field tokens from simple expression", () => {
    expect(extractFieldPaths('name == "foo"')).toEqual(["name"]);
  });

  it("returns dotted paths", () => {
    expect(
      extractFieldPaths('resource.conditions.Applied.status == "True"'),
    ).toEqual(["resource.conditions.Applied.status"]);
  });

  it("deduplicates repeated fields", () => {
    expect(extractFieldPaths('name == "x" && name != "y"')).toEqual(["name"]);
  });

  it("returns multiple distinct fields", () => {
    expect(extractFieldPaths('name == "x" && resourceType == "Pod"')).toEqual([
      "name",
      "resourceType",
    ]);
  });

  it("extracts field inside has()", () => {
    const paths = extractFieldPaths("has(resource.labels)");
    expect(paths).toContain("resource.labels");
  });

  it("extracts field from reversed in", () => {
    const paths = extractFieldPaths('"Ready" in resource.conditions');
    expect(paths).toContain("resource.conditions");
  });

  it("returns empty for expression with no fields", () => {
    expect(extractFieldPaths('"hello"')).toEqual([]);
  });

  it("handles complex expression with has + reversed in + standard", () => {
    const paths = extractFieldPaths(
      'has(resource.labels) && "Ready" in resource.conditions && name == "x"',
    );
    expect(paths).toContain("resource.labels");
    expect(paths).toContain("resource.conditions");
    expect(paths).toContain("name");
  });
});

describe("resolveFieldValue", () => {
  const obj = {
    name: "test-pod",
    resource: {
      conditions: {
        Applied: {
          status: "True",
        },
        Ready: {
          status: "False",
        },
      },
      labels: {
        team: "platform",
        env: "prod",
      },
    },
  };

  it("resolves shallow path", () => {
    expect(resolveFieldValue(obj, "name")).toBe("test-pod");
  });

  it("resolves deep dotted path", () => {
    expect(resolveFieldValue(obj, "resource.conditions.Applied.status")).toBe(
      "True",
    );
  });

  it("returns undefined for missing path", () => {
    expect(
      resolveFieldValue(obj, "resource.nonexistent.field"),
    ).toBeUndefined();
  });

  it("resolves to object for non-leaf path", () => {
    const val = resolveFieldValue(obj, "resource.labels");
    expect(val).toEqual({ team: "platform", env: "prod" });
  });

  it("returns undefined for empty path", () => {
    expect(resolveFieldValue(obj, "")).toBeUndefined();
  });

  it("returns undefined when obj is empty", () => {
    expect(resolveFieldValue({}, "name")).toBeUndefined();
  });
});
