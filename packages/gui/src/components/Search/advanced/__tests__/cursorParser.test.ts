import { describe, expect, it } from "vitest";

import { getCursorContext } from "../cursorParser";
import { getStaticFields } from "../fieldRegistry";

const fields = getStaticFields();

describe("cursorParser", () => {
  it("returns field context for empty expression", () => {
    const ctx = getCursorContext("", 0, fields);
    expect(ctx.kind).toBe("field");
    expect(ctx.partial).toBe("");
  });

  it("returns field context when typing a field name", () => {
    const ctx = getCursorContext("nam", 3, fields);
    expect(ctx.kind).toBe("field");
    expect(ctx.partial).toBe("nam");
  });

  it("returns operator context after a complete field", () => {
    const ctx = getCursorContext("name ", 5, fields);
    expect(ctx.kind).toBe("operator");
    expect(ctx.fieldName).toBe("name");
  });

  it("returns operator context when typing a complete operator", () => {
    const ctx = getCursorContext("name ==", 7, fields);
    expect(ctx.kind).toBe("operator");
    expect(ctx.fieldName).toBe("name");
  });

  it("returns value context after a complete operator", () => {
    const ctx = getCursorContext("name == ", 8, fields);
    expect(ctx.kind).toBe("value");
    expect(ctx.fieldName).toBe("name");
  });

  it("returns value context when typing a value", () => {
    const ctx = getCursorContext('name == "fo', 11, fields);
    expect(ctx.kind).toBe("value");
    expect(ctx.fieldName).toBe("name");
  });

  it("returns combinator context after a complete value", () => {
    const ctx = getCursorContext('name == "foo" ', 14, fields);
    expect(ctx.kind).toBe("combinator");
  });

  it("returns field context after a combinator", () => {
    const ctx = getCursorContext('name == "foo" && ', 17, fields);
    expect(ctx.kind).toBe("field");
  });

  it("returns field context after a combinator with partial", () => {
    const ctx = getCursorContext('name == "foo" && res', 20, fields);
    expect(ctx.kind).toBe("field");
    expect(ctx.partial).toBe("res");
  });

  it("handles nested field names", () => {
    const ctx = getCursorContext("resource.spec.name ", 19, fields);
    expect(ctx.kind).toBe("operator");
    expect(ctx.fieldName).toBe("resource.spec.name");
  });

  it("handles cursor in the middle of expression", () => {
    const ctx = getCursorContext(
      'name == "foo" && resourceType == "bar"',
      17,
      fields,
    );
    expect(ctx.kind).toBe("field");
  });

  it("returns field context after open paren", () => {
    const ctx = getCursorContext("(", 1, fields);
    expect(ctx.kind).toBe("field");
  });
});
