import { describe, expect, it } from "vitest";

import { tokenize } from "../tokenizer";

describe("tokenizer", () => {
  it("tokenizes a simple equality expression", () => {
    const tokens = tokenize('name == "foo"');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful).toEqual([
      { type: "field", value: "name", start: 0, end: 4 },
      { type: "operator", value: "==", start: 5, end: 7 },
      { type: "value", value: '"foo"', start: 8, end: 13 },
    ]);
  });

  it("tokenizes a compound expression with &&", () => {
    const tokens = tokenize('name == "a" && resourceType == "b"');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful).toHaveLength(7);
    expect(meaningful[3]).toEqual(
      expect.objectContaining({ type: "combinator", value: "&&" }),
    );
  });

  it("tokenizes dot-call methods", () => {
    const tokens = tokenize('name.startsWith("prod")');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful).toEqual([
      { type: "field", value: "name", start: 0, end: 4 },
      {
        type: "dot-call",
        value: '.startsWith("prod")',
        start: 4,
        end: 23,
      },
    ]);
  });

  it("tokenizes nested field names with dots", () => {
    const tokens = tokenize('resource.spec.name == "x"');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[0]).toEqual(
      expect.objectContaining({ type: "field", value: "resource.spec.name" }),
    );
  });

  it("tokenizes numeric values", () => {
    const tokens = tokenize("resource.spec.replicas > 3");
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[2]).toEqual(
      expect.objectContaining({ type: "value", value: "3" }),
    );
  });

  it("tokenizes boolean values", () => {
    const tokens = tokenize("enabled == true");
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[2]).toEqual(
      expect.objectContaining({ type: "value", value: "true" }),
    );
  });

  it("tokenizes the 'in' operator", () => {
    const tokens = tokenize('name in ["a", "b"]');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[1]).toEqual(
      expect.objectContaining({ type: "operator", value: "in" }),
    );
    expect(meaningful[2]).toEqual(
      expect.objectContaining({ type: "value", value: '["a", "b"]' }),
    );
  });

  it("tokenizes parenthesized groups", () => {
    const tokens = tokenize('(name == "a")');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[0]).toEqual(
      expect.objectContaining({ type: "paren", value: "(" }),
    );
    expect(meaningful[4]).toEqual(
      expect.objectContaining({ type: "paren", value: ")" }),
    );
  });

  it("tokenizes || combinator", () => {
    const tokens = tokenize('a == "1" || b == "2"');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[3]).toEqual(
      expect.objectContaining({ type: "combinator", value: "||" }),
    );
  });

  it("handles empty expression", () => {
    expect(tokenize("")).toEqual([]);
  });

  it("preserves position information", () => {
    const tokens = tokenize('name == "x"');
    for (const token of tokens) {
      expect(token.value).toBe('name == "x"'.slice(token.start, token.end));
    }
  });

  it("does not treat unsupported methods as dot-calls", () => {
    const tokens = tokenize('name.contains("test")');
    const meaningful = tokens.filter((t) => t.type !== "whitespace");
    expect(meaningful[0].type).toBe("field");
    expect(meaningful[0].value).toBe("name.contains");
  });
});
