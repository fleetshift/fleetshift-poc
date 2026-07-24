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

  describe("has() macro", () => {
    it("tokenizes basic has() as macro", () => {
      const tokens = tokenize("has(resource.labels)");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful).toEqual([
        { type: "macro", value: "has", start: 0, end: 3 },
        { type: "paren", value: "(", start: 3, end: 4 },
        { type: "field", value: "resource.labels", start: 4, end: 19 },
        { type: "paren", value: ")", start: 19, end: 20 },
      ]);
    });

    it("tokenizes has() with dotted path", () => {
      const tokens = tokenize("has(resource.labels.team)");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful).toEqual([
        { type: "macro", value: "has", start: 0, end: 3 },
        { type: "paren", value: "(", start: 3, end: 4 },
        { type: "field", value: "resource.labels.team", start: 4, end: 24 },
        { type: "paren", value: ")", start: 24, end: 25 },
      ]);
    });

    it("tokenizes negated has()", () => {
      const tokens = tokenize("!has(resource.pauseReason)");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful[0]).toEqual({
        type: "operator",
        value: "!",
        start: 0,
        end: 1,
      });
      expect(meaningful[1]).toEqual({
        type: "macro",
        value: "has",
        start: 1,
        end: 4,
      });
      expect(meaningful[2]).toEqual({
        type: "paren",
        value: "(",
        start: 4,
        end: 5,
      });
    });

    it("tokenizes has() combined with other expressions", () => {
      const tokens = tokenize('has(resource.labels) && name == "x"');
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful).toHaveLength(8);
      expect(meaningful[0]).toEqual(
        expect.objectContaining({ type: "macro", value: "has" }),
      );
      expect(meaningful[4]).toEqual(
        expect.objectContaining({ type: "combinator", value: "&&" }),
      );
      expect(meaningful[5]).toEqual(
        expect.objectContaining({ type: "field", value: "name" }),
      );
    });

    it("tokenizes multiple has() calls", () => {
      const tokens = tokenize(
        "has(resource.labels) || has(resource.conditions)",
      );
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful[0]).toEqual(
        expect.objectContaining({ type: "macro", value: "has" }),
      );
      expect(meaningful[4]).toEqual(
        expect.objectContaining({ type: "combinator", value: "||" }),
      );
      expect(meaningful[5]).toEqual(
        expect.objectContaining({ type: "macro", value: "has" }),
      );
    });

    it("treats bare 'has' without parens as a field", () => {
      const tokens = tokenize("has");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful).toEqual([
        { type: "field", value: "has", start: 0, end: 3 },
      ]);
    });

    it("treats 'has' followed by non-paren as a field", () => {
      const tokens = tokenize("has == true");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful[0]).toEqual(
        expect.objectContaining({ type: "field", value: "has" }),
      );
    });

    it("recognizes has() with space before paren", () => {
      const tokens = tokenize("has (resource.labels)");
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful[0]).toEqual(
        expect.objectContaining({ type: "macro", value: "has" }),
      );
    });

    it("preserves positions for macro tokens", () => {
      const expr = "has(resource.labels) && has(resource.conditions)";
      const tokens = tokenize(expr);
      for (const token of tokens) {
        expect(token.value).toBe(expr.slice(token.start, token.end));
      }
    });
  });

  describe("reversed 'in' operator", () => {
    it("tokenizes string literal in field path", () => {
      const tokens = tokenize('"team" in resource.labels');
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful).toEqual([
        { type: "value", value: '"team"', start: 0, end: 6 },
        { type: "operator", value: "in", start: 7, end: 9 },
        { type: "field", value: "resource.labels", start: 10, end: 25 },
      ]);
    });

    it("tokenizes reversed in combined with has()", () => {
      const tokens = tokenize(
        '"Ready" in resource.conditions && has(resource.labels)',
      );
      const meaningful = tokens.filter((t) => t.type !== "whitespace");
      expect(meaningful[0]).toEqual(
        expect.objectContaining({ type: "value", value: '"Ready"' }),
      );
      expect(meaningful[1]).toEqual(
        expect.objectContaining({ type: "operator", value: "in" }),
      );
      expect(meaningful[2]).toEqual(
        expect.objectContaining({
          type: "field",
          value: "resource.conditions",
        }),
      );
      expect(meaningful[3]).toEqual(
        expect.objectContaining({ type: "combinator", value: "&&" }),
      );
      expect(meaningful[4]).toEqual(
        expect.objectContaining({ type: "macro", value: "has" }),
      );
    });
  });
});
