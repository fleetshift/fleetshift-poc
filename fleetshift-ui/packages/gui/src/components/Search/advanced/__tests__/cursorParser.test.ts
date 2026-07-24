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

  it("returns combinator context after has()", () => {
    const ctx = getCursorContext("has(resource.labels) ", 21, fields);
    expect(ctx.kind).toBe("combinator");
  });

  it("returns field context after has() and combinator", () => {
    const ctx = getCursorContext("has(resource.labels) && ", 24, fields);
    expect(ctx.kind).toBe("field");
    expect(ctx.partial).toBe("");
  });

  it("returns combinator context after closing group paren", () => {
    const ctx = getCursorContext('(name == "foo") ', 16, fields);
    expect(ctx.kind).toBe("combinator");
  });

  describe("reversed 'in' operator", () => {
    it("returns operator context after a string value (for reversed in)", () => {
      // After typing "Ready" the user should see `in` as an operator option
      const ctx = getCursorContext('"Ready" ', 8, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("returns field context after reversed in operator", () => {
      // After `"Ready" in ` the cursor should suggest field paths
      const ctx = getCursorContext('"Ready" in ', 11, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.reversedIn).toBe(true);
    });

    it("returns field context while typing path after reversed in", () => {
      const ctx = getCursorContext('"Ready" in resource.', 20, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("resource.");
      expect(ctx.reversedIn).toBe(true);
    });

    it("returns combinator after complete reversed in expression", () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions ',
        31,
        fields,
      );
      expect(ctx.kind).toBe("combinator");
    });

    it("returns field after reversed in expression and combinator", () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions && ',
        34,
        fields,
      );
      expect(ctx.kind).toBe("field");
    });

    it("returns operator after value in combined has + reversed in", () => {
      const ctx = getCursorContext(
        'has(resource.state) || "Ready" ',
        31,
        fields,
      );
      expect(ctx.kind).toBe("operator");
    });

    it("returns field when typing path on cursor after reversed in", () => {
      // "Ready" in resourc|e — cursor on the field token
      const ctx = getCursorContext('"Ready" in resource', 19, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("resource");
      expect(ctx.reversedIn).toBe(true);
    });

    it("does not set reversedIn for normal in operator", () => {
      const ctx = getCursorContext(
        'resource.observation.kind in ["Pod"] ',
        37,
        fields,
      );
      expect(ctx.reversedIn).toBeUndefined();
    });

    it("normal value after operator still gives combinator", () => {
      // name == "foo" | — standard flow, value follows operator
      const ctx = getCursorContext('name == "foo" ', 14, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("returns operator after value inside parens (reversed in)", () => {
      // ("Ready" |) — value in field position inside group
      const ctx = getCursorContext('("Ready" ', 9, fields);
      expect(ctx.kind).toBe("operator");
    });
  });

  describe("complex queries at various cursor positions", () => {
    // Complex: has() && reversed-in && standard expression
    const complex1 =
      'has(resource.state) && "Ready" in resource.conditions && name == "foo"';

    it("complex1: combinator after has()", () => {
      // has(resource.state) |
      const ctx = getCursorContext(complex1, 20, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("complex1: field after first &&", () => {
      // has(resource.state) && |
      const ctx = getCursorContext(complex1, 23, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex1: operator after reversed-in value", () => {
      // has(resource.state) && "Ready" |
      const ctx = getCursorContext(complex1, 31, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("complex1: field after reversed in operator", () => {
      // has(resource.state) && "Ready" in |
      const ctx = getCursorContext(complex1, 34, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex1: combinator after reversed-in field", () => {
      // has(resource.state) && "Ready" in resource.conditions |
      const ctx = getCursorContext(complex1, 55, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("complex1: field after second &&", () => {
      // ... && name |
      const ctx = getCursorContext(complex1, 58, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex1: operator after 'name'", () => {
      // ... && name |
      const ctx = getCursorContext(complex1, 63, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("complex1: value after ==", () => {
      // ... name == |
      const ctx = getCursorContext(complex1, 66, fields);
      expect(ctx.kind).toBe("value");
    });

    // Complex: multiple has() with ||
    const complex2 = "has(resource.state) || has(resource.conditions)";

    it("complex2: combinator after first has()", () => {
      const ctx = getCursorContext(complex2, 20, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("complex2: field after ||", () => {
      const ctx = getCursorContext(complex2, 23, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex2: combinator after second has()", () => {
      const ctx = getCursorContext(complex2, 47, fields);
      expect(ctx.kind).toBe("combinator");
    });

    // Complex: !has() combined with reversed in
    const complex3 = '!has(resource.pauseReason) && "team" in resource.labels';

    it("complex3: combinator after !has()", () => {
      const ctx = getCursorContext(complex3, 27, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("complex3: operator after reversed-in value", () => {
      // ... && "team" |
      const ctx = getCursorContext(complex3, 36, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("complex3: field after in", () => {
      // ... "team" in |
      const ctx = getCursorContext(complex3, 39, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex3: combinator after complete reversed-in", () => {
      const ctx = getCursorContext(complex3 + " ", 56, fields);
      expect(ctx.kind).toBe("combinator");
    });

    // Complex: reversed in with grouping parens
    const complex4 = '("Ready" in resource.conditions)';

    it("complex4: operator after value inside parens", () => {
      const ctx = getCursorContext(complex4, 9, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("complex4: field after in inside parens", () => {
      const ctx = getCursorContext(complex4, 12, fields);
      expect(ctx.kind).toBe("field");
    });

    it("complex4: combinator after closing paren", () => {
      const ctx = getCursorContext(complex4 + " ", 33, fields);
      expect(ctx.kind).toBe("combinator");
    });

    // Complex: two reversed-in clauses
    const complex5 =
      '"Ready" in resource.conditions && "team" in resource.labels';

    it("complex5: combinator after first reversed-in", () => {
      const ctx = getCursorContext(complex5, 31, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("complex5: operator after second reversed-in value", () => {
      const ctx = getCursorContext(complex5, 40, fields);
      expect(ctx.kind).toBe("operator");
    });

    it("complex5: field after second in", () => {
      const ctx = getCursorContext(complex5, 43, fields);
      expect(ctx.kind).toBe("field");
    });
  });

  describe("half-written queries — reversed in progressive typing", () => {
    // Simulates typing "Ready" in resource.conditions keystroke by keystroke

    it('after opening quote: "', () => {
      const ctx = getCursorContext('"', 1, fields);
      // typing a value — value context is acceptable (building the string)
      expect(ctx.kind).toBe("value");
    });

    it('after closing quote: "Ready"', () => {
      const ctx = getCursorContext('"Ready"', 7, fields);
      // value is complete and in field position → suggest operators
      expect(ctx.kind).toBe("operator");
    });

    it('after space: "Ready" |', () => {
      const ctx = getCursorContext('"Ready" ', 8, fields);
      // value is in field position → should suggest operators (including `in`)
      expect(ctx.kind).toBe("operator");
    });

    it('typing i: "Ready" i|', () => {
      // "i" is parsed as a field token, but it's really partial "in" operator
      // after a value-in-field-position, a field-like token is ambiguous;
      // field context is acceptable since the user hasn't finished typing
      const ctx = getCursorContext('"Ready" i', 9, fields);
      expect(["field", "operator"]).toContain(ctx.kind);
    });

    it('after in + space: "Ready" in |', () => {
      const ctx = getCursorContext('"Ready" in ', 11, fields);
      expect(ctx.kind).toBe("field");
    });

    it('typing path: "Ready" in res|', () => {
      const ctx = getCursorContext('"Ready" in res', 14, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("res");
    });

    it('typing dotted path: "Ready" in resource.|', () => {
      const ctx = getCursorContext('"Ready" in resource.', 20, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("resource.");
    });

    it('typing full path: "Ready" in resource.conditions|', () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions',
        30,
        fields,
      );
      expect(ctx.kind).toBe("field");
    });

    it('after complete expression: "Ready" in resource.conditions |', () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions ',
        31,
        fields,
      );
      expect(ctx.kind).toBe("combinator");
    });

    it('typing &&: "Ready" in resource.conditions &|', () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions &&',
        33,
        fields,
      );
      expect(ctx.kind).toBe("combinator");
    });

    it('after &&: "Ready" in resource.conditions && |', () => {
      const ctx = getCursorContext(
        '"Ready" in resource.conditions && ',
        34,
        fields,
      );
      expect(ctx.kind).toBe("field");
    });
  });

  describe("half-written queries — has() progressive typing", () => {
    it("typing h: h|", () => {
      const ctx = getCursorContext("h", 1, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("h");
    });

    it("typing has: has|", () => {
      const ctx = getCursorContext("has", 3, fields);
      // no paren yet → treated as field
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("has");
    });

    it("after open paren: has(|", () => {
      const ctx = getCursorContext("has(", 4, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.insideMacro).toBe(true);
    });

    it("typing path inside: has(res|", () => {
      const ctx = getCursorContext("has(res", 7, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.partial).toBe("res");
      expect(ctx.insideMacro).toBe(true);
    });

    it("typing dotted path inside: has(resource.|", () => {
      const ctx = getCursorContext("has(resource.", 13, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.insideMacro).toBe(true);
    });

    it("full path before close: has(resource.labels|", () => {
      const ctx = getCursorContext("has(resource.labels", 19, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.insideMacro).toBe(true);
    });

    it("does not set insideMacro after normal paren", () => {
      const ctx = getCursorContext("(", 1, fields);
      expect(ctx.kind).toBe("field");
      expect(ctx.insideMacro).toBeUndefined();
    });

    it("after close paren: has(resource.labels)|", () => {
      const ctx = getCursorContext("has(resource.labels)", 20, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("space after close: has(resource.labels) |", () => {
      const ctx = getCursorContext("has(resource.labels) ", 21, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("typing &&: has(resource.labels) &&|", () => {
      const ctx = getCursorContext("has(resource.labels) &&", 23, fields);
      expect(ctx.kind).toBe("combinator");
    });

    it("after &&: has(resource.labels) && |", () => {
      const ctx = getCursorContext("has(resource.labels) && ", 24, fields);
      expect(ctx.kind).toBe("field");
    });
  });

  describe("half-written queries — mixed has + reversed in", () => {
    it('has() && "| — typing value after combinator', () => {
      const ctx = getCursorContext('has(resource.state) && "', 24, fields);
      // on a value token (typing the quote)
      expect(ctx.kind).toBe("value");
    });

    it('has() && "Ready"| — complete value in field position', () => {
      const ctx = getCursorContext(
        'has(resource.state) && "Ready"',
        30,
        fields,
      );
      // value is complete and follows combinator → suggest operators
      expect(ctx.kind).toBe("operator");
    });

    it('has() && "Ready" | — space after value', () => {
      const ctx = getCursorContext(
        'has(resource.state) && "Ready" ',
        31,
        fields,
      );
      // value after combinator → field position → should suggest operators
      expect(ctx.kind).toBe("operator");
    });

    it('has() && "Ready" in | — after in', () => {
      const ctx = getCursorContext(
        'has(resource.state) && "Ready" in ',
        34,
        fields,
      );
      expect(ctx.kind).toBe("field");
    });

    it('has() && "Ready" in resource.conditions | — done', () => {
      const ctx = getCursorContext(
        'has(resource.state) && "Ready" in resource.conditions ',
        54,
        fields,
      );
      expect(ctx.kind).toBe("combinator");
    });
  });
});
