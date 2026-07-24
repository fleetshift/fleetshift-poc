import { describe, expect, it } from "vitest";

import { FIELD_TREE } from "../fieldTree";
import { validateFieldTree } from "../fieldTreeSchema";

describe("fieldTree schema", () => {
  it("validates FIELD_TREE structure", () => {
    expect(() => validateFieldTree(FIELD_TREE)).not.toThrow();
  });

  it("rejects node without type or children", () => {
    expect(() =>
      validateFieldTree([{ segment: "x", path: "x", label: "X" }]),
    ).toThrow();
  });

  it("rejects node with empty segment", () => {
    expect(() =>
      validateFieldTree([
        { segment: "", path: "x", label: "X", type: "string" },
      ]),
    ).toThrow();
  });

  it("rejects node with empty label", () => {
    expect(() =>
      validateFieldTree([
        { segment: "x", path: "x", label: "", type: "string" },
      ]),
    ).toThrow();
  });

  it("accepts valid leaf node", () => {
    expect(() =>
      validateFieldTree([
        { segment: "name", path: "name", label: "Name", type: "string" },
      ]),
    ).not.toThrow();
  });

  it("accepts valid branch node", () => {
    expect(() =>
      validateFieldTree([
        {
          segment: "parent",
          path: "parent",
          label: "Parent",
          children: [
            {
              segment: "child",
              path: "parent.child",
              label: "Child",
              type: "string",
            },
          ],
        },
      ]),
    ).not.toThrow();
  });
});
