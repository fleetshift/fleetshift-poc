import { describe, expect, it } from "vitest";

import { getStaticFields } from "../fieldRegistry";
import {
  getAllOperators,
  getOperatorsForField,
  operatorByRqbName,
} from "../operatorMap";

describe("operatorMap", () => {
  it("returns all operators", () => {
    const ops = getAllOperators();
    expect(ops.length).toBeGreaterThan(0);
    expect(ops.every((op) => op.rqbName && op.celSyntax && op.label)).toBe(
      true,
    );
  });

  it("looks up operator by RQB name", () => {
    const eq = operatorByRqbName("=");
    expect(eq).toBeDefined();
    expect(eq!.celSyntax).toBe("==");
    expect(eq!.label).toBe("equals");
  });

  it("returns undefined for unknown operator", () => {
    expect(operatorByRqbName("nope")).toBeUndefined();
  });

  it("returns operators for string field", () => {
    const nameField = getStaticFields().find((f) => f.name === "name")!;
    const ops = getOperatorsForField(nameField);
    expect(ops.length).toBeGreaterThan(0);
    const names = ops.map((op) => op.rqbName);
    expect(names).toContain("=");
    expect(names).toContain("beginsWith");
    expect(names).not.toContain("contains");
    expect(names).not.toContain("endsWith");
  });

  it("returns operators for numeric field", () => {
    const replicas = getStaticFields().find(
      (f) => f.name === "resource.observation.extracted.replicas",
    )!;
    const ops = getOperatorsForField(replicas);
    const names = ops.map((op) => op.rqbName);
    expect(names).toContain(">");
    expect(names).toContain("<");
    expect(names).not.toContain("contains");
  });

  it("all operators have valueRequired set", () => {
    for (const op of getAllOperators()) {
      expect(typeof op.valueRequired).toBe("boolean");
    }
  });
});
