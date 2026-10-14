import { getAllLeaves, getNodeAt } from "./fieldTree";
import type { FieldDef } from "./types";

const STRING_OPERATORS = ["=", "!=", "beginsWith", "in"];
const NUMERIC_OPERATORS = ["=", "!=", ">", "<", ">=", "<="];
const BOOLEAN_OPERATORS = ["=", "!="];

function nodeToFieldDef(node: {
  path: string;
  label: string;
  type?: string;
  enumValues?: string[];
}): FieldDef {
  const t = node.type ?? "string";
  let operators: string[];
  if (t === "number") operators = NUMERIC_OPERATORS;
  else if (t === "boolean") operators = BOOLEAN_OPERATORS;
  else operators = STRING_OPERATORS;

  const def: FieldDef = {
    name: node.path,
    label: node.label,
    type: node.enumValues ? "enum" : (t as FieldDef["type"]),
    operators,
  };
  if (node.enumValues) def.enumValues = node.enumValues;
  return def;
}

const DYNAMIC_RESOURCE_FIELD: FieldDef = {
  name: "resource.*",
  label: "Resource Field",
  type: "string",
  operators: [...STRING_OPERATORS, ">", "<", ">=", "<="],
};

let cachedLeaves: FieldDef[] | undefined;

export function getStaticFields(): FieldDef[] {
  if (cachedLeaves) return cachedLeaves;
  cachedLeaves = getAllLeaves().map(nodeToFieldDef);
  return cachedLeaves;
}

export function getFieldByName(name: string): FieldDef | undefined {
  return getStaticFields().find((f) => f.name === name);
}

export function resolveField(name: string): FieldDef | undefined {
  const exact = getStaticFields().find((f) => f.name === name);
  if (exact) return exact;

  const node = getNodeAt(name);
  if (node && !node.children) return nodeToFieldDef(node);

  if (name.startsWith("resource.")) return DYNAMIC_RESOURCE_FIELD;
  return undefined;
}
