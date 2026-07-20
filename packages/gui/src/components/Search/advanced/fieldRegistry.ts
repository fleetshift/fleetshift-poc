import type { FieldDef } from "./types";

const STRING_OPERATORS = ["=", "!=", "beginsWith", "in"];
const NUMERIC_OPERATORS = ["=", "!=", ">", "<", ">=", "<="];

const STATIC_FIELDS: FieldDef[] = [
  {
    name: "name",
    label: "Resource Name",
    type: "string",
    operators: STRING_OPERATORS,
  },
  {
    name: "resourceType",
    label: "Resource Type",
    type: "string",
    operators: STRING_OPERATORS,
  },
  {
    name: "resource.spec.name",
    label: "Spec Name",
    type: "string",
    operators: STRING_OPERATORS,
  },
  {
    name: "resource.spec.replicas",
    label: "Spec Replicas",
    type: "number",
    operators: NUMERIC_OPERATORS,
  },
  {
    name: "resource.metadata.labels",
    label: "Labels",
    type: "string",
    operators: ["=", "!="],
  },
];

const DYNAMIC_RESOURCE_FIELD: FieldDef = {
  name: "resource.*",
  label: "Resource Field",
  type: "string",
  operators: [...STRING_OPERATORS, ">", "<", ">=", "<="],
};

const byName = new Map(STATIC_FIELDS.map((f) => [f.name, f]));

export function getStaticFields(): FieldDef[] {
  return STATIC_FIELDS;
}

export function getFieldByName(name: string): FieldDef | undefined {
  return byName.get(name);
}

export function resolveField(name: string): FieldDef | undefined {
  const exact = byName.get(name);
  if (exact) return exact;
  if (name.startsWith("resource.")) return DYNAMIC_RESOURCE_FIELD;
  return undefined;
}
