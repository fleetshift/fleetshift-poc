import type { FieldDef, OperatorDef } from "./types";

const OPERATORS: OperatorDef[] = [
  { rqbName: "=", celSyntax: "==", label: "equals", valueRequired: true },
  {
    rqbName: "!=",
    celSyntax: "!=",
    label: "not equals",
    valueRequired: true,
  },
  {
    rqbName: "beginsWith",
    celSyntax: ".startsWith()",
    label: "starts with",
    valueRequired: true,
  },
  { rqbName: ">", celSyntax: ">", label: "greater than", valueRequired: true },
  { rqbName: "<", celSyntax: "<", label: "less than", valueRequired: true },
  {
    rqbName: ">=",
    celSyntax: ">=",
    label: "greater or equal",
    valueRequired: true,
  },
  {
    rqbName: "<=",
    celSyntax: "<=",
    label: "less or equal",
    valueRequired: true,
  },
  {
    rqbName: "in",
    celSyntax: "in",
    label: "in list",
    valueRequired: true,
  },
];

const byRqbName = new Map(OPERATORS.map((op) => [op.rqbName, op]));

export function operatorByRqbName(name: string): OperatorDef | undefined {
  return byRqbName.get(name);
}

export function getAllOperators(): OperatorDef[] {
  return OPERATORS;
}

export function getOperatorsForField(field: FieldDef): OperatorDef[] {
  return field.operators
    .map((name) => byRqbName.get(name))
    .filter((op): op is OperatorDef => op !== undefined);
}
