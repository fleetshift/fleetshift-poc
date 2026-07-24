export type {
  DefaultRuleGroupType as RuleGroupType,
  DefaultRuleType as RuleType,
} from "@react-querybuilder/core";

export interface FieldDef {
  name: string;
  label: string;
  type: "string" | "number" | "boolean" | "enum";
  operators: string[];
  enumValues?: string[];
}

export interface OperatorDef {
  rqbName: string;
  celSyntax: string;
  label: string;
  valueRequired: boolean;
}

export type CursorContextKind =
  | "field"
  | "operator"
  | "value"
  | "combinator"
  | "empty";

export interface CursorContext {
  kind: CursorContextKind;
  fieldName?: string;
  partial: string;
  replaceRange: [number, number];
  insideMacro?: boolean;
  reversedIn?: boolean;
}

export interface HistoryEntry {
  expression: string;
  timestamp: number;
  favorite: boolean;
}

export interface Suggestion {
  type: CursorContextKind | "path" | "semantic";
  value: string;
  label: string;
  description?: string;
  cursorOffset?: number;
  celPreview?: string;
}

export interface FieldNode {
  segment: string;
  path: string;
  label: string;
  description?: string;
  children?: FieldNode[];
  type?: "string" | "number" | "boolean";
  enumValues?: string[];
  container?: boolean;
}

export type TokenType =
  | "field"
  | "operator"
  | "value"
  | "combinator"
  | "paren"
  | "dot-call"
  | "macro"
  | "whitespace"
  | "unknown";

export interface Token {
  type: TokenType;
  value: string;
  start: number;
  end: number;
}

export interface ValidationResult {
  valid: boolean;
  error?: string;
}
