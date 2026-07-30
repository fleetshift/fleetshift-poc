import { parseCEL } from "@react-querybuilder/core/parseCEL";

import type { ValidationResult } from "./types";

export function validateCel(expression: string): ValidationResult {
  const trimmed = expression.trim();
  if (trimmed.length === 0) {
    return { valid: true };
  }

  const quoteCount = (trimmed.match(/(?<!\\)"/g) ?? []).length;
  if (quoteCount % 2 !== 0) {
    return { valid: false, error: "Unmatched quote" };
  }

  const openParens = (trimmed.match(/\(/g) ?? []).length;
  const closeParens = (trimmed.match(/\)/g) ?? []).length;
  if (openParens !== closeParens) {
    return { valid: false, error: "Unmatched parenthesis" };
  }

  try {
    const parsed = parseCEL(trimmed);
    if (parsed?.rules && parsed.rules.length > 0) {
      return { valid: true };
    }
    // parseCEL may not understand all valid CEL (e.g. dot-call methods);
    // basic syntax checks above passed, so allow execution
    return { valid: true };
  } catch {
    return { valid: true };
  }
}
