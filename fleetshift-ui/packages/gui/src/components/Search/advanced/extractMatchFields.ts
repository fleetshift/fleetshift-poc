import { tokenize } from "./tokenizer";

export function extractFieldPaths(expression: string): string[] {
  const tokens = tokenize(expression);
  const seen = new Set<string>();
  const result: string[] = [];
  for (const t of tokens) {
    if (t.type === "field" && !seen.has(t.value)) {
      seen.add(t.value);
      result.push(t.value);
    }
  }
  return result;
}

export function resolveFieldValue(
  obj: Record<string, unknown>,
  path: string,
): unknown {
  if (!path) return undefined;
  const segments = path.split(".");
  let current: unknown = obj;
  for (const seg of segments) {
    if (current == null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[seg];
  }
  return current;
}
