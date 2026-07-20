import type { Token } from "./types";

const COMPARISON_OPS = ["==", "!=", ">=", "<=", ">", "<"];
const COMBINATORS = ["&&", "||"];
const DOT_METHODS = ["startsWith"];

export function tokenize(expression: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;

  while (i < expression.length) {
    if (expression[i] === " " || expression[i] === "\t") {
      const start = i;
      while (
        i < expression.length &&
        (expression[i] === " " || expression[i] === "\t")
      ) {
        i++;
      }
      tokens.push({
        type: "whitespace",
        value: expression.slice(start, i),
        start,
        end: i,
      });
      continue;
    }

    if (expression[i] === "(" || expression[i] === ")") {
      tokens.push({
        type: "paren",
        value: expression[i],
        start: i,
        end: i + 1,
      });
      i++;
      continue;
    }

    if (expression[i] === '"' || expression[i] === "'") {
      const quote = expression[i];
      const start = i;
      i++;
      while (i < expression.length && expression[i] !== quote) {
        if (expression[i] === "\\") i++;
        i++;
      }
      if (i < expression.length) i++;
      tokens.push({
        type: "value",
        value: expression.slice(start, i),
        start,
        end: i,
      });
      continue;
    }

    const twoChar = expression.slice(i, i + 2);
    if (COMBINATORS.includes(twoChar)) {
      tokens.push({ type: "combinator", value: twoChar, start: i, end: i + 2 });
      i += 2;
      continue;
    }

    if (COMPARISON_OPS.includes(twoChar)) {
      tokens.push({ type: "operator", value: twoChar, start: i, end: i + 2 });
      i += 2;
      continue;
    }

    if (
      (expression[i] === ">" || expression[i] === "<") &&
      expression[i + 1] !== "="
    ) {
      tokens.push({
        type: "operator",
        value: expression[i],
        start: i,
        end: i + 1,
      });
      i++;
      continue;
    }

    if (expression[i] === ".") {
      const methodMatch = DOT_METHODS.find(
        (m) => expression.slice(i + 1, i + 1 + m.length) === m,
      );
      if (methodMatch) {
        const start = i;
        i += 1 + methodMatch.length;
        if (expression[i] === "(") {
          let depth = 1;
          i++;
          while (i < expression.length && depth > 0) {
            if (expression[i] === "(") depth++;
            else if (expression[i] === ")") depth--;
            if (depth > 0) i++;
          }
          if (i < expression.length) i++;
        }
        tokens.push({
          type: "dot-call",
          value: expression.slice(start, i),
          start,
          end: i,
        });
        continue;
      }
    }

    if (expression[i] === "!" && expression[i + 1] !== "=") {
      tokens.push({ type: "operator", value: "!", start: i, end: i + 1 });
      i++;
      continue;
    }

    if (/[a-zA-Z_]/.test(expression[i])) {
      const start = i;
      while (i < expression.length && /[a-zA-Z0-9_.]/.test(expression[i])) {
        if (expression[i] === ".") {
          const afterDot = expression.slice(i + 1);
          const isMethod = DOT_METHODS.some((m) => afterDot.startsWith(m));
          if (isMethod) break;
        }
        i++;
      }
      const word = expression.slice(start, i);

      if (word === "in") {
        tokens.push({ type: "operator", value: word, start, end: i });
      } else if (word === "true" || word === "false") {
        tokens.push({ type: "value", value: word, start, end: i });
      } else {
        tokens.push({ type: "field", value: word, start, end: i });
      }
      continue;
    }

    if (/[0-9]/.test(expression[i])) {
      const start = i;
      while (i < expression.length && /[0-9.]/.test(expression[i])) {
        i++;
      }
      tokens.push({
        type: "value",
        value: expression.slice(start, i),
        start,
        end: i,
      });
      continue;
    }

    if (expression[i] === "[") {
      const start = i;
      let depth = 1;
      i++;
      while (i < expression.length && depth > 0) {
        if (expression[i] === "[") depth++;
        else if (expression[i] === "]") depth--;
        i++;
      }
      tokens.push({
        type: "value",
        value: expression.slice(start, i),
        start,
        end: i,
      });
      continue;
    }

    tokens.push({
      type: "unknown",
      value: expression[i],
      start: i,
      end: i + 1,
    });
    i++;
  }

  return tokens;
}
