import { tokenize } from "./tokenizer";
import type { CursorContext, FieldDef, Token } from "./types";

export function getCursorContext(
  expression: string,
  cursorPos: number,
  _fields: FieldDef[],
): CursorContext {
  if (expression.trim().length === 0) {
    return { kind: "field", partial: "", replaceRange: [0, 0] };
  }

  const tokens = tokenize(expression);
  const meaningful = tokens.filter((t) => t.type !== "whitespace");

  const currentToken = findTokenAtCursor(tokens, cursorPos);
  const prevMeaningful = findPrevMeaningful(meaningful, cursorPos);

  if (currentToken && currentToken.type !== "whitespace") {
    const partial = expression.slice(currentToken.start, cursorPos);
    const replaceRange: [number, number] = [
      currentToken.start,
      currentToken.end,
    ];

    if (currentToken.type === "field") {
      const beforeField = findPrevMeaningful(meaningful, currentToken.start);
      if (
        !beforeField ||
        beforeField.type === "combinator" ||
        beforeField.type === "paren"
      ) {
        return { kind: "field", partial, replaceRange };
      }
      if (beforeField.type === "operator") {
        return {
          kind: "value",
          fieldName: findFieldBefore(meaningful, beforeField),
          partial,
          replaceRange,
        };
      }
    }

    if (currentToken.type === "operator") {
      return {
        kind: "operator",
        fieldName: findFieldBefore(meaningful, currentToken),
        partial,
        replaceRange,
      };
    }

    if (currentToken.type === "value" || currentToken.type === "dot-call") {
      return {
        kind: "value",
        fieldName: findFieldBeforeValue(meaningful, currentToken),
        partial,
        replaceRange,
      };
    }

    if (currentToken.type === "combinator") {
      return { kind: "combinator", partial, replaceRange };
    }
  }

  const insertPos = cursorPos;
  const replaceRange: [number, number] = [insertPos, insertPos];

  if (!prevMeaningful) {
    return { kind: "field", partial: "", replaceRange };
  }

  if (prevMeaningful.type === "field") {
    const beforeField = findPrevMeaningful(meaningful, prevMeaningful.start);
    if (
      !beforeField ||
      beforeField.type === "combinator" ||
      beforeField.type === "paren"
    ) {
      return {
        kind: "operator",
        fieldName: prevMeaningful.value,
        partial: "",
        replaceRange,
      };
    }
  }

  if (prevMeaningful.type === "operator") {
    return {
      kind: "value",
      fieldName: findFieldBefore(meaningful, prevMeaningful),
      partial: "",
      replaceRange,
    };
  }

  if (prevMeaningful.type === "value" || prevMeaningful.type === "dot-call") {
    return { kind: "combinator", partial: "", replaceRange };
  }

  if (prevMeaningful.type === "combinator" || prevMeaningful.type === "paren") {
    return { kind: "field", partial: "", replaceRange };
  }

  return { kind: "field", partial: "", replaceRange };
}

function findTokenAtCursor(tokens: Token[], pos: number): Token | undefined {
  return tokens.find(
    (t) => t.start <= pos && pos <= t.end && t.start !== t.end,
  );
}

function findPrevMeaningful(
  meaningful: Token[],
  pos: number,
): Token | undefined {
  let prev: Token | undefined;
  for (const t of meaningful) {
    if (t.end <= pos) prev = t;
    else break;
  }
  return prev;
}

function findFieldBefore(
  meaningful: Token[],
  anchor: Token,
): string | undefined {
  const idx = meaningful.indexOf(anchor);
  if (idx <= 0) return undefined;
  const prev = meaningful[idx - 1];
  return prev.type === "field" ? prev.value : undefined;
}

function findFieldBeforeValue(
  meaningful: Token[],
  anchor: Token,
): string | undefined {
  const idx = meaningful.indexOf(anchor);
  for (let i = idx - 1; i >= 0; i--) {
    if (meaningful[i].type === "field") return meaningful[i].value;
    if (meaningful[i].type === "combinator" || meaningful[i].type === "paren")
      break;
  }
  return undefined;
}
