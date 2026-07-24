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
    let replaceRange: [number, number] = [currentToken.start, currentToken.end];

    if (currentToken.type === "field") {
      const lastDot = partial.lastIndexOf(".");
      if (lastDot !== -1) {
        replaceRange = [currentToken.start + lastDot + 1, currentToken.end];
      }

      const beforeField = findPrevMeaningful(meaningful, currentToken.start);
      if (
        !beforeField ||
        beforeField.type === "combinator" ||
        beforeField.type === "paren"
      ) {
        const insideMacro =
          beforeField?.type === "paren" && beforeField.value === "("
            ? findPrevMeaningful(meaningful, beforeField.start)?.type ===
                "macro" || undefined
            : undefined;
        return { kind: "field", partial, replaceRange, insideMacro };
      }
      if (beforeField.type === "operator") {
        if (isReversedIn(meaningful, beforeField)) {
          return { kind: "field", partial, replaceRange, reversedIn: true };
        }
        return {
          kind: "value",
          fieldName: findFieldBefore(meaningful, beforeField),
          partial,
          replaceRange,
        };
      }
    }

    if (currentToken.type === "operator") {
      if (
        cursorPos === currentToken.end &&
        isReversedIn(meaningful, currentToken)
      ) {
        return {
          kind: "field",
          partial: "",
          replaceRange: [cursorPos, cursorPos],
          reversedIn: true,
        };
      }
      return {
        kind: "operator",
        fieldName: findFieldBefore(meaningful, currentToken),
        partial,
        replaceRange,
      };
    }

    if (currentToken.type === "value" || currentToken.type === "dot-call") {
      if (
        cursorPos === currentToken.end &&
        isCompleteValue(currentToken) &&
        isValueInFieldPosition(meaningful, currentToken)
      ) {
        return {
          kind: "operator",
          partial: "",
          replaceRange: [cursorPos, cursorPos],
        };
      }
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
    if (
      beforeField.type === "operator" &&
      isReversedIn(meaningful, beforeField)
    ) {
      return { kind: "combinator", partial: "", replaceRange };
    }
  }

  if (prevMeaningful.type === "operator") {
    if (isReversedIn(meaningful, prevMeaningful)) {
      return { kind: "field", partial: "", replaceRange, reversedIn: true };
    }
    return {
      kind: "value",
      fieldName: findFieldBefore(meaningful, prevMeaningful),
      partial: "",
      replaceRange,
    };
  }

  if (prevMeaningful.type === "value" || prevMeaningful.type === "dot-call") {
    if (isValueInFieldPosition(meaningful, prevMeaningful)) {
      return { kind: "operator", partial: "", replaceRange };
    }
    return { kind: "combinator", partial: "", replaceRange };
  }

  if (prevMeaningful.type === "combinator") {
    return { kind: "field", partial: "", replaceRange };
  }

  if (prevMeaningful.type === "paren") {
    if (prevMeaningful.value === ")") {
      return { kind: "combinator", partial: "", replaceRange };
    }
    const beforeParen = findPrevMeaningful(meaningful, prevMeaningful.start);
    const insideMacro = beforeParen?.type === "macro" ? true : undefined;
    return { kind: "field", partial: "", replaceRange, insideMacro };
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

function isCompleteValue(token: Token): boolean {
  const v = token.value;
  if (v.length < 2) return false;
  const q = v[0];
  return (q === '"' || q === "'") && v[v.length - 1] === q;
}

function isReversedIn(meaningful: Token[], operatorToken: Token): boolean {
  if (operatorToken.value !== "in") return false;
  const idx = meaningful.indexOf(operatorToken);
  if (idx <= 0) return false;
  return meaningful[idx - 1].type === "value";
}

function isValueInFieldPosition(
  meaningful: Token[],
  valueToken: Token,
): boolean {
  const idx = meaningful.indexOf(valueToken);
  if (idx === 0) return true;
  const prev = meaningful[idx - 1];
  return (
    prev.type === "combinator" || (prev.type === "paren" && prev.value === "(")
  );
}
