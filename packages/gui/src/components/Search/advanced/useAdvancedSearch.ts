import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { validateCel } from "./celValidator";
import { getCursorContext } from "./cursorParser";
import { getStaticFields, resolveField } from "./fieldRegistry";
import { createFieldIndex, queryFields } from "./fieldSearchIndex";
import { getOperatorsForField } from "./operatorMap";
import type { Suggestion, ValidationResult } from "./types";
import { useSearchHistory } from "./useSearchHistory";

const fields = getStaticFields();
const fieldIndex = createFieldIndex(fields);

const COMBINATOR_SUGGESTIONS: Suggestion[] = [
  {
    type: "combinator",
    value: "&& ",
    label: "AND",
    description: "Both conditions must be true",
  },
  {
    type: "combinator",
    value: "|| ",
    label: "OR",
    description: "Either condition can be true",
  },
];

export function useAdvancedSearch() {
  const [expression, setExpression] = useState("");
  const [cursorPos, setCursorPos] = useState(0);
  const [validation, setValidation] = useState<ValidationResult>({
    valid: true,
  });
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();
  const history = useSearchHistory();

  const context = useMemo(
    () => getCursorContext(expression, cursorPos, fields),
    [expression, cursorPos],
  );

  useEffect(() => {
    async function computeSuggestions() {
      switch (context.kind) {
        case "field": {
          const matched = await queryFields(
            fieldIndex,
            context.partial,
            fields,
          );
          setSuggestions(
            matched.map((f) => ({
              type: "field" as const,
              value: f.name + " ",
              label: f.name,
              description: f.label,
            })),
          );
          break;
        }
        case "operator": {
          const field = context.fieldName
            ? resolveField(context.fieldName)
            : undefined;
          const ops = field ? getOperatorsForField(field) : [];
          setSuggestions(
            ops.map((op) => ({
              type: "operator" as const,
              value: op.celSyntax.includes(".")
                ? op.celSyntax
                : op.celSyntax + " ",
              label: op.celSyntax,
              description: op.label,
            })),
          );
          break;
        }
        case "value": {
          const field = context.fieldName
            ? resolveField(context.fieldName)
            : undefined;
          if (field?.enumValues) {
            setSuggestions(
              field.enumValues
                .filter(
                  (v) =>
                    !context.partial ||
                    v
                      .toLowerCase()
                      .includes(
                        context.partial.replace(/"/g, "").toLowerCase(),
                      ),
                )
                .map((v) => ({
                  type: "value" as const,
                  value: `"${v}" `,
                  label: v,
                })),
            );
          } else {
            setSuggestions([]);
          }
          break;
        }
        case "combinator": {
          setSuggestions(COMBINATOR_SUGGESTIONS);
          break;
        }
        default:
          setSuggestions([]);
      }
    }
    computeSuggestions();
  }, [context]);

  useEffect(() => {
    clearTimeout(debounceRef.current);
    if (!expression.trim()) {
      setValidation({ valid: true });
      return;
    }
    debounceRef.current = setTimeout(() => {
      setValidation(validateCel(expression));
    }, 300);
    return () => clearTimeout(debounceRef.current);
  }, [expression]);

  const acceptSuggestion = useCallback(
    (suggestion: Suggestion): number => {
      const [start, end] = context.replaceRange;
      const before = expression.slice(0, start);
      const after = expression.slice(end);
      const newExpr = before + suggestion.value + after;
      const newPos = before.length + suggestion.value.length;
      setExpression(newExpr);
      setCursorPos(newPos);
      return newPos;
    },
    [expression, context.replaceRange],
  );

  const execute = useCallback(() => {
    if (!expression.trim()) return;
    history.save(expression);
    return expression.trim();
  }, [expression, history]);

  const loadExpression = useCallback((expr: string) => {
    setExpression(expr);
    setCursorPos(expr.length);
  }, []);

  return {
    expression,
    setExpression,
    cursorPos,
    setCursorPos,
    context,
    suggestions,
    acceptSuggestion,
    validation,
    execute,
    loadExpression,
    history: history.entries,
    historyLoaded: history.loaded,
    toggleFavorite: history.toggleFavorite,
    removeHistory: history.remove,
  };
}
