import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { validateCel } from "./celValidator";
import { getCursorContext } from "./cursorParser";
import { getStaticFields, resolveField } from "./fieldRegistry";
import { getChildrenAt, getNodeAt, getTopLevelNodes } from "./fieldTree";
import { getOperatorsForField } from "./operatorMap";
import { querySemantic } from "./semanticIndex";
import type { FieldNode, Suggestion, ValidationResult } from "./types";
import { useSearchHistory } from "./useSearchHistory";

const fields = getStaticFields();

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

function nodeToSuggestion(node: FieldNode, asShortcut: boolean): Suggestion {
  const isLeaf = !node.children;
  return {
    type: "path",
    value: asShortcut
      ? node.path + (isLeaf ? " " : ".")
      : node.segment + (isLeaf ? " " : "."),
    label: node.label,
    description: node.description ?? (isLeaf ? node.type : "…"),
    celPreview: asShortcut ? node.path : undefined,
  };
}

function filterNodes(
  nodes: FieldNode[],
  partial: string,
  asShortcut: boolean,
): Suggestion[] {
  const lower = partial.toLowerCase();
  return nodes
    .filter(
      (n) =>
        n.segment.toLowerCase().startsWith(lower) ||
        n.label.toLowerCase().includes(lower),
    )
    .map((n) => nodeToSuggestion(n, asShortcut));
}

async function computeFieldSuggestions(partial: string): Promise<Suggestion[]> {
  if (!partial || !partial.includes(".")) {
    const topLevel = getTopLevelNodes();

    if (!partial) {
      return topLevel.map((n) => nodeToSuggestion(n, false));
    }

    const filtered = filterNodes(topLevel, partial, false);

    if (filtered.length > 0) return filtered;

    const semantic = await querySemantic(partial);
    return semantic.map((s) => ({
      type: "semantic" as const,
      value: s.expression,
      label: s.label,
      description: s.category,
      celPreview: s.expression,
    }));
  }

  if (partial.endsWith(".")) {
    const children = getChildrenAt(partial);
    if (children.length > 0) {
      return children.map((n) => nodeToSuggestion(n, false));
    }
    return [];
  }

  const lastDot = partial.lastIndexOf(".");
  const parentPath = partial.slice(0, lastDot + 1);
  const typedSegment = partial.slice(lastDot + 1);
  const children = getChildrenAt(parentPath);
  return filterNodes(children, typedSegment, false);
}

async function computeValueSuggestions(
  fieldName: string | undefined,
  partial: string,
): Promise<Suggestion[]> {
  const field = fieldName ? resolveField(fieldName) : undefined;

  if (field?.enumValues) {
    const cleanPartial = partial.replace(/"/g, "").toLowerCase();
    return field.enumValues
      .filter((v) => !cleanPartial || v.toLowerCase().includes(cleanPartial))
      .map((v) => ({
        type: "value" as const,
        value: `"${v}" `,
        label: v,
      }));
  }

  const node = fieldName ? getNodeAt(fieldName) : undefined;
  if (node?.enumValues) {
    const cleanPartial = partial.replace(/"/g, "").toLowerCase();
    return node.enumValues
      .filter((v) => !cleanPartial || v.toLowerCase().includes(cleanPartial))
      .map((v) => ({
        type: "value" as const,
        value: `"${v}" `,
        label: v,
      }));
  }

  if (!partial || partial === '""' || partial === '"') {
    const suggestions: Suggestion[] = [
      {
        type: "value",
        value: '""',
        label: '""',
        description: "Type a value",
        cursorOffset: -1,
      },
    ];

    if (fieldName) {
      const semantic = await querySemantic(
        fieldName.split(".").pop() ?? fieldName,
      );
      for (const s of semantic.slice(0, 4)) {
        suggestions.push({
          type: "semantic",
          value: s.valueOnly + " ",
          label: s.label,
          description: s.category,
          celPreview: s.expression,
        });
      }
    }
    return suggestions;
  }

  const cleanPartial = partial.replace(/"/g, "");
  if (cleanPartial.length >= 2) {
    const semantic = await querySemantic(cleanPartial);
    if (semantic.length > 0) {
      return semantic.map((s) => ({
        type: "semantic" as const,
        value: s.valueOnly + " ",
        label: s.label,
        description: s.category,
        celPreview: s.expression,
      }));
    }
  }

  return [];
}

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
    let cancelled = false;

    async function compute() {
      let result: Suggestion[];

      switch (context.kind) {
        case "field":
          result = await computeFieldSuggestions(context.partial);
          break;

        case "operator": {
          const field = context.fieldName
            ? resolveField(context.fieldName)
            : undefined;
          const ops = field ? getOperatorsForField(field) : [];
          result = ops.map((op) => ({
            type: "operator" as const,
            value: op.celSyntax.includes(".")
              ? op.celSyntax
              : op.celSyntax + " ",
            label: op.celSyntax,
            description: op.label,
          }));
          break;
        }

        case "value":
          result = await computeValueSuggestions(
            context.fieldName,
            context.partial,
          );
          break;

        case "combinator":
          result = COMBINATOR_SUGGESTIONS;
          break;

        default:
          result = [];
      }

      if (!cancelled) setSuggestions(result);
    }

    compute();
    return () => {
      cancelled = true;
    };
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
      if (suggestion.type === "semantic" && context.kind === "field") {
        setExpression(suggestion.value);
        const newPos = suggestion.value.length;
        setCursorPos(newPos);
        return newPos + (suggestion.cursorOffset ?? 0);
      }

      const [start, end] = context.replaceRange;
      const before = expression.slice(0, start);
      const after = expression.slice(end);
      const newExpr = before + suggestion.value + after;
      const newPos = before.length + suggestion.value.length;
      setExpression(newExpr);
      setCursorPos(newPos);
      return newPos + (suggestion.cursorOffset ?? 0);
    },
    [expression, context],
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
