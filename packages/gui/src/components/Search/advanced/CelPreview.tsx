import {
  CheckCircleIcon,
  ExclamationCircleIcon,
} from "@patternfly/react-icons";
import { useMemo } from "react";

import { tokenize } from "./tokenizer";
import type { ValidationResult } from "./types";

interface CelPreviewProps {
  expression: string;
  validation: ValidationResult;
}

const TOKEN_CLASSES: Record<string, string> = {
  field: "ome-search__cel-token--field",
  operator: "ome-search__cel-token--operator",
  value: "ome-search__cel-token--value",
  combinator: "ome-search__cel-token--combinator",
  "dot-call": "ome-search__cel-token--operator",
  paren: "ome-search__cel-token--paren",
};

export default function CelPreview({
  expression,
  validation,
}: CelPreviewProps) {
  const tokens = useMemo(() => tokenize(expression), [expression]);

  if (!expression.trim()) return null;

  return (
    <div className="ome-search__cel-preview">
      <div className="ome-search__cel-preview-status">
        {validation.valid ? (
          <CheckCircleIcon className="ome-search__cel-preview-icon--valid" />
        ) : (
          <ExclamationCircleIcon className="ome-search__cel-preview-icon--invalid" />
        )}
      </div>
      <code className="ome-search__cel-preview-expression">
        {tokens.map((token, i) => (
          <span key={i} className={TOKEN_CLASSES[token.type] ?? ""}>
            {token.value}
          </span>
        ))}
      </code>
      {!validation.valid && validation.error && (
        <span className="ome-search__cel-preview-error">
          {validation.error}
        </span>
      )}
    </div>
  );
}
