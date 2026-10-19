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
  field: "ocm-fleet-search__cel-token--field",
  operator: "ocm-fleet-search__cel-token--operator",
  value: "ocm-fleet-search__cel-token--value",
  combinator: "ocm-fleet-search__cel-token--combinator",
  "dot-call": "ocm-fleet-search__cel-token--operator",
  paren: "ocm-fleet-search__cel-token--paren",
};

export default function CelPreview({
  expression,
  validation,
}: CelPreviewProps) {
  const tokens = useMemo(() => tokenize(expression), [expression]);

  if (!expression.trim()) return null;

  return (
    <div className="ocm-fleet-search__cel-preview">
      <div className="ocm-fleet-search__cel-preview-status">
        {validation.valid ? (
          <CheckCircleIcon className="ocm-fleet-search__cel-preview-icon--valid" />
        ) : (
          <ExclamationCircleIcon className="ocm-fleet-search__cel-preview-icon--invalid" />
        )}
      </div>
      <code className="ocm-fleet-search__cel-preview-expression">
        {tokens.map((token, i) => (
          <span key={i} className={TOKEN_CLASSES[token.type] ?? ""}>
            {token.value}
          </span>
        ))}
      </code>
      {!validation.valid && validation.error && (
        <span className="ocm-fleet-search__cel-preview-error">
          {validation.error}
        </span>
      )}
    </div>
  );
}
