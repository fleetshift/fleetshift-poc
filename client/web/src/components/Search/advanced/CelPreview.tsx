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
  field: "ocm-rm-search__cel-token--field",
  operator: "ocm-rm-search__cel-token--operator",
  value: "ocm-rm-search__cel-token--value",
  combinator: "ocm-rm-search__cel-token--combinator",
  "dot-call": "ocm-rm-search__cel-token--operator",
  paren: "ocm-rm-search__cel-token--paren",
};

export default function CelPreview({
  expression,
  validation,
}: CelPreviewProps) {
  const tokens = useMemo(() => tokenize(expression), [expression]);

  if (!expression.trim()) return null;

  return (
    <div className="ocm-rm-search__cel-preview">
      <div className="ocm-rm-search__cel-preview-status">
        {validation.valid ? (
          <CheckCircleIcon className="ocm-rm-search__cel-preview-icon--valid" />
        ) : (
          <ExclamationCircleIcon className="ocm-rm-search__cel-preview-icon--invalid" />
        )}
      </div>
      <code className="ocm-rm-search__cel-preview-expression">
        {tokens.map((token, i) => (
          <span key={i} className={TOKEN_CLASSES[token.type] ?? ""}>
            {token.value}
          </span>
        ))}
      </code>
      {!validation.valid && validation.error && (
        <span className="ocm-rm-search__cel-preview-error">
          {validation.error}
        </span>
      )}
    </div>
  );
}
