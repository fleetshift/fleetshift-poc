import type { RefObject } from "react";
import { type ReactNode, useMemo } from "react";

interface EventLogTerminalProps {
  lines: string[];
  searchTerm: string;
  containerRef: RefObject<HTMLDivElement | null>;
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function highlightLine(line: string, searchTerm: string): ReactNode {
  if (searchTerm.length < 2) return line;

  const pattern = new RegExp(`(${escapeRegExp(searchTerm)})`, "gi");
  const parts = line.split(pattern);
  if (parts.length === 1) return line;

  return parts.map((part, i) =>
    pattern.test(part) ? (
      <mark key={i} className="ome-gcphcp-terminal__highlight">
        {part}
      </mark>
    ) : (
      part
    ),
  );
}

export default function EventLogTerminal({
  lines,
  searchTerm,
  containerRef,
}: EventLogTerminalProps) {
  const rendered = useMemo(
    () => lines.map((line) => highlightLine(line, searchTerm)),
    [lines, searchTerm],
  );

  return (
    <div ref={containerRef} className="ome-gcphcp-terminal">
      {lines.length === 0 ? (
        <div className="ome-gcphcp-terminal__empty">
          Waiting for delivery events...
        </div>
      ) : (
        rendered.map((content, i) => (
          <div key={i} className="ome-gcphcp-terminal__line">
            <span className="ome-gcphcp-terminal__line-number">{i + 1}</span>
            <span className="ome-gcphcp-terminal__line-text">{content}</span>
          </div>
        ))
      )}
    </div>
  );
}
