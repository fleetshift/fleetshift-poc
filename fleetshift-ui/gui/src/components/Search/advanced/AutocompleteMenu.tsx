import { MenuGroup, MenuItem } from "@patternfly/react-core";
import clsx from "clsx";
import { useCallback, useEffect, useRef } from "react";

import type { Suggestion } from "./types";

interface AutocompleteMenuProps {
  suggestions: Suggestion[];
  selectedIndex: number;
  onSelect: (suggestion: Suggestion) => void;
  onIndexChange: (index: number) => void;
}

const TYPE_LABELS: Record<string, string> = {
  field: "Fields",
  path: "Fields",
  operator: "Operators",
  value: "Values",
  combinator: "Logic",
  semantic: "Did you mean?",
};

export default function AutocompleteMenu({
  suggestions,
  selectedIndex,
  onSelect,
  onIndexChange,
}: AutocompleteMenuProps) {
  const itemRefs = useRef<Map<number, HTMLElement>>(new Map());

  useEffect(() => {
    const el = itemRefs.current.get(selectedIndex);
    el?.scrollIntoView({ block: "nearest" });
  }, [selectedIndex]);

  const handleItemClick = useCallback(
    (idx: number) => {
      onSelect(suggestions[idx]);
    },
    [onSelect, suggestions],
  );

  if (suggestions.length === 0) return null;

  const grouped = new Map<
    string,
    { suggestion: Suggestion; index: number }[]
  >();
  suggestions.forEach((s, i) => {
    const group = grouped.get(s.type) ?? [];
    group.push({ suggestion: s, index: i });
    grouped.set(s.type, group);
  });

  return (
    <>
      {Array.from(grouped.entries()).map(([type, items]) =>
        type === "operator" ? (
          <MenuGroup key={type} label={TYPE_LABELS[type]}>
            <div className="ome-search__operator-chips">
              {items.map(({ suggestion, index }) => (
                <button
                  key={`${type}-${index}`}
                  ref={(el: HTMLElement | null) => {
                    if (el) itemRefs.current.set(index, el);
                    else itemRefs.current.delete(index);
                  }}
                  className={clsx(
                    "ome-search__operator-chip",
                    index === selectedIndex &&
                      "ome-search__operator-chip--active",
                  )}
                  onClick={() => handleItemClick(index)}
                  onMouseEnter={() => onIndexChange(index)}
                  title={suggestion.description}
                  type="button"
                >
                  <code>{suggestion.label}</code>
                </button>
              ))}
            </div>
          </MenuGroup>
        ) : (
          <MenuGroup key={type} label={TYPE_LABELS[type] ?? type}>
            {items.map(({ suggestion, index }) => (
              <MenuItem
                key={`${type}-${index}`}
                ref={(el: HTMLElement | null) => {
                  if (el) itemRefs.current.set(index, el);
                  else itemRefs.current.delete(index);
                }}
                isFocused={index === selectedIndex}
                onClick={() => handleItemClick(index)}
                onMouseEnter={() => onIndexChange(index)}
                description={
                  suggestion.celPreview ? (
                    <code>{suggestion.celPreview}</code>
                  ) : (
                    suggestion.description
                  )
                }
              >
                {suggestion.label}
              </MenuItem>
            ))}
          </MenuGroup>
        ),
      )}
    </>
  );
}
