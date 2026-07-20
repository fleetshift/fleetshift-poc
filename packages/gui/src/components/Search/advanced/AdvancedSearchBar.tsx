import "./advanced-search.scss";

import {
  Divider,
  Menu,
  MenuContent,
  MenuFooter,
  MenuGroup,
  MenuItem,
  MenuList,
  Spinner,
  TextInput,
} from "@patternfly/react-core";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

import AutocompleteMenu from "./AutocompleteMenu";
import CelPreview from "./CelPreview";
import HistoryPanel from "./HistoryPanel";
import type { Suggestion } from "./types";
import { useAdvancedSearch } from "./useAdvancedSearch";

interface AdvancedSearchBarProps {
  onDeactivate: () => void;
  onExecute: (expression: string) => void;
  onExpressionChange?: (expression: string) => void;
  results?: ReactNode;
  lastFilter?: string;
  isLoading?: boolean;
}

export default function AdvancedSearchBar({
  onDeactivate,
  onExecute,
  onExpressionChange,
  results,
  lastFilter,
  isLoading,
}: AdvancedSearchBarProps) {
  const {
    expression,
    setExpression,
    setCursorPos,
    suggestions,
    acceptSuggestion,
    validation,
    execute,
    loadExpression,
    history,
    toggleFavorite,
    removeHistory,
  } = useAdvancedSearch();

  const [selectedIndex, setSelectedIndex] = useState(0);
  const [menuVisible, setMenuVisible] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const pendingCursorRef = useRef<number | null>(null);

  const handleChange = useCallback(
    (_e: unknown, value: string) => {
      setExpression(value);
      const pos = inputRef.current?.selectionStart ?? value.length;
      setCursorPos(pos);
      setSelectedIndex(0);
      setMenuVisible(true);
      onExpressionChange?.(value);
    },
    [setExpression, setCursorPos, onExpressionChange],
  );

  const handleSelect = useCallback(
    (suggestion: Suggestion) => {
      const newPos = acceptSuggestion(suggestion);
      pendingCursorRef.current = newPos;
      setMenuVisible(true);
      setSelectedIndex(0);
    },
    [acceptSuggestion],
  );

  const handleKeyDown = useCallback(
    (ev: React.KeyboardEvent) => {
      if (ev.key === "Escape") {
        if (menuVisible) {
          setMenuVisible(false);
        } else {
          onDeactivate();
        }
        ev.preventDefault();
        return;
      }

      if (ev.key === "Enter") {
        setMenuVisible(false);
        const result = execute();
        if (result) onExecute(result);
        ev.preventDefault();
        return;
      }

      if (ev.key === "Tab" && suggestions.length > 0 && menuVisible) {
        ev.preventDefault();
        handleSelect(suggestions[selectedIndex]);
        return;
      }

      if (ev.key === "ArrowDown" && menuVisible) {
        ev.preventDefault();
        setSelectedIndex((prev) =>
          prev < suggestions.length - 1 ? prev + 1 : 0,
        );
        return;
      }

      if (ev.key === "ArrowUp" && menuVisible) {
        ev.preventDefault();
        setSelectedIndex((prev) =>
          prev > 0 ? prev - 1 : suggestions.length - 1,
        );
        return;
      }
    },
    [
      menuVisible,
      suggestions,
      selectedIndex,
      handleSelect,
      execute,
      onDeactivate,
      onExecute,
    ],
  );

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    if (pendingCursorRef.current !== null) {
      const pos = pendingCursorRef.current;
      pendingCursorRef.current = null;
      requestAnimationFrame(() => {
        const el = inputRef.current;
        if (el) {
          el.focus();
          el.setSelectionRange(pos, pos);
        }
      });
    }
  }, [expression]);

  const handleClick = useCallback(() => {
    const pos = inputRef.current?.selectionStart ?? expression.length;
    setCursorPos(pos);
    setMenuVisible(true);
  }, [expression.length, setCursorPos]);

  const handleBlur = useCallback((e: React.FocusEvent) => {
    if (menuRef.current?.contains(e.relatedTarget as Node)) return;
    setTimeout(() => setMenuVisible(false), 150);
  }, []);

  const handleHistorySelect = useCallback(
    (expr: string) => {
      loadExpression(expr);
      setMenuVisible(false);
      onExpressionChange?.(expr);
      requestAnimationFrame(() => inputRef.current?.focus());
    },
    [loadExpression, onExpressionChange],
  );

  const showSuggestions = menuVisible && suggestions.length > 0;
  const showHistory = menuVisible && !expression.trim() && history.length > 0;
  const showPreview = expression.trim().length > 0;
  const hasResults = !!results;
  const hasEmptyResults = !isLoading && lastFilter && !results;
  const showDropdown =
    showSuggestions ||
    showHistory ||
    hasResults ||
    hasEmptyResults ||
    isLoading;

  return (
    <div className="ome-search__advanced-bar">
      <TextInput
        ref={inputRef}
        value={expression}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onClick={handleClick}
        onBlur={handleBlur}
        placeholder="CEL filter — Tab to accept hint, Enter to search"
        aria-label="Advanced CEL search"
      />
      {showDropdown && (
        <Menu ref={menuRef} className="ome-search__autocomplete">
          <MenuContent>
            <MenuList>
              {showSuggestions && (
                <AutocompleteMenu
                  suggestions={suggestions}
                  selectedIndex={selectedIndex}
                  onSelect={handleSelect}
                  onIndexChange={setSelectedIndex}
                />
              )}
              {showHistory && (
                <HistoryPanel
                  entries={history}
                  onSelect={handleHistorySelect}
                  onToggleFavorite={toggleFavorite}
                  onRemove={removeHistory}
                />
              )}
              {(showSuggestions || showHistory) &&
                (hasResults || hasEmptyResults || isLoading) && <Divider />}
              {isLoading && (
                <MenuItem isDisabled>
                  <Spinner size="sm" /> Searching...
                </MenuItem>
              )}
              {hasResults && <MenuGroup label="Results">{results}</MenuGroup>}
              {hasEmptyResults && (
                <MenuItem isDisabled>
                  No resources matched
                  <div className="pf-v6-u-mt-sm">
                    <code className="pf-v6-u-font-size-sm">{lastFilter}</code>
                  </div>
                </MenuItem>
              )}
            </MenuList>
          </MenuContent>
          {showPreview && (
            <MenuFooter>
              <CelPreview expression={expression} validation={validation} />
            </MenuFooter>
          )}
        </Menu>
      )}
    </div>
  );
}
