import "./advanced-search.scss";

import {
  Divider,
  Menu,
  MenuContent,
  MenuGroup,
  MenuItem,
  MenuList,
  Spinner,
  TextInput,
} from "@patternfly/react-core";
import {
  Children,
  cloneElement,
  isValidElement,
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import AutocompleteMenu from "./AutocompleteMenu";
import CelPreview from "./CelPreview";
import HistoryPanel from "./HistoryPanel";
import { tokenize } from "./tokenizer";
import type { Suggestion } from "./types";
import { useAdvancedSearch } from "./useAdvancedSearch";
import { useTypingPlaceholder } from "./useTypingPlaceholder";

const TOKEN_CLASSES: Record<string, string> = {
  field: "ome-search__cel-token--field",
  operator: "ome-search__cel-token--operator",
  value: "ome-search__cel-token--value",
  combinator: "ome-search__cel-token--combinator",
  "dot-call": "ome-search__cel-token--operator",
  macro: "ome-search__cel-token--macro",
  paren: "ome-search__cel-token--paren",
};

const PLACEHOLDER_HINTS = [
  "clusters",
  "running pods",
  'resource.observation.kind == "Pod"',
  "kube-system namespace",
  'resource.conditions.Ready.status == "False"',
  "degraded resources",
  'resource.observation.metadata.namespace == "kube-system"',
  "RBAC roles",
  'resource.observation.kind in ["DaemonSet", "Deployment"]',
  "workload deployments",
  'resource.conditions.Available.status == "True"',
  "available services",
  "has(resource.localLabels)",
  '"Ready" in resource.conditions',
  'name.startsWith("projects/fleet")',
];

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
  const placeholder = useTypingPlaceholder(PLACEHOLDER_HINTS, "Try: ");
  const [inputScrollLeft, setInputScrollLeft] = useState(0);
  const inputTokens = useMemo(() => {
    const tokens = tokenize(expression);
    while (tokens.length > 0 && tokens[tokens.length - 1].type === "whitespace")
      tokens.pop();
    return tokens;
  }, [expression]);
  const inputRef = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const pendingCursorRef = useRef<number | null>(null);

  const mirrorRef = useCallback((node: HTMLDivElement | null) => {
    if (!node || !inputRef.current) return;
    const cs = getComputedStyle(inputRef.current);
    node.style.font = cs.font;
    node.style.letterSpacing = cs.letterSpacing;
    node.style.paddingLeft = cs.paddingLeft;
    node.style.paddingRight = cs.paddingRight;
  }, []);

  const handleInputScroll = useCallback(() => {
    setInputScrollLeft(inputRef.current?.scrollLeft ?? 0);
  }, []);

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
      const { pos, expr } = acceptSuggestion(suggestion);
      pendingCursorRef.current = pos;
      setMenuVisible(true);
      setSelectedIndex(0);
      onExpressionChange?.(expr);
    },
    [acceptSuggestion, onExpressionChange],
  );

  const handleHistorySelect = useCallback(
    (expr: string) => {
      loadExpression(expr);
      setMenuVisible(true);
      onExpressionChange?.(expr);
      requestAnimationFrame(() => inputRef.current?.focus());
    },
    [loadExpression, onExpressionChange],
  );

  const showSuggestions = suggestions.length > 0;
  const showHistory = !expression.trim() && history.length > 0;

  const flatHistory = useMemo(() => {
    if (!showHistory) return [];
    const favorites = history.filter((e) => e.favorite);
    const recent = history.filter((e) => !e.favorite).slice(0, 10);
    return [...favorites, ...recent];
  }, [history, showHistory]);

  const resultItems = useMemo(() => Children.toArray(results), [results]);
  const resultStartIndex = suggestions.length + flatHistory.length;
  const navigableCount = resultStartIndex + resultItems.length;
  const hasOperatorChips = suggestions.some((s) => s.type === "operator");

  const handleKeyDown = useCallback(
    (ev: React.KeyboardEvent) => {
      if (ev.key === "Enter") {
        ev.preventDefault();
        if (
          menuVisible &&
          selectedIndex >= resultStartIndex &&
          resultItems.length > 0
        ) {
          const el = menuRef.current?.querySelector<HTMLElement>(
            "li.pf-m-focus a, li.pf-m-focus button",
          );
          if (el) el.click();
        } else if (
          menuVisible &&
          selectedIndex >= suggestions.length &&
          flatHistory.length > 0
        ) {
          const historyIdx = selectedIndex - suggestions.length;
          const expr = flatHistory[historyIdx].expression;
          loadExpression(expr);
          onExpressionChange?.(expr);
          onExecute(expr);
        } else {
          const result = execute();
          if (result) onExecute(result);
        }
        return;
      }

      if (ev.key === "Tab" && menuVisible && navigableCount > 0) {
        ev.preventDefault();
        if (selectedIndex >= resultStartIndex && resultItems.length > 0) {
          const el = menuRef.current?.querySelector<HTMLElement>(
            "li.pf-m-focus a, li.pf-m-focus button",
          );
          if (el) el.click();
        } else if (selectedIndex < suggestions.length) {
          handleSelect(suggestions[selectedIndex]);
        } else {
          const historyIdx = selectedIndex - suggestions.length;
          handleHistorySelect(flatHistory[historyIdx].expression);
        }
        return;
      }

      if (ev.key === "ArrowDown" && menuVisible && navigableCount > 0) {
        ev.preventDefault();
        setSelectedIndex((prev) => (prev < navigableCount - 1 ? prev + 1 : 0));
        return;
      }

      if (ev.key === "ArrowUp" && menuVisible && navigableCount > 0) {
        ev.preventDefault();
        setSelectedIndex((prev) => (prev > 0 ? prev - 1 : navigableCount - 1));
        return;
      }

      if (
        hasOperatorChips &&
        ev.key === "ArrowRight" &&
        menuVisible &&
        navigableCount > 0
      ) {
        ev.preventDefault();
        setSelectedIndex((prev) => (prev < navigableCount - 1 ? prev + 1 : 0));
        return;
      }

      if (
        hasOperatorChips &&
        ev.key === "ArrowLeft" &&
        menuVisible &&
        navigableCount > 0
      ) {
        ev.preventDefault();
        setSelectedIndex((prev) => (prev > 0 ? prev - 1 : navigableCount - 1));
        return;
      }
    },
    [
      menuVisible,
      suggestions,
      selectedIndex,
      handleSelect,
      handleHistorySelect,
      flatHistory,
      navigableCount,
      hasOperatorChips,
      execute,
      onExecute,
      loadExpression,
      onExpressionChange,
      resultStartIndex,
      resultItems,
    ],
  );

  useEffect(() => {
    inputRef.current?.focus();
    setMenuVisible(true);

    const el = inputRef.current;
    const onScroll = () => handleInputScroll();
    el?.addEventListener("scroll", onScroll);

    const handleClickOutside = (e: MouseEvent) => {
      if (!wrapperRef.current?.contains(e.target as Node)) {
        setMenuVisible(false);
      }
    };

    const handleGlobalEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        setMenuVisible((prev) => {
          if (prev) return false;
          onDeactivate();
          return false;
        });
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    document.addEventListener("keydown", handleGlobalEsc);
    return () => {
      el?.removeEventListener("scroll", onScroll);
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleGlobalEsc);
    };
  }, [onDeactivate, handleInputScroll]);

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

  useEffect(() => {
    const container = menuRef.current?.querySelector(".pf-v6-c-menu__content");
    const focused = container?.querySelector(".pf-m-focus");
    if (focused) {
      focused.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex]);

  useEffect(() => {
    const recalc = () => {
      if (!menuRef.current) return;
      const { top } = menuRef.current.getBoundingClientRect();
      if (top === 0) return;
      menuRef.current.style.maxHeight = `${window.innerHeight - top - 4}px`;
    };
    window.addEventListener("resize", recalc);
    recalc();
    return () => window.removeEventListener("resize", recalc);
  }, [expression, suggestions]);

  const handleClick = useCallback(() => {
    const pos = inputRef.current?.selectionStart ?? expression.length;
    setCursorPos(pos);
    setMenuVisible(true);
  }, [expression.length, setCursorPos]);

  const handleResultClick = useCallback(() => {
    setMenuVisible(false);
  }, []);

  const showPreview = expression.trim().length > 0;
  const hasResults = !!results;
  const hasEmptyResults = !isLoading && lastFilter && !results;
  const showDropdown =
    menuVisible &&
    (showSuggestions ||
      showHistory ||
      showPreview ||
      hasResults ||
      hasEmptyResults ||
      isLoading);

  return (
    <div ref={wrapperRef} className="ome-search__advanced-bar">
      <div className="ome-search__input-wrapper">
        <TextInput
          ref={inputRef}
          value={expression}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onClick={handleClick}
          placeholder={placeholder}
          aria-label="Advanced CEL search"
          className="ome-search__input--colored"
        />
        {expression && (
          <div className="ome-search__input-mirror" aria-hidden="true">
            {/* eslint-disable no-restricted-syntax -- dynamic scroll sync + font sync with input */}
            <div
              ref={mirrorRef}
              className="ome-search__input-mirror-scroll"
              style={{ transform: `translateX(-${inputScrollLeft}px)` }}
            >
              {/* eslint-enable no-restricted-syntax */}
              {inputTokens.map((token, i) => (
                <span key={i} className={TOKEN_CLASSES[token.type] ?? ""}>
                  {token.value}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
      {showDropdown && (
        <Menu ref={menuRef} className="ome-search__autocomplete">
          {showPreview && (
            <div className="ome-search__cel-preview-header">
              <CelPreview expression={expression} validation={validation} />
            </div>
          )}
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
                  focusedIndex={selectedIndex}
                  indexOffset={suggestions.length}
                />
              )}
              {(showSuggestions || showHistory) &&
                (hasResults || hasEmptyResults || isLoading) && <Divider />}
              {isLoading && (
                <MenuItem isDisabled>
                  <Spinner size="sm" /> Searching...
                </MenuItem>
              )}
              {hasResults && (
                <MenuGroup label="Results" onClick={handleResultClick}>
                  {resultItems.map((child, i) => {
                    const focused = selectedIndex === resultStartIndex + i;
                    if (
                      isValidElement(child) &&
                      typeof child.type !== "string"
                    ) {
                      return cloneElement(
                        child as React.ReactElement<Record<string, unknown>>,
                        { isFocused: focused },
                      );
                    }
                    return child;
                  })}
                </MenuGroup>
              )}
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
          {(showSuggestions || showHistory) && (
            <div className="ome-search__keyboard-hints">
              <span>
                <kbd>&uarr;</kbd>
                <kbd>&darr;</kbd> navigate
              </span>
              {hasOperatorChips && (
                <span>
                  <kbd>&larr;</kbd>
                  <kbd>&rarr;</kbd> operators
                </span>
              )}
              <span>
                <kbd>Tab</kbd> accept
              </span>
              <span>
                <kbd>Enter</kbd> search
              </span>
              <span>
                <kbd>Esc</kbd> close
              </span>
            </div>
          )}
        </Menu>
      )}
    </div>
  );
}
