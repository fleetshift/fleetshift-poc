import "./FleetSearch.scss";

import { loadPfIcon, PluginLink } from "@fleetshift/common";
import {
  Button,
  Divider,
  Label,
  Menu,
  MenuContent,
  MenuGroup,
  MenuItem,
  MenuList,
  SearchInput,
  ToolbarItem,
  Tooltip,
} from "@patternfly/react-core";
import { Popper } from "@patternfly/react-core/dist/esm/helpers/Popper/Popper";
import { CodeIcon, SearchIcon } from "@patternfly/react-icons";
import clsx from "clsx";
import {
  ComponentType,
  forwardRef,
  Fragment,
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link } from "react-router-dom";

import { useInventorySearch } from "../../hooks/useInventorySearch";
import AdvancedSearchBar from "./advanced/AdvancedSearchBar";
import type { GroupedResults, SearchResultItem } from "./searchIndex";
import { useSearch } from "./SearchProvider";

const CATEGORY_LABELS: Record<string, string> = {
  resources: "Resources",
  nav: "Pages",
  setting: "Settings",
};

const KNOWN_CATEGORIES = ["resources", "nav", "setting"];

function LazyIcon({
  name,
  Fallback,
}: {
  name: string;
  Fallback: ComponentType;
}) {
  const IconC = useMemo(
    () =>
      name.length === 0
        ? Fallback
        : lazy(() =>
            loadPfIcon(name.replace("IconIcon", "Icon")).then((r) =>
              r === null ? { default: Fallback } : { default: r },
            ),
          ),
    [name, Fallback],
  );

  if (name.length === 0) {
    return <Fallback />;
  }

  return (
    <Suspense fallback={<Fallback />}>
      <IconC />
    </Suspense>
  );
}

function ResultIcon({
  name,
  IconComponent,
}: {
  name: string;
  IconComponent?: React.ComponentType;
}) {
  const Fallback = IconComponent ?? SearchIcon;
  return <LazyIcon name={name} Fallback={Fallback} />;
}

function HighlightedText({ html }: { html: string }) {
  return <span dangerouslySetInnerHTML={{ __html: html }} />;
}

function shortPath(path: string): string {
  const segments = path.split(".");
  return segments.length > 2 ? segments.slice(-2).join(".") : path;
}

function truncateValue(val: string, max = 40): string {
  return val.length > max ? val.slice(0, max) + "…" : val;
}

function MatchFields({
  fields,
}: {
  fields: Array<{ path: string; value: string }>;
}) {
  if (fields.length === 0) return null;
  return (
    <div className="ome-search__match-fields">
      {fields.map((f) => (
        <span key={f.path} className="ome-search__match-field">
          {shortPath(f.path)}: <mark>{truncateValue(f.value)}</mark>
        </span>
      ))}
    </div>
  );
}

const linkComponentCache = new Map<
  string,
  React.ForwardRefExoticComponent<React.RefAttributes<HTMLAnchorElement>>
>();
function getLinkComponent(to: string) {
  let cached = linkComponentCache.get(to);
  if (!cached) {
    cached = forwardRef<
      HTMLAnchorElement,
      React.HTMLAttributes<HTMLAnchorElement>
    >((props, ref) => <Link to={to} {...props} ref={ref} />);
    linkComponentCache.set(to, cached);
  }
  return cached;
}

type PluginLinkInfo = {
  scope: string;
  module: string;
  to?: string;
  search?: string;
};
const pluginLinkComponentCache = new Map<
  string,
  React.ForwardRefExoticComponent<React.RefAttributes<HTMLAnchorElement>>
>();
function getPluginLinkComponent(link: PluginLinkInfo) {
  const key = `${link.scope}::${link.module}::${link.to ?? ""}::${link.search ?? ""}`;
  let cached = pluginLinkComponentCache.get(key);
  if (!cached) {
    cached = forwardRef<
      HTMLAnchorElement,
      React.HTMLAttributes<HTMLAnchorElement>
    >((props, ref) => (
      <PluginLink
        scope={link.scope}
        module={link.module}
        to={link.search ? { pathname: link.to, search: link.search } : link.to}
        {...props}
        ref={ref}
      />
    ));
    pluginLinkComponentCache.set(key, cached);
  }
  return cached;
}

function totalCount(results: GroupedResults): number {
  let count = 0;
  for (const items of Object.values(results)) count += items.length;
  return count;
}

function categoryOrder(results: GroupedResults): string[] {
  const seen = new Set(KNOWN_CATEGORIES);
  const order = [...KNOWN_CATEGORIES];
  for (const key of Object.keys(results)) {
    if (!seen.has(key)) order.push(key);
  }
  return order;
}

interface FleetSearchProps {
  onStateChange?: (isOpen: boolean) => void;
}

const EMPTY: GroupedResults = {};

const FleetSearch = ({ onStateChange }: FleetSearchProps) => {
  const { query } = useSearch();
  const { search: inventorySearch, filterSearch } = useInventorySearch();
  const [searchValue, setSearchValue] = useState("");
  const [isOpen, setIsOpen] = useState(false);
  const [results, setResults] = useState<GroupedResults>(EMPTY);
  const [isAdvanced, setIsAdvanced] = useState(false);
  const [lastFilter, setLastFilter] = useState("");
  const [advancedResults, setAdvancedResults] = useState<SearchResultItem[]>(
    [],
  );
  const [isAdvancedLoading, setIsAdvancedLoading] = useState(false);
  const toggleRef = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const blockCloseRef = useRef(false);
  const requestIdRef = useRef(0);
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const advancedDebounceRef = useRef<ReturnType<typeof setTimeout>>();
  const advancedRequestIdRef = useRef(0);

  const total = totalCount(results);
  const isMac = navigator.platform.startsWith("Mac");
  const shortcutHint = isMac ? "⌘⇧F" : "Ctrl+Shift+F";

  const closeMenu = useCallback(() => {
    setIsOpen(false);
    onStateChange?.(false);
  }, [onStateChange]);

  const clearSearch = useCallback(() => {
    ++requestIdRef.current;
    clearTimeout(debounceTimerRef.current);
    setSearchValue("");
    setResults(EMPTY);
    closeMenu();
  }, [closeMenu]);

  const handleChange = useCallback(
    (_e: unknown, value: string) => {
      setSearchValue(value);
      if (!value.trim()) {
        ++requestIdRef.current;
        clearTimeout(debounceTimerRef.current);
        setResults(EMPTY);
        setIsOpen(false);
        onStateChange?.(false);
        return;
      }

      const id = ++requestIdRef.current;

      query(value).then((r) => {
        if (id !== requestIdRef.current) return;
        setResults((prev) => ({ ...prev, ...r }));
        const hasResults = totalCount(r) > 0;
        if (hasResults) {
          setIsOpen(true);
          onStateChange?.(true);
        }
      });

      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = setTimeout(async () => {
        const invResults = await inventorySearch(value);
        if (id !== requestIdRef.current) return;
        setResults((prev) => ({ ...prev, resources: invResults }));
        if (invResults.length > 0) {
          setIsOpen(true);
          onStateChange?.(true);
        }
      }, 300);
    },
    [query, onStateChange, inventorySearch],
  );

  const handleClear = useCallback(() => {
    ++requestIdRef.current;
    clearTimeout(debounceTimerRef.current);
    setSearchValue("");
    setResults(EMPTY);
    setIsOpen(false);
    onStateChange?.(false);
  }, [onStateChange]);

  const handleInputClick = useCallback(() => {
    if (!isOpen && total > 0) {
      setIsOpen(true);
      onStateChange?.(true);
      blockCloseRef.current = true;
    }
  }, [isOpen, total, onStateChange]);

  const handleInputKeyDown = useCallback(
    (ev: React.KeyboardEvent) => {
      if (isOpen && ev.key === "ArrowDown" && menuRef.current) {
        ev.preventDefault();
        const first = menuRef.current.querySelector<HTMLElement>(
          "li > button, li > a",
        );
        first?.focus();
      } else if (isOpen && ev.key === "Escape") {
        closeMenu();
      }
    },
    [isOpen, closeMenu],
  );

  useEffect(() => {
    const handleKeys = (e: KeyboardEvent) => {
      if (!isOpen) return;
      if (
        menuRef.current?.contains(e.target as Node) ||
        toggleRef.current?.contains(e.target as Node)
      ) {
        if (e.key === "Escape" || e.key === "Tab") {
          closeMenu();
          toggleRef.current?.focus();
        }
      }
    };
    const handleClickOutside = (e: MouseEvent) => {
      if (
        !blockCloseRef.current &&
        isOpen &&
        !menuRef.current?.contains(e.target as Node)
      ) {
        closeMenu();
      }
      blockCloseRef.current = false;
    };

    window.addEventListener("keydown", handleKeys);
    window.addEventListener("click", handleClickOutside);
    return () => {
      window.removeEventListener("keydown", handleKeys);
      window.removeEventListener("click", handleClickOutside);
    };
  }, [isOpen, closeMenu]);

  useEffect(() => {
    const recalc = () => {
      if (!menuRef.current) return;
      const { top } = menuRef.current.getBoundingClientRect();
      if (top === 0) return;
      menuRef.current.style.maxHeight = `${window.innerHeight - top - 4}px`;
    };
    window.addEventListener("resize", recalc);
    requestAnimationFrame(recalc);
    return () => window.removeEventListener("resize", recalc);
  }, [isOpen, total]);

  const renderItem = (item: SearchResultItem) => {
    if (item.Component) {
      return (
        <div key={item.id} role="none" onClick={clearSearch}>
          <item.Component title={item.title} description={item.description} />
        </div>
      );
    }

    if (item.pluginLink) {
      const isNavigable = item.navigable !== false;
      return (
        <MenuItem
          key={item.id}
          icon={
            <ResultIcon name={item.icon} IconComponent={item.IconComponent} />
          }
          description={
            <>
              {isNavigable ? (
                (item.descriptionNode ?? item.description)
              ) : (
                <span className="pf-v6-u-text-color-subtle">
                  {item.descriptionNode ?? item.description}
                  <Label isCompact color="grey" className="pf-v6-u-ml-sm">
                    Cluster
                  </Label>
                </span>
              )}
              {item.matchFields && item.matchFields.length > 0 && (
                <MatchFields fields={item.matchFields} />
              )}
            </>
          }
          component={getPluginLinkComponent(item.pluginLink)}
          onClick={clearSearch}
        >
          <HighlightedText html={item.title} />
          {item.status && (
            <span
              className={`ome-search__status ome-search__status--${item.status}`}
            >
              {item.status}
            </span>
          )}
        </MenuItem>
      );
    }

    if (!item.pathname) {
      return (
        <MenuItem
          key={item.id}
          icon={<ResultIcon name={item.icon} />}
          description={
            item.matchFields && item.matchFields.length > 0 ? (
              <>
                {item.description}
                <MatchFields fields={item.matchFields} />
              </>
            ) : (
              item.description
            )
          }
          isDisabled
        >
          <HighlightedText html={item.title} />
        </MenuItem>
      );
    }

    return (
      <MenuItem
        key={item.id}
        icon={
          <ResultIcon name={item.icon} IconComponent={item.IconComponent} />
        }
        description={
          item.description ? (
            <HighlightedText html={item.description} />
          ) : undefined
        }
        component={getLinkComponent(item.pathname)}
        onClick={clearSearch}
      >
        <HighlightedText html={item.title} />
        {item.status && (
          <span
            className={`ome-search__status ome-search__status--${item.status}`}
          >
            {item.status}
          </span>
        )}
      </MenuItem>
    );
  };

  const toggle = (
    <SearchInput
      placeholder={`Search pages, clusters, settings... (${shortcutHint} for CEL filter)`}
      value={searchValue}
      onChange={handleChange}
      onClear={handleClear}
      onClick={handleInputClick}
      onKeyDown={handleInputKeyDown}
      ref={toggleRef}
    />
  );

  const menu = (
    <Menu ref={menuRef} className="ome-search__menu">
      <MenuContent>
        <MenuList>
          {(() => {
            const orderedCategories = categoryOrder(results);
            let renderedCount = 0;

            const toFeatureId = (id: string) => {
              if (id.startsWith("group-")) return `group.${id.slice(6)}`;
              return id.replace(/^(ext|nav)-/, "");
            };

            return orderedCategories.map((cat) => {
              const items = results[cat];
              if (!items || items.length === 0) return null;
              const isFirst = renderedCount === 0;
              renderedCount++;

              // Build child lookup: featureId → children
              const childrenByFeature = new Map<string, SearchResultItem[]>();
              const itemByFeature = new Map<string, SearchResultItem>();
              for (const item of items) {
                itemByFeature.set(toFeatureId(item.id), item);
                if (item.feature) {
                  const list = childrenByFeature.get(item.feature) ?? [];
                  list.push(item);
                  childrenByFeature.set(item.feature, list);
                }
              }

              // Roots: items with no feature, or whose feature parent isn't in this category
              const roots = items.filter(
                (item) => !item.feature || !itemByFeature.has(item.feature),
              );

              const renderTree = (
                item: SearchResultItem,
                isLast: boolean,
                depth: number,
              ): React.ReactNode => {
                const featureId = toFeatureId(item.id);
                const children = childrenByFeature.get(featureId) ?? [];
                const hasChildren = children.length > 0;

                if (depth === 0 && !hasChildren) {
                  return renderItem(item);
                }

                const wrapClass =
                  depth === 0
                    ? "ome-search__tree-group"
                    : `ome-search__tree-child${isLast ? " ome-search__tree-child--last" : ""}`;

                return (
                  <div key={item.id} className={wrapClass}>
                    {renderItem(item)}
                    {hasChildren && (
                      <div className="ome-search__tree-children" role="group">
                        {children.map((child, idx) =>
                          renderTree(
                            child,
                            idx === children.length - 1,
                            depth + 1,
                          ),
                        )}
                      </div>
                    )}
                  </div>
                );
              };

              return (
                <Fragment key={cat}>
                  {!isFirst && <Divider />}
                  <MenuGroup label={CATEGORY_LABELS[cat] ?? cat}>
                    {roots.map((item, idx) =>
                      renderTree(item, idx === roots.length - 1, 0),
                    )}
                  </MenuGroup>
                </Fragment>
              );
            });
          })()}
          {total === 0 && searchValue && !isAdvanced && (
            <MenuItem isDisabled>No results found</MenuItem>
          )}
        </MenuList>
      </MenuContent>
    </Menu>
  );

  const handleAdvancedToggle = useCallback(() => {
    setIsAdvanced((prev) => !prev);
    setLastFilter("");
    setAdvancedResults([]);
    setIsAdvancedLoading(false);
    clearTimeout(advancedDebounceRef.current);
    ++advancedRequestIdRef.current;
    if (isOpen) closeMenu();
  }, [isOpen, closeMenu]);

  useEffect(() => {
    const handleShortcut = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === "f") {
        e.preventDefault();
        handleAdvancedToggle();
      }
    };
    window.addEventListener("keydown", handleShortcut);
    return () => window.removeEventListener("keydown", handleShortcut);
  }, [handleAdvancedToggle]);

  const handleAdvancedDeactivate = useCallback(() => {
    setIsAdvanced(false);
  }, []);

  const handleAdvancedExecute = useCallback(
    async (expr: string) => {
      clearTimeout(advancedDebounceRef.current);
      const id = ++advancedRequestIdRef.current;
      setIsAdvancedLoading(true);
      setLastFilter(expr);
      const items = await filterSearch(expr);
      if (id !== advancedRequestIdRef.current) return;
      setAdvancedResults(items);
      setIsAdvancedLoading(false);
    },
    [filterSearch],
  );

  const handleAdvancedExpressionChange = useCallback(
    (expr: string) => {
      clearTimeout(advancedDebounceRef.current);
      if (!expr.trim()) {
        ++advancedRequestIdRef.current;
        setAdvancedResults([]);
        setIsAdvancedLoading(false);
        setLastFilter("");
        return;
      }
      setIsAdvancedLoading(true);
      advancedDebounceRef.current = setTimeout(async () => {
        const id = ++advancedRequestIdRef.current;
        const items = await filterSearch(expr.trim());
        if (id !== advancedRequestIdRef.current) return;
        setAdvancedResults(items);
        setLastFilter(expr.trim());
        setIsAdvancedLoading(false);
      }, 400);
    },
    [filterSearch],
  );

  const advancedResultNodes =
    advancedResults.length > 0 ? advancedResults.map(renderItem) : undefined;

  return (
    <ToolbarItem style={{ width: "100%" }}>
      <div
        ref={containerRef}
        className={clsx("ome-search", isAdvanced && "ome-search--advanced")}
      >
        {isAdvanced ? (
          <AdvancedSearchBar
            onDeactivate={handleAdvancedDeactivate}
            onExecute={handleAdvancedExecute}
            onExpressionChange={handleAdvancedExpressionChange}
            results={advancedResultNodes}
            lastFilter={lastFilter}
            isLoading={isAdvancedLoading}
          />
        ) : (
          <Popper
            trigger={toggle}
            popper={menu}
            appendTo={containerRef.current || undefined}
            isVisible={isOpen}
          />
        )}
        <Tooltip
          content={isAdvanced ? "Simple search" : "Advanced filter (CEL)"}
        >
          <Button
            variant="plain"
            onClick={handleAdvancedToggle}
            aria-label="Toggle advanced search"
            className="ome-search__advanced-toggle"
          >
            <CodeIcon />
          </Button>
        </Tooltip>
      </div>
    </ToolbarItem>
  );
};

export default FleetSearch;
