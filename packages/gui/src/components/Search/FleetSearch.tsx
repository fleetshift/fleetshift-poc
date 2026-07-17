import "./FleetSearch.scss";

import { loadPfIcon, PluginLink } from "@fleetshift/common";
import {
  Divider,
  Menu,
  MenuContent,
  MenuGroup,
  MenuItem,
  MenuList,
  SearchInput,
} from "@patternfly/react-core";
import { Popper } from "@patternfly/react-core/dist/esm/helpers/Popper/Popper";
import { SearchIcon } from "@patternfly/react-icons";
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
  const { search: inventorySearch } = useInventorySearch();
  const [searchValue, setSearchValue] = useState("");
  const [isOpen, setIsOpen] = useState(false);
  const [results, setResults] = useState<GroupedResults>(EMPTY);
  const toggleRef = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const blockCloseRef = useRef(false);
  const requestIdRef = useRef(0);
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout>>();

  const total = totalCount(results);

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
      return (
        <MenuItem
          key={item.id}
          icon={
            <ResultIcon name={item.icon} IconComponent={item.IconComponent} />
          }
          description={item.descriptionNode ?? item.description}
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
          description={item.description}
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
      placeholder="Search pages, clusters, settings..."
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
          {total === 0 && searchValue && (
            <MenuItem isDisabled>No results found</MenuItem>
          )}
        </MenuList>
      </MenuContent>
    </Menu>
  );

  return (
    <div ref={containerRef} className="ome-search">
      <Popper
        trigger={toggle}
        popper={menu}
        appendTo={containerRef.current || undefined}
        isVisible={isOpen}
      />
    </div>
  );
};

export default FleetSearch;
