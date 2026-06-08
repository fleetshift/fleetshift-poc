import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
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
import {
  CogIcon,
  CubesIcon,
  SearchIcon,
  ServerIcon,
} from "@patternfly/react-icons";
import { useSearch } from "./SearchProvider";
import type { GroupedResults, SearchResultItem } from "./searchIndex";
import "./FleetSearch.scss";

const CATEGORY_LABELS: Record<string, string> = {
  nav: "Pages",
  cluster: "Clusters",
  setting: "Settings",
};

const KNOWN_CATEGORIES = ["nav", "cluster", "setting"];

const ICON_MAP: Record<string, React.ComponentType> = {
  CubesIcon,
  ServerIcon,
  CogIcon,
};

function ResultIcon({
  name,
  IconComponent,
}: {
  name: string;
  IconComponent?: React.ComponentType;
}) {
  if (IconComponent) return <IconComponent />;
  const Icon = ICON_MAP[name] ?? SearchIcon;
  return <Icon />;
}

function HighlightedText({ html }: { html: string }) {
  return <span dangerouslySetInnerHTML={{ __html: html }} />;
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
  const [searchValue, setSearchValue] = useState("");
  const [isOpen, setIsOpen] = useState(false);
  const [results, setResults] = useState<GroupedResults>(EMPTY);
  const toggleRef = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const blockCloseRef = useRef(false);

  const total = totalCount(results);

  const closeMenu = useCallback(() => {
    setIsOpen(false);
    onStateChange?.(false);
  }, [onStateChange]);

  const clearSearch = useCallback(() => {
    setSearchValue("");
    setResults(EMPTY);
    closeMenu();
  }, [closeMenu]);

  const handleChange = useCallback(
    async (_e: unknown, value: string) => {
      setSearchValue(value);
      if (!value.trim()) {
        setResults(EMPTY);
        setIsOpen(false);
        onStateChange?.(false);
        return;
      }
      const r = await query(value);
      setResults(r);
      const hasResults = totalCount(r) > 0;
      setIsOpen(hasResults);
      onStateChange?.(hasResults);
    },
    [query, onStateChange],
  );

  const handleClear = useCallback(() => {
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
        component={(props: React.HTMLAttributes<HTMLAnchorElement>) => (
          <Link to={item.pathname} {...props} />
        )}
        onClick={clearSearch}
      >
        <HighlightedText html={item.title} />
        {item.status && (
          <span
            className={`fs-search__status fs-search__status--${item.status}`}
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
    <Menu ref={menuRef} className="fs-search__menu">
      <MenuContent>
        <MenuList>
          {categoryOrder(results).map((cat, idx) => {
            const items = results[cat];
            if (!items || items.length === 0) return null;

            const parents: SearchResultItem[] = [];
            const childrenByFeature = new Map<string, SearchResultItem[]>();
            const standalone: SearchResultItem[] = [];

            const toFeatureId = (id: string) => id.replace(/^(ext|nav)-/, "");

            for (const item of items) {
              if (item.feature) {
                const list = childrenByFeature.get(item.feature) ?? [];
                list.push(item);
                childrenByFeature.set(item.feature, list);
              } else {
                const featureId = toFeatureId(item.id);
                if (
                  childrenByFeature.has(featureId) ||
                  items.some((i) => i.feature === featureId)
                ) {
                  parents.push(item);
                } else {
                  standalone.push(item);
                }
              }
            }

            const orphanChildren: SearchResultItem[] = [];
            for (const [featureId, children] of childrenByFeature) {
              if (!parents.some((p) => toFeatureId(p.id) === featureId)) {
                orphanChildren.push(...children);
              }
            }
            const isLast = categoryOrder.length - 1 === idx;
            return (
              <>
                {!isLast ? <Divider /> : null}
                <MenuGroup key={cat} label={CATEGORY_LABELS[cat] ?? cat}>
                  {parents.map((parent) => {
                    const featureId = toFeatureId(parent.id);
                    const children = childrenByFeature.get(featureId) ?? [];
                    return (
                      <div key={parent.id} className="fs-search__tree-group">
                        {renderItem(parent)}
                        {children.length > 0 && (
                          <div
                            className="fs-search__tree-children"
                            role="group"
                          >
                            {children.map((child, idx) => (
                              <div
                                key={child.id}
                                className={`fs-search__tree-child${idx === children.length - 1 ? " fs-search__tree-child--last" : ""}`}
                              >
                                {renderItem(child)}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {standalone.map((item) => renderItem(item))}
                  {orphanChildren.map((item) => renderItem(item))}
                </MenuGroup>
              </>
            );
          })}
          {total === 0 && searchValue && (
            <MenuItem isDisabled>No results found</MenuItem>
          )}
        </MenuList>
      </MenuContent>
    </Menu>
  );

  return (
    <div ref={containerRef} className="fs-search">
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
