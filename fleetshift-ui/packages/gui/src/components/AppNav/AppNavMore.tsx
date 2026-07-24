import { MORE_ENTRY_ID, type NavLayoutMore } from "@fleetshift/common";
import { NavExpandable } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useMemo } from "react";
import { useLocation } from "react-router-dom";

import type { PluginPage } from "../../contexts/AppConfigContext";
import AppNavGroup from "./AppNavGroup";
import AppNavItem from "./AppNavItem";

interface AppNavMoreProps {
  more: NavLayoutMore;
  pageMap: Map<string, PluginPage>;
  iconMap: Map<string, ComponentType>;
}

/** Collapsed "More" nav section for hidden items. Auto-expands on active route match. */
const AppNavMore = ({ more, pageMap, iconMap }: AppNavMoreProps) => {
  const location = useLocation();

  const hasActiveChild = useMemo(() => {
    const check = (children: NavLayoutMore["children"]): boolean =>
      children.some((entry) => {
        if (entry.type === "page") {
          const page = pageMap.get(entry.pageId);
          if (!page) return false;
          const fullPath = `/${page.path}`;
          return (
            location.pathname === fullPath ||
            location.pathname.startsWith(fullPath + "/")
          );
        }
        if (entry.type === "group") {
          return entry.children.some((c) => {
            const page = pageMap.get(c.pageId);
            if (!page) return false;
            const fullPath = `/${page.path}`;
            return (
              location.pathname === fullPath ||
              location.pathname.startsWith(fullPath + "/")
            );
          });
        }
        return false;
      });
    return check(more.children);
  }, [more.children, pageMap, location.pathname]);

  const visibleChildren = useMemo(
    () =>
      more.children.filter((entry) => {
        if (entry.type === "page") return pageMap.has(entry.pageId);
        if (entry.type === "group") {
          return entry.children.some((c) => pageMap.has(c.pageId));
        }
        return false;
      }),
    [more.children, pageMap],
  );

  if (visibleChildren.length === 0) return null;

  return (
    <NavExpandable
      title="More"
      groupId={MORE_ENTRY_ID}
      isActive={hasActiveChild}
      isExpanded={hasActiveChild}
    >
      {visibleChildren.map((entry) => {
        if (entry.type === "page") {
          const page = pageMap.get(entry.pageId);
          if (!page) return null;
          return (
            <AppNavItem
              key={page.id}
              page={page}
              iconMap={iconMap}
              iconOverride={entry.iconOverride}
            />
          );
        }
        if (entry.type === "group") {
          return (
            <AppNavGroup
              key={entry.groupId}
              group={entry}
              pageMap={pageMap}
              iconMap={iconMap}
            />
          );
        }
        return null;
      })}
    </NavExpandable>
  );
};

export default AppNavMore;
