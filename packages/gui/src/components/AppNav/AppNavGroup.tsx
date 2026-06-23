import { NavExpandable } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useLocation } from "react-router-dom";

import type {
  NavLayoutGroup,
  PluginPage,
} from "../../contexts/AppConfigContext";
import AppNavItem from "./AppNavItem";

interface AppNavGroupProps {
  group: NavLayoutGroup;
  pageMap: Map<string, PluginPage>;
  iconMap: Map<string, ComponentType>;
}

const AppNavGroup = ({ group, pageMap, iconMap }: AppNavGroupProps) => {
  const location = useLocation();

  const childPages = group.children
    .map((c) => pageMap.get(c.pageId))
    .filter(Boolean) as PluginPage[];
  if (childPages.length === 0) return null;

  const groupBasePath = `/${group.groupId}`;
  const isActive = location.pathname.startsWith(groupBasePath + "/");

  return (
    <NavExpandable
      title={group.label}
      groupId={group.groupId}
      isActive={isActive}
      isExpanded={isActive}
    >
      {childPages.map((page) => (
        <AppNavItem key={page.id} page={page} iconMap={iconMap} />
      ))}
    </NavExpandable>
  );
};

export default AppNavGroup;
