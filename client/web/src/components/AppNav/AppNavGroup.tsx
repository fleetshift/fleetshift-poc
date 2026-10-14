import {
  DynamicPfIcon,
  iconSlugToName,
  isCustomGroup,
  type NavLayoutGroup,
} from "@fleetshift/common";
import { Icon, NavExpandable } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useLocation } from "react-router-dom";

import type { PluginPage } from "../../contexts/AppConfigContext";
import AppNavItem from "./AppNavItem";

interface AppNavGroupProps {
  group: NavLayoutGroup;
  pageMap: Map<string, PluginPage>;
  iconMap: Map<string, ComponentType>;
}

const AppNavGroup = ({ group, pageMap, iconMap }: AppNavGroupProps) => {
  const location = useLocation();

  const customIconName =
    isCustomGroup(group) && group.icon ? iconSlugToName(group.icon) : null;

  const childPages = group.children
    .map((c) => ({ page: pageMap.get(c.pageId), iconOverride: c.iconOverride }))
    .filter(
      (item): item is { page: PluginPage; iconOverride: string | undefined } =>
        item.page !== undefined,
    );
  if (childPages.length === 0) return null;

  const groupBasePath = `/${group.groupId}`;
  const isActive = location.pathname.startsWith(groupBasePath + "/");

  const title = customIconName ? (
    <>
      <Icon isInline className="pf-v6-u-mr-sm">
        <DynamicPfIcon name={customIconName} />
      </Icon>
      {group.label}
    </>
  ) : (
    group.label
  );

  return (
    <NavExpandable
      title={title}
      groupId={group.groupId}
      isActive={isActive}
      isExpanded={isActive}
    >
      {childPages.map(({ page, iconOverride }) => (
        <AppNavItem
          key={page.id}
          page={page}
          iconMap={iconMap}
          iconOverride={iconOverride}
        />
      ))}
    </NavExpandable>
  );
};

export default AppNavGroup;
