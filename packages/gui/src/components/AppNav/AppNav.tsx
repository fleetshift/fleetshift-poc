import {
  CORE_EXTENSION_META,
  orderByIds,
  useNavOrder,
} from "@fleetshift/common";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import { Divider, Nav, NavList } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useMemo } from "react";

import type {
  NavLayoutEntry,
  PluginPage,
} from "../../contexts/AppConfigContext";
import { useAppConfig } from "../../contexts/AppConfigContext";
import { isModuleExtension } from "../../extensions/isModuleExtension";
import AppNavGroup from "./AppNavGroup";
import AppNavItem from "./AppNavItem";

const isBottom = (scope: string) =>
  CORE_EXTENSION_META[scope]?.navSection === "bottom";

const AppNav = () => {
  const { pluginPages, navLayout } = useAppConfig();
  const { order: savedOrder } = useNavOrder();
  const [moduleExtensions] = useResolvedExtensions(isModuleExtension);

  const iconMap = useMemo(() => {
    const map = new Map<string, ComponentType>();
    for (const ext of moduleExtensions) {
      map.set(ext.properties.label, ext.properties.icon);
    }
    return map;
  }, [moduleExtensions]);

  const pageMap = useMemo(() => {
    const map = new Map<string, PluginPage>();
    for (const page of pluginPages) {
      map.set(page.id, page);
    }
    return map;
  }, [pluginPages]);

  const { mainEntries, bottomEntries } = useMemo(() => {
    const main: (NavLayoutEntry & { id: string; label: string })[] = [];
    const bottom: (NavLayoutEntry & { id: string; label: string })[] = [];
    for (const entry of navLayout) {
      if (entry.type === "page") {
        const page = pageMap.get(entry.pageId);
        if (!page) continue;
        const tagged = { ...entry, id: entry.pageId, label: page.title };
        (isBottom(page.scope) ? bottom : main).push(tagged);
      } else if (entry.type === "group") {
        const scope = `${entry.pluginKey}-plugin`;
        const tagged = { ...entry, id: entry.groupId, label: entry.label };
        (isBottom(scope) ? bottom : main).push(tagged);
      }
    }
    return {
      mainEntries: orderByIds(main, savedOrder, "label"),
      bottomEntries: orderByIds(bottom, savedOrder, "label"),
    };
  }, [navLayout, pageMap, savedOrder]);

  const renderEntry = (entry: NavLayoutEntry & { id: string }) => {
    if (entry.type === "page") {
      const page = pageMap.get(entry.pageId);
      if (!page) return null;
      return <AppNavItem key={page.id} page={page} iconMap={iconMap} />;
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
  };

  return (
    <Nav>
      <NavList>{mainEntries.map(renderEntry)}</NavList>
      {bottomEntries.length > 0 && (
        <>
          <Divider />
          <NavList>{bottomEntries.map(renderEntry)}</NavList>
        </>
      )}
    </Nav>
  );
};

export default AppNav;
