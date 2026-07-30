import type { NavLayoutEntry, NavLayoutMore } from "@fleetshift/common";
import {
  CORE_EXTENSION_META,
  mergeLayout,
  orderByIds,
  useNavLayout,
} from "@fleetshift/common";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import { Divider, Nav, NavList } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useMemo } from "react";

import type { PluginPage } from "../../contexts/AppConfigContext";
import { useAppConfig } from "../../contexts/AppConfigContext";
import { isModuleExtension } from "../../extensions/isModuleExtension";
import AppNavGroup from "./AppNavGroup";
import AppNavItem from "./AppNavItem";
import AppNavMore from "./AppNavMore";

const isBottom = (scope: string) =>
  CORE_EXTENSION_META[scope]?.navSection === "bottom";

type TaggedEntry = NavLayoutEntry & { id: string; label: string };

function splitBySection(
  entries: TaggedEntry[],
  pageMap: Map<string, PluginPage>,
): { main: TaggedEntry[]; bottom: TaggedEntry[]; more: NavLayoutMore | null } {
  const main: TaggedEntry[] = [];
  const bottom: TaggedEntry[] = [];
  let more: NavLayoutMore | null = null;
  for (const entry of entries) {
    if (entry.type === "page") {
      const page = pageMap.get(entry.pageId);
      if (!page) continue;
      (isBottom(page.scope) ? bottom : main).push(entry);
    } else if (entry.type === "group") {
      const scope = `${entry.pluginKey}-plugin`;
      (isBottom(scope) ? bottom : main).push(entry);
    } else if (entry.type === "section") {
      // Sections default to main nav area
      main.push(entry);
    } else if (entry.type === "more") {
      more = entry as unknown as NavLayoutMore;
    }
  }
  return { main, bottom, more };
}

function tagEntries(
  layout: NavLayoutEntry[],
  pageMap: Map<string, PluginPage>,
): TaggedEntry[] {
  const result: TaggedEntry[] = [];
  for (const entry of layout) {
    if (entry.type === "page") {
      const page = pageMap.get(entry.pageId);
      if (!page) continue;
      result.push({ ...entry, id: entry.pageId, label: page.title });
    } else if (entry.type === "group") {
      result.push({ ...entry, id: entry.groupId, label: entry.label });
    } else if (entry.type === "section") {
      result.push({ ...entry, id: entry.id, label: entry.label });
    } else if (entry.type === "more") {
      result.push({ ...entry, id: "_more", label: "More" } as TaggedEntry);
    }
  }
  return result;
}

const AppNav = () => {
  const { pluginPages, navLayout } = useAppConfig();
  const { override, legacyOrder, loaded } = useNavLayout();
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

  const { mainEntries, bottomEntries, moreEntry } = useMemo(() => {
    if (override) {
      // New path: merge backend layout with user override
      const merged = mergeLayout(navLayout, override);
      const tagged = tagEntries(merged, pageMap);
      const { main, bottom, more } = splitBySection(tagged, pageMap);
      return { mainEntries: main, bottomEntries: bottom, moreEntry: more };
    }

    // Legacy path: flat string[] ordering via orderByIds
    const tagged = tagEntries(navLayout, pageMap);
    const { main, bottom } = splitBySection(tagged, pageMap);
    return {
      mainEntries: orderByIds(main, legacyOrder, "label"),
      bottomEntries: orderByIds(bottom, legacyOrder, "label"),
      moreEntry: null,
    };
  }, [navLayout, pageMap, override, legacyOrder]);

  // Defer rendering until IndexedDB layout is loaded to prevent
  // a flash of default nav order before the user's saved order applies.
  if (!loaded) return null;

  const renderEntry = (entry: NavLayoutEntry & { id: string }) => {
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
  };

  return (
    <Nav>
      <NavList>
        {mainEntries.map(renderEntry)}
        {moreEntry && (
          <AppNavMore more={moreEntry} pageMap={pageMap} iconMap={iconMap} />
        )}
      </NavList>
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
