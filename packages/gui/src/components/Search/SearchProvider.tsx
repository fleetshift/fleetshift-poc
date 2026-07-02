import {
  getCachedPfIcon,
  iconSlugToName,
  isCustomGroup,
  loadPfIcon,
  mergeLayout,
  type NavLayoutEntry as CommonNavLayoutEntry,
  type NavLayoutGroup as CommonNavLayoutGroup,
  useExtensionInstall,
  useNavLayout,
} from "@fleetshift/common";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useRef,
} from "react";

import { useAppConfig } from "../../contexts/AppConfigContext";
import { isClusterProviderExtension } from "../../extensions/isClusterProviderExtension";
import { isModuleExtension } from "../../extensions/isModuleExtension";
import type { SearchResultProps } from "../../extensions/searchTypes";
import {
  createSearchDB,
  type GroupedResults,
  insertEntry,
  queryIndex,
  type SearchDB,
  type SearchEntry,
  type SearchResultItem,
} from "./searchIndex";

interface SearchContextValue {
  query: (term: string) => Promise<GroupedResults>;
}

const SearchContext = createContext<SearchContextValue | null>(null);

const MOCK_CLUSTERS: Omit<SearchEntry, "category" | "icon">[] = [
  {
    id: "cl-1",
    title: "prod-east-1",
    description: "Production cluster in US East",
    pathname: "/clusters/prod-east-1",
    status: "healthy",
    meta: "4.20.5 Production aws us-east-1",
  },
  {
    id: "cl-2",
    title: "prod-west-2",
    description: "Production cluster in US West",
    pathname: "/clusters/prod-west-2",
    status: "degraded",
    meta: "4.17.12 Production aws us-west-2",
  },
  {
    id: "cl-3",
    title: "prod-eu-1",
    description: "Production cluster in EU Frankfurt",
    pathname: "/clusters/prod-eu-1",
    status: "healthy",
    meta: "4.20.5 Production on-prem eu-central",
  },
  {
    id: "cl-4",
    title: "dev-central-1",
    description: "Development cluster",
    pathname: "/clusters/dev-central-1",
    status: "healthy",
    meta: "4.20.5 Development on-prem us-central",
  },
  {
    id: "cl-5",
    title: "dev-sandbox",
    description: "Development sandbox cluster",
    pathname: "/clusters/dev-sandbox",
    status: "healthy",
    meta: "4.14.38 Development on-prem us-east-1",
  },
  {
    id: "cl-6",
    title: "edge-retail-nyc",
    description: "Edge cluster at NYC retail location",
    pathname: "/clusters/edge-retail-nyc",
    status: "healthy",
    meta: "4.17.12 Edge on-prem nyc",
  },
  {
    id: "cl-7",
    title: "edge-retail-chi",
    description: "Edge cluster at Chicago retail",
    pathname: "/clusters/edge-retail-chi",
    status: "critical",
    meta: "4.14.38 Edge on-prem chicago",
  },
  {
    id: "cl-8",
    title: "infra-hub",
    description: "Infrastructure hub cluster",
    pathname: "/clusters/infra-hub",
    status: "healthy",
    meta: "4.20.5 Infrastructure on-prem us-central",
  },
];

const SETTINGS: Omit<SearchEntry, "category" | "icon">[] = [
  {
    id: "set-debug",
    title: "Debug console",
    description: "Open the debug page with plugin and config info",
    pathname: "/debug",
    status: "",
    meta: "developer debug",
  },
];

/** Walk all layout entries, recursing into "more" children. */
function forEachEntry(
  entries: CommonNavLayoutEntry[],
  cb: (entry: CommonNavLayoutEntry) => void,
): void {
  for (const entry of entries) {
    if (entry.type === "more") {
      forEachEntry(entry.children, cb);
    } else {
      cb(entry);
    }
  }
}

export function SearchProvider({ children }: { children: ReactNode }) {
  const { pluginPages, navLayout } = useAppConfig();
  const { override } = useNavLayout();
  const {
    loaded: installLoaded,
    isInstalled,
    state: installState,
  } = useExtensionInstall();
  const [moduleExtensions, modulesLoaded] =
    useResolvedExtensions(isModuleExtension);
  const [cpExtensions, cpLoaded] = useResolvedExtensions(
    isClusterProviderExtension,
  );
  const dbRef = useRef<SearchDB | null>(null);
  const componentMapRef = useRef(
    new Map<string, React.ComponentType<SearchResultProps>>(),
  );
  const iconMapRef = useRef(new Map<string, React.ComponentType>());
  const featureParentRef = useRef(new Map<string, SearchResultItem>());
  const installVersion = useRef(0);
  const prevInstallState = useRef(installState);

  if (prevInstallState.current !== installState) {
    prevInstallState.current = installState;
    installVersion.current += 1;
  }

  const currentInstallVersion = installVersion.current;

  useEffect(() => {
    if (!modulesLoaded || !cpLoaded || !installLoaded) return;
    let cancelled = false;

    // Merge backend layout with user override so search reflects custom groups,
    // moved modules, and hidden ("more") items.
    const mergedLayout = override
      ? mergeLayout(navLayout, override)
      : navLayout;

    const db = createSearchDB();
    dbRef.current = db;
    componentMapRef.current.clear();
    iconMapRef.current.clear();
    featureParentRef.current.clear();

    const inserts: ReturnType<typeof insertEntry>[] = [];

    // Maps extension type → global feature ID for parent-child linking
    const typeToFeatureId = new Map<string, string>();

    // Build a set of page IDs that have a richer module extension
    const modulePageIds = new Set<string>();
    for (const ext of moduleExtensions) {
      const page = pluginPages.find((p) => p.title === ext.properties.label);
      if (page) modulePageIds.add(page.id);
    }

    // Build page lookup for group path resolution
    const pageMap = new Map<string, (typeof pluginPages)[number]>();
    for (const page of pluginPages) {
      pageMap.set(page.id, page);
    }

    // Build group lookup from MERGED layout (including "more" children)
    const pageToGroup = new Map<string, CommonNavLayoutGroup>();
    const groupsById = new Map<string, CommonNavLayoutGroup>();
    forEachEntry(mergedLayout, (entry) => {
      if (entry.type === "group") {
        groupsById.set(entry.groupId, entry);
        for (const child of entry.children) {
          pageToGroup.set(child.pageId, entry);
        }
      }
    });

    // Build lookup for page-level icon overrides from merged layout
    const pageIconOverride = new Map<string, string>();
    forEachEntry(mergedLayout, (entry) => {
      if (entry.type === "page" && entry.iconOverride) {
        pageIconOverride.set(entry.pageId, entry.iconOverride);
      } else if (entry.type === "group") {
        for (const child of entry.children) {
          if (child.iconOverride) {
            pageIconOverride.set(child.pageId, child.iconOverride);
          }
        }
      }
    });

    // Pages inside groups are indexed via module extensions below — skip them here
    const groupedPageIds = new Set(pageToGroup.keys());

    // Nav pages — skip any that have a module extension or belong to a group
    // Also skip detail pages whose path contains route params (e.g. ":id")
    for (const page of pluginPages) {
      if (modulePageIds.has(page.id)) continue;
      if (groupedPageIds.has(page.id)) continue;
      if (page.path.includes(":")) continue;

      const navId = `nav-${page.id}`;
      const pathname = `/${page.path}`;
      const status = isInstalled(page.scope) ? "" : "not-enabled";
      const pageIcon = pageIconOverride.get(page.id);
      const iconName = pageIcon ? iconSlugToName(pageIcon) : "CubesIcon";
      inserts.push(
        insertEntry(db, {
          id: navId,
          title: page.title,
          description: `Navigate to ${page.title}`,
          category: "nav",
          pathname,
          icon: iconName,
          status,
          meta: page.path,
        }),
      );

      // Load overridden icon component for search result rendering
      if (pageIcon) {
        const cached = getCachedPfIcon(iconName);
        if (cached) {
          iconMapRef.current.set(navId, cached);
        } else {
          loadPfIcon(iconName).then((comp) => {
            if (!cancelled && comp) iconMapRef.current.set(navId, comp);
          });
        }
      }

      featureParentRef.current.set(page.id, {
        id: navId,
        title: page.title,
        description: `Navigate to ${page.title}`,
        category: "nav",
        pathname,
        icon: iconName,
        status,
      });
    }

    for (const cluster of MOCK_CLUSTERS) {
      inserts.push(
        insertEntry(db, {
          ...cluster,
          category: "cluster",
          icon: "ServerIcon",
        }),
      );
    }

    for (const setting of SETTINGS) {
      inserts.push(
        insertEntry(db, { ...setting, category: "setting", icon: "CogIcon" }),
      );
    }

    // Module extensions: feature entries with description, keywords, extensionPoints
    for (const ext of moduleExtensions) {
      const {
        id,
        label,
        description,
        keywords,
        extensionPoints,
        searchResult,
        icon,
      } = ext.properties;

      const page = pluginPages.find((p) => p.title === label);
      if (!page) continue;

      const globalFeatureId = `${page.pluginKey}.${id}`;
      const basePath = `/${page.path}`;
      const entryId = `ext-${globalFeatureId}`;
      const moduleStatus = isInstalled(page.scope) ? "" : "not-enabled";

      const group = pageToGroup.get(page.id);
      const groupFeature = group ? `group.${group.groupId}` : undefined;

      // Resolve icon: page iconOverride > extension icon > group icon > fallback
      const extPageIcon = pageIconOverride.get(page.id);
      let resolvedIconName = "";
      if (extPageIcon) {
        resolvedIconName = iconSlugToName(extPageIcon);
      } else if (!icon && group && isCustomGroup(group) && group.icon) {
        resolvedIconName = iconSlugToName(group.icon);
      }

      inserts.push(
        insertEntry(db, {
          id: entryId,
          title: label,
          description: description ?? `Navigate to ${label}`,
          category: "nav",
          pathname: basePath,
          icon: resolvedIconName,
          status: moduleStatus,
          meta: keywords ? keywords.join(" ") : "",
          feature: groupFeature,
        }),
      );

      if (icon) {
        iconMapRef.current.set(entryId, icon);
      } else if (resolvedIconName) {
        // Load the overridden or inherited icon component
        const cached = getCachedPfIcon(resolvedIconName);
        if (cached) {
          iconMapRef.current.set(entryId, cached);
        } else {
          loadPfIcon(resolvedIconName).then((comp) => {
            if (!cancelled && comp) iconMapRef.current.set(entryId, comp);
          });
        }
      }
      if (searchResult) {
        componentMapRef.current.set(entryId, searchResult);
      }

      featureParentRef.current.set(globalFeatureId, {
        id: entryId,
        title: label,
        description: description ?? `Navigate to ${label}`,
        category: "nav",
        pathname: basePath,
        icon: resolvedIconName,
        status: moduleStatus,
        feature: groupFeature,
      });

      if (extensionPoints) {
        for (const ep of Object.values(extensionPoints)) {
          typeToFeatureId.set(ep.type, globalFeatureId);

          if (ep.type === "fleetshift.cluster-provider") {
            const createId = `ext-${globalFeatureId}-create`;
            inserts.push(
              insertEntry(db, {
                id: createId,
                title: "Create cluster",
                description: "Launch the cluster creation wizard",
                category: "nav",
                pathname: `${basePath}?create`,
                icon: "",
                status: moduleStatus,
                meta: "create deploy provision wizard",
                feature: globalFeatureId,
              }),
            );
            if (icon) iconMapRef.current.set(createId, icon);
          }
        }
      }
    }

    // Scan cluster-provider extensions: link to parent via type
    for (const ext of cpExtensions) {
      const { id, label, description, keywords, to, searchResult, searchIcon } =
        ext.properties;

      const parentFeatureId = typeToFeatureId.get(
        "fleetshift.cluster-provider",
      );
      const parent = parentFeatureId
        ? featureParentRef.current.get(parentFeatureId)
        : undefined;

      const extPath = to?.pathname ?? "";
      const extSearch = to?.search ?? "";
      let resolvedPathname = "";
      if (parent) {
        const parts = [parent.pathname, extPath].filter(Boolean);
        resolvedPathname = parts.join("/").replace(/\/+/g, "/") + extSearch;
      }

      const entryId = `ext-${id}`;

      inserts.push(
        insertEntry(db, {
          id: entryId,
          title: `Create ${label} cluster`,
          description,
          category: parent?.category ?? "nav",
          pathname: resolvedPathname,
          icon: "",
          status: "",
          meta: keywords ? keywords.join(" ") : "",
          feature: parentFeatureId,
        }),
      );

      if (searchIcon) {
        iconMapRef.current.set(entryId, searchIcon);
      }
      if (searchResult) {
        componentMapRef.current.set(entryId, searchResult);
      }
    }

    // Group parent entries: one per merged layout group so children link via `feature`.
    for (const group of groupsById.values()) {
      const custom = isCustomGroup(group);
      const groupFeatureId = `group.${group.groupId}`;
      const firstChild = group.children[0];
      const firstPage = firstChild ? pageMap.get(firstChild.pageId) : undefined;
      const groupPath = firstPage
        ? `/${firstPage.path.split("/")[0]}`
        : `/${group.groupId}`;
      const entryId = `group-${group.groupId}`;

      const description =
        custom && group.description
          ? group.description
          : `${group.label} settings and configuration`;
      const meta = [
        group.label,
        ...(custom && group.keywords ? group.keywords : []),
      ].join(" ");
      const category = "nav";
      const iconName =
        custom && group.icon ? iconSlugToName(group.icon) : "CogIcon";

      inserts.push(
        insertEntry(db, {
          id: entryId,
          title: group.label,
          description,
          category,
          pathname: groupPath,
          icon: iconName,
          status: "",
          meta,
        }),
      );

      // Load custom group icon component for search result rendering
      if (custom && group.icon) {
        const cached = getCachedPfIcon(iconName);
        if (cached) {
          iconMapRef.current.set(entryId, cached);
        } else {
          loadPfIcon(iconName).then((comp) => {
            if (!cancelled && comp) iconMapRef.current.set(entryId, comp);
          });
        }
      }

      featureParentRef.current.set(groupFeatureId, {
        id: entryId,
        title: group.label,
        description,
        category,
        pathname: groupPath,
        icon: iconName,
        status: "",
      });
    }

    Promise.all(inserts);
    return () => {
      cancelled = true;
    };
  }, [
    modulesLoaded,
    cpLoaded,
    installLoaded,
    isInstalled,
    moduleExtensions,
    cpExtensions,
    pluginPages,
    navLayout,
    override,
    currentInstallVersion,
  ]);

  const query = useCallback(async (term: string): Promise<GroupedResults> => {
    if (!dbRef.current) return {};
    const results = await queryIndex(dbRef.current, term);

    for (const items of Object.values(results)) {
      for (const item of items) {
        const comp = componentMapRef.current.get(item.id);
        if (comp) item.Component = comp;
        const icon = iconMapRef.current.get(item.id);
        if (icon) item.IconComponent = icon;
      }
    }

    for (const items of Object.values(results)) {
      const needed: string[] = [];
      const seen = new Set<string>();
      for (const item of items) {
        if (item.feature) needed.push(item.feature);
      }
      while (needed.length > 0) {
        const featureId = needed.shift();
        if (!featureId || seen.has(featureId)) continue;
        seen.add(featureId);
        const parent = featureParentRef.current.get(featureId);
        if (!parent) continue;
        const targetCat = parent.category;
        const targetItems = results[targetCat] ?? [];
        if (targetItems.some((i) => i.id === parent.id)) continue;
        const parentItem = { ...parent };
        const comp = componentMapRef.current.get(parentItem.id);
        if (comp) parentItem.Component = comp;
        const icon = iconMapRef.current.get(parentItem.id);
        if (icon) parentItem.IconComponent = icon;
        results[targetCat] = [parentItem, ...targetItems];
        if (parentItem.feature) needed.push(parentItem.feature);
      }
    }

    return results;
  }, []);

  return (
    <SearchContext.Provider value={{ query }}>
      {children}
    </SearchContext.Provider>
  );
}

export function useSearch() {
  const ctx = useContext(SearchContext);
  if (!ctx) throw new Error("useSearch must be used within a SearchProvider");
  return ctx;
}
