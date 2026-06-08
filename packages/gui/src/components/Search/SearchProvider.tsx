import {
  createContext,
  useContext,
  useEffect,
  useRef,
  ReactNode,
  useCallback,
} from "react";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import { useAppConfig } from "../../contexts/AppConfigContext";
import { isModuleExtension } from "../../extensions/isModuleExtension";
import { isClusterProviderExtension } from "../../extensions/isClusterProviderExtension";
import { isSetupExtension } from "../../extensions/isSetupExtension";
import type { SearchResultProps } from "../../extensions/searchTypes";
import {
  createSearchDB,
  insertEntry,
  queryIndex,
  type SearchDB,
  type GroupedResults,
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

export function SearchProvider({ children }: { children: ReactNode }) {
  const { pluginPages } = useAppConfig();
  const [moduleExtensions, modulesLoaded] =
    useResolvedExtensions(isModuleExtension);
  const [cpExtensions, cpLoaded] = useResolvedExtensions(
    isClusterProviderExtension,
  );
  const [setupExtensions, setupLoaded] =
    useResolvedExtensions(isSetupExtension);
  const dbRef = useRef<SearchDB | null>(null);
  const componentMapRef = useRef(
    new Map<string, React.ComponentType<SearchResultProps>>(),
  );
  const iconMapRef = useRef(new Map<string, React.ComponentType>());
  const featureParentRef = useRef(new Map<string, SearchResultItem>());
  const builtRef = useRef(false);

  useEffect(() => {
    if (!modulesLoaded || !cpLoaded || !setupLoaded || builtRef.current) return;
    builtRef.current = true;

    const db = createSearchDB();
    dbRef.current = db;

    // Maps extension type → global feature ID for parent-child linking
    const typeToFeatureId = new Map<string, string>();

    // Build a set of page IDs that have a richer module extension
    const modulePageIds = new Set<string>();
    for (const ext of moduleExtensions) {
      const page = pluginPages.find((p) => p.title === ext.properties.label);
      if (page) modulePageIds.add(page.id);
    }

    // Nav pages — skip any that have a module extension (those get richer entries below)
    // Also skip detail pages whose path contains route params (e.g. ":id")
    for (const page of pluginPages) {
      if (modulePageIds.has(page.id)) continue;
      if (page.path.includes(":")) continue;

      const navId = `nav-${page.id}`;
      const pathname = `/${page.path}`;
      insertEntry(db, {
        id: navId,
        title: page.title,
        description: `Navigate to ${page.title}`,
        category: "nav",
        pathname,
        icon: "CubesIcon",
        status: "",
        meta: page.path,
      });

      featureParentRef.current.set(page.id, {
        id: navId,
        title: page.title,
        description: `Navigate to ${page.title}`,
        category: "nav",
        pathname,
        icon: "CubesIcon",
        status: "",
      });
    }

    for (const cluster of MOCK_CLUSTERS) {
      insertEntry(db, { ...cluster, category: "cluster", icon: "ServerIcon" });
    }

    for (const setting of SETTINGS) {
      insertEntry(db, { ...setting, category: "setting", icon: "CogIcon" });
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
      } = ext.properties;

      const page = pluginPages.find((p) => p.title === label);
      if (!page) continue;

      const globalFeatureId = `${page.pluginKey}.${id}`;
      const basePath = `/${page.path}`;
      const entryId = `ext-${globalFeatureId}`;

      insertEntry(db, {
        id: entryId,
        title: label,
        description: description ?? `Navigate to ${label}`,
        category: "nav",
        pathname: basePath,
        icon: "CubesIcon",
        status: "",
        meta: keywords ? keywords.join(" ") : "",
      });

      if (searchResult) {
        componentMapRef.current.set(entryId, searchResult);
      }

      featureParentRef.current.set(globalFeatureId, {
        id: entryId,
        title: label,
        description: description ?? `Navigate to ${label}`,
        category: "nav",
        pathname: basePath,
        icon: "CubesIcon",
        status: "",
      });

      if (extensionPoints) {
        for (const ep of Object.values(extensionPoints)) {
          typeToFeatureId.set(ep.type, globalFeatureId);
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
      let resolvedPathname = "";
      if (parent) {
        const parts = [parent.pathname, extPath].filter(Boolean);
        resolvedPathname = parts.join("/").replace(/\/+/g, "/");
      }

      const entryId = `ext-${id}`;

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
      });

      if (searchIcon) {
        iconMapRef.current.set(entryId, searchIcon);
      }
      if (searchResult) {
        componentMapRef.current.set(entryId, searchResult);
      }
    }

    // Scan setup extensions: link to parent via type
    for (const ext of setupExtensions) {
      const { id, label, description, path } = ext.properties;

      const parentFeatureId = typeToFeatureId.get("fleetshift.setup");
      const parent = parentFeatureId
        ? featureParentRef.current.get(parentFeatureId)
        : undefined;

      let resolvedPathname = "";
      if (parent) {
        const parts = [parent.pathname, path].filter(Boolean);
        resolvedPathname = parts.join("/").replace(/\/+/g, "/");
      }

      const entryId = `ext-${id}`;

      insertEntry(db, {
        id: entryId,
        title: label,
        description: description ?? `Navigate to ${label}`,
        category: parent?.category ?? "nav",
        pathname: resolvedPathname,
        icon: "CogIcon",
        status: "",
        meta: [label, id].join(" "),
        feature: parentFeatureId,
      });
    }
  }, [
    modulesLoaded,
    cpLoaded,
    setupLoaded,
    moduleExtensions,
    cpExtensions,
    setupExtensions,
    pluginPages,
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

    for (const [cat, items] of Object.entries(results)) {
      const resultIds = new Set(items.map((i) => i.id));
      const needed = new Set<string>();
      for (const item of items) {
        if (!item.feature) continue;
        const parent = featureParentRef.current.get(item.feature);
        if (parent && !resultIds.has(parent.id)) {
          needed.add(item.feature);
        }
      }
      for (const featureId of needed) {
        const parent = featureParentRef.current.get(featureId);
        if (!parent) continue;
        const parentItem = { ...parent };
        const comp = componentMapRef.current.get(parentItem.id);
        if (comp) parentItem.Component = comp;
        results[cat] = [parentItem, ...results[cat]];
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
