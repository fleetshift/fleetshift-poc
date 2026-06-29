import { AnimationsProvider } from "@patternfly/react-core";
import { ScalprumProvider } from "@scalprum/react-core";
import { PropsWithChildren, useMemo, useRef } from "react";
import { BrowserRouter } from "react-router-dom";

import PluginLoader from "./components/Root/PluginLoader";
import { scopeListenersRef, scopeRef } from "./components/Root/ScopeBridge";
import ScopeInitializer from "./components/Root/ScopeInitializer";
import { AppConfigProvider, useAppConfig } from "./contexts/AppConfigContext";
import Routes from "./routes/Routes";

const ScalprumShell = ({ children }: PropsWithChildren) => {
  const { scalprumConfig, assetsHost, pluginPages, navLayout } = useAppConfig();

  const pluginPagesRef = useRef(pluginPages);
  pluginPagesRef.current = pluginPages;
  const navLayoutRef = useRef(navLayout);
  navLayoutRef.current = navLayout;

  const api = useMemo(
    () => ({
      fleetshift: {
        apiBase: "/v1",
        getPluginPagePath: (scope: string, module: string) => {
          const page = pluginPagesRef.current.find(
            (p) => p.scope === scope && p.module === module,
          );
          return page ? `/${page.path}` : undefined;
        },
        getNavPages: () => {
          const pageMap = new Map(pluginPagesRef.current.map((p) => [p.id, p]));
          const result: { id: string; scope: string; title: string }[] = [];
          for (const entry of navLayoutRef.current) {
            if (entry.type === "page") {
              const p = pageMap.get(entry.pageId);
              if (p) result.push({ id: p.id, scope: p.scope, title: p.title });
            } else if (entry.type === "group") {
              const groupScope = `${entry.pluginKey}-plugin`;
              result.push({
                id: entry.groupId,
                scope: groupScope,
                title: entry.label,
              });
              for (const child of entry.children) {
                const p = pageMap.get(child.pageId);
                if (p)
                  result.push({ id: p.id, scope: p.scope, title: p.title });
              }
            } else if (entry.type === "section") {
              for (const child of entry.children) {
                const p = pageMap.get(child.pageId);
                if (p)
                  result.push({ id: p.id, scope: p.scope, title: p.title });
              }
            }
          }
          return result;
        },
        getBackendLayout: () => navLayoutRef.current,
        getClusterIdsForPlugin: () => [] as string[],
        getClusterName: (clusterId: string) => clusterId,
        onClustersChange: () => () => {},
        getScope: () => scopeRef.current,
        onScopeChange: (fn: () => void) => {
          scopeListenersRef.current.add(fn);
          return () => {
            scopeListenersRef.current.delete(fn);
          };
        },
        on: () => () => {},
      },
    }),
    [],
  );

  return (
    <ScalprumProvider
      config={scalprumConfig}
      api={api}
      pluginSDKOptions={{
        pluginLoaderOptions: {
          transformPluginManifest: (manifest) => {
            const newManifest = { ...manifest };
            if (
              manifest.name === "routing-plugin" &&
              "loadScripts" in newManifest &&
              Array.isArray(newManifest.loadScripts)
            ) {
              newManifest.loadScripts = newManifest.loadScripts.map(
                (script: string) =>
                  script.startsWith("http")
                    ? script
                    : `${assetsHost}/${script}`,
              );
            }
            return newManifest;
          },
        },
      }}
    >
      <PluginLoader>{children}</PluginLoader>
    </ScalprumProvider>
  );
};

export const App = () => (
  <AnimationsProvider config={{ hasAnimations: true }}>
    <ScopeInitializer>
      <BrowserRouter>
        <AppConfigProvider>
          <ScalprumShell>
            <Routes />
          </ScalprumShell>
        </AppConfigProvider>
      </BrowserRouter>
    </ScopeInitializer>
  </AnimationsProvider>
);
