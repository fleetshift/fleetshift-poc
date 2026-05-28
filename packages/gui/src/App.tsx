import { BrowserRouter } from "react-router-dom";
import { ScalprumProvider } from "@scalprum/react-core";
import { PropsWithChildren, useMemo, useRef } from "react";
import { AppConfigProvider, useAppConfig } from "./contexts/AppConfigContext";
import { AnimationsProvider } from "@patternfly/react-core";
import ScopeInitializer from "./components/Root/ScopeInitializer";
import PluginLoader from "./components/Root/PluginLoader";
import { scopeListenersRef, scopeRef } from "./components/Root/ScopeBridge";
import Routes from "./routes/Routes";

const ScalprumShell = ({ children }: PropsWithChildren) => {
  const { scalprumConfig, assetsHost, pluginPages } = useAppConfig();

  const pluginPagesRef = useRef(pluginPages);
  pluginPagesRef.current = pluginPages;

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
            if (manifest.name === "routing-plugin") {
              newManifest.loadScripts = manifest.loadScripts.map((script) =>
                script.startsWith("http") ? script : `${assetsHost}/${script}`,
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
