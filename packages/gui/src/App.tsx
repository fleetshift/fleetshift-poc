import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
} from "react-router-dom";
import { ScalprumProvider, ScalprumComponent } from "@scalprum/react-core";
import { useScalprum } from "@scalprum/react-core";
import { initSharedScope } from "@scalprum/core";
import { PropsWithChildren, useEffect, useMemo, useRef, useState } from "react";
import { AppLayout } from "./layouts/AppLayout";
import { SetupLayout } from "./layouts/SetupLayout";
import { ClusterProvider } from "./contexts/ClusterContext";
import { ScopeProvider, useScope } from "./contexts/ScopeContext";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { PluginRegistryProvider } from "./contexts/PluginRegistryContext";
import { AppConfigProvider, useAppConfig } from "./contexts/AppConfigContext";
import { PluginPage } from "./pages/PluginPage";
import { DebugPage } from "./pages/DebugPage";
import { AnimationsProvider } from "@patternfly/react-core";
import { AuthErrorState } from "./components/AuthErrorState";

const scopeRef = { current: "all" as string };
const scopeListenersRef = { current: new Set<() => void>() };

const ScopeInitializer = ({ children }: PropsWithChildren) => {
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    initSharedScope().then(() => setLoading(false));
  }, []);
  if (loading) return null;
  return <>{children}</>;
};

const PluginLoader = ({ children }: PropsWithChildren) => {
  const { pluginStore, config } = useScalprum();
  const [initialLoad, setInitialLoad] = useState(true);

  useEffect(() => {
    const loads: Promise<void>[] = [];
    for (const entry of Object.values(config)) {
      if (entry.pluginManifest) {
        loads.push(pluginStore.loadPlugin(entry.pluginManifest));
      } else if (entry.manifestLocation) {
        loads.push(pluginStore.loadPlugin(entry.manifestLocation));
      }
    }
    if (loads.length === 0) {
      setInitialLoad(false);
      return;
    }
    Promise.all(loads).then(() => setInitialLoad(false));
  }, [config, pluginStore]);

  if (initialLoad) return null;
  return <>{children}</>;
};

const ScalprumShell = ({ children }: PropsWithChildren) => {
  const { scalprumConfig, assetsHost } = useAppConfig();

  const api = useMemo(
    () => ({
      fleetshift: {
        apiBase: "/v1",
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

const ScopeBridge = () => {
  const { scope } = useScope();
  useEffect(() => {
    scopeRef.current = scope;
    scopeListenersRef.current.forEach((fn) => fn());
  }, [scope]);
  return null;
};

const AuthGate = ({ children }: PropsWithChildren) => {
  const { loading, user, authError, login } = useAuth();
  const loginTriggered = useRef(false);

  useEffect(() => {
    if (!loading && !user && !authError && !loginTriggered.current) {
      loginTriggered.current = true;
      login();
    }
  }, [loading, user, authError, login]);

  if (authError) {
    return <AuthErrorState onSignIn={login} />;
  }

  if (loading || !user) return null;
  return <>{children}</>;
};

const AppRoutes = () => {
  const { pluginPages, navLayout } = useAppConfig();

  const sortedPages = useMemo(
    () => [...pluginPages].sort((a, b) => b.path.length - a.path.length),
    [pluginPages],
  );

  const pageMap = useMemo(() => {
    const map = new Map<string, (typeof pluginPages)[number]>();
    for (const page of pluginPages) map.set(page.id, page);
    return map;
  }, [pluginPages]);

  const firstNavPath = useMemo(() => {
    for (const entry of navLayout) {
      if (entry.type === "page") {
        const page = pageMap.get(entry.pageId);
        if (page) return `/${page.path}`;
      }
    }
    return "/debug";
  }, [navLayout, pageMap]);

  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<Navigate to={firstNavPath} replace />} />
        <Route path="/debug" element={<DebugPage />} />
        {sortedPages.map((page) => (
          <Route
            key={page.id}
            path={`/${page.path}/*`}
            element={
              <PluginPage
                scope={page.scope}
                module={page.module}
                pluginKey={page.pluginKey}
              />
            }
          />
        ))}
      </Route>
    </Routes>
  );
};

const AppConfigBridge = ({ children }: PropsWithChildren) => {
  const { pluginEntries, assetsHost } = useAppConfig();

  return (
    <PluginRegistryProvider
      pluginEntries={pluginEntries}
      assetsHost={assetsHost}
    >
      <ClusterProvider>
        <ScopeProvider>
          <ScopeBridge />
          {children}
        </ScopeProvider>
      </ClusterProvider>
    </PluginRegistryProvider>
  );
};

const SetupPluginPage = () => (
  <ScalprumComponent scope="day-one-plugin" module="InitialSetupForm" />
);

const SetupRoutes = () => (
  <Routes>
    <Route path="/setup" element={<SetupLayout />}>
      <Route index element={<SetupPluginPage />} />
    </Route>
  </Routes>
);

const AppShell = () => {
  const location = useLocation();

  if (location.pathname.startsWith("/setup")) {
    return <SetupRoutes />;
  }

  return (
    <AuthProvider>
      <AuthGate>
        <AppConfigBridge>
          <AppRoutes />
        </AppConfigBridge>
      </AuthGate>
    </AuthProvider>
  );
};

export const App = () => (
  <AnimationsProvider config={{ hasAnimations: true }}>
    <ScopeInitializer>
      <BrowserRouter>
        <AppConfigProvider>
          <ScalprumShell>
            <AppShell />
          </ScalprumShell>
        </AppConfigProvider>
      </BrowserRouter>
    </ScopeInitializer>
  </AnimationsProvider>
);
