import { useMemo } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import AuthGate from "../components/Auth/AuthGate";
import AppConfigBridge from "../components/Root/AppConfigBridge";
import ScalprumShell from "../components/Root/ScalprumShell";
import { AppConfigProvider, useAppConfig } from "../contexts/AppConfigContext";
import { AuthProvider } from "../contexts/AuthContext";
import { AppLayout } from "../layouts/AppLayout";
import { DebugPage } from "../pages/DebugPage";
import { NotFoundPage } from "../pages/NotFoundPage";
import { PluginPage } from "../pages/PluginPage";

const ConsoleRoutesInner = () => {
  const { pluginPages, navLayout } = useAppConfig();

  const sortedPages = useMemo(
    () => [...pluginPages].sort((a, b) => b.path.length - a.path.length),
    [pluginPages],
  );

  const groupRedirects = useMemo(() => {
    const pageMap = new Map(pluginPages.map((p) => [p.id, p]));
    const redirects: { from: string; to: string }[] = [];
    for (const entry of navLayout) {
      if (entry.type !== "group" || entry.children.length === 0) continue;
      const firstChild = pageMap.get(entry.children[0].pageId);
      if (firstChild) {
        redirects.push({
          from: `/${entry.groupId}`,
          to: `/${firstChild.path}`,
        });
      }
    }
    return redirects;
  }, [navLayout, pluginPages]);

  return (
    <AppConfigBridge>
      <Routes>
        <Route element={<AppLayout />}>
          <Route
            path="/"
            element={<Navigate to="/overview/overview" replace />}
          />
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
          {groupRedirects.map((r) => (
            <Route
              key={r.from}
              path={r.from}
              element={<Navigate to={r.to} replace />}
            />
          ))}
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </AppConfigBridge>
  );
};

const ConsoleRoutes = () => (
  <AuthProvider>
    <AuthGate>
      <AppConfigProvider>
        <ScalprumShell>
          <ConsoleRoutesInner />
        </ScalprumShell>
      </AppConfigProvider>
    </AuthGate>
  </AuthProvider>
);

export default ConsoleRoutes;
