import { Bullseye, Spinner } from "@patternfly/react-core";
import { preloadModule } from "@scalprum/core";
import { useCallback, useEffect, useMemo } from "react";
import { Navigate, Outlet, Route, Routes, useNavigate } from "react-router-dom";

import AuthGate from "../components/Auth/AuthGate";
import ScalprumShell from "../components/Root/ScalprumShell";
import { AppConfigProvider } from "../contexts/AppConfigContext";
import { AuthProvider } from "../contexts/AuthContext";
import {
  type PreloadTarget,
  useSetupExtensions,
} from "../hooks/useSetupExtensions";
import { SetupLayout } from "../layouts/SetupLayout";
import type { ResolvedSetup } from "../utils/setupExtensions";

interface StepWrapperProps {
  ext: ResolvedSetup;
  onSetupNext: () => void;
  onSetupSkip?: () => void;
  preloadNext?: PreloadTarget;
}

const SetupStep = ({
  ext,
  onSetupNext,
  onSetupSkip,
  preloadNext,
}: StepWrapperProps) => {
  useEffect(() => {
    if (!preloadNext) return;
    preloadModule(preloadNext.scope, preloadNext.module).catch(() => {});
  }, [preloadNext]);

  const Component = ext.properties.component;
  return <Component onSetupNext={onSetupNext} onSetupSkip={onSetupSkip} />;
};

const SetupRoutesInner = () => {
  const { authRoutes, nonAuthRoutes, loaded, preloadMap } =
    useSetupExtensions();
  const navigate = useNavigate();

  const allSteps = useMemo(
    () => [...nonAuthRoutes, ...authRoutes],
    [nonAuthRoutes, authRoutes],
  );

  const nextPathFor = useCallback(
    (stepId: string) => {
      const idx = allSteps.findIndex((s) => s.properties.id === stepId);
      const next = allSteps[idx + 1];
      return next ? `/setup/${next.properties.path}` : "/";
    },
    [allSteps],
  );

  const preloadTargetFor = useCallback(
    (stepId: string): PreloadTarget | undefined => {
      const idx = allSteps.findIndex((s) => s.properties.id === stepId);
      const next = allSteps[idx + 1];
      return next ? preloadMap.get(next.properties.id) : undefined;
    },
    [allSteps, preloadMap],
  );

  const skipToConsole = useCallback(() => navigate("/"), [navigate]);

  if (!loaded) {
    return (
      <Bullseye>
        <Spinner size="xl" />
      </Bullseye>
    );
  }

  if (nonAuthRoutes.length === 0 && authRoutes.length === 0) {
    return (
      <div>
        No setup extensions found. Please install at least one setup extension
        to use the setup console.
      </div>
    );
  }

  const defaultSetupPath =
    nonAuthRoutes[0]?.properties.path ?? authRoutes[0]?.properties.path ?? "/";

  return (
    <Routes>
      <Route path="/setup/*" element={<SetupLayout />}>
        {nonAuthRoutes.map((ext) => (
          <Route
            key={ext.properties.id}
            path={`${ext.properties.path}/*`}
            element={
              <SetupStep
                ext={ext}
                onSetupNext={() => navigate(nextPathFor(ext.properties.id))}
                preloadNext={preloadTargetFor(ext.properties.id)}
              />
            }
          />
        ))}
        <Route
          element={
            <AuthProvider>
              <AuthGate>
                <Outlet />
              </AuthGate>
            </AuthProvider>
          }
        >
          {authRoutes.map((ext, i) => {
            const isLast =
              i === authRoutes.length - 1 &&
              nextPathFor(ext.properties.id) === "/";
            return (
              <Route
                key={ext.properties.id}
                path={`${ext.properties.path}/*`}
                element={
                  <SetupStep
                    ext={ext}
                    onSetupNext={() => navigate(nextPathFor(ext.properties.id))}
                    onSetupSkip={isLast ? undefined : skipToConsole}
                    preloadNext={preloadTargetFor(ext.properties.id)}
                  />
                }
              />
            );
          })}
        </Route>
        <Route path="*" element={<Navigate to={defaultSetupPath} replace />} />
      </Route>
    </Routes>
  );
};

const SetupRoutes = () => (
  <AuthProvider requireAuth={false}>
    <AppConfigProvider>
      <ScalprumShell>
        <SetupRoutesInner />
      </ScalprumShell>
    </AppConfigProvider>
  </AuthProvider>
);

export default SetupRoutes;
