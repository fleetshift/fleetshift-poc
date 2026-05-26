import { Navigate, Outlet, Route, Routes } from "react-router-dom";
import { SetupLayout } from "../layouts/SetupLayout";
import { AuthProvider } from "../contexts/AuthContext";
import AuthGate from "../components/Auth/AuthGate";
import { useSetupExtensions } from "../hooks/useSetupExtensions";
import { Bullseye, Spinner } from "@patternfly/react-core";

const SetupRoutes = () => {
  const { authRoutes, nonAuthRoutes, loaded } = useSetupExtensions();

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
            path={ext.properties.path}
            element={<ext.properties.component />}
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
          {authRoutes.map((ext) => (
            <Route
              key={ext.properties.id}
              path={ext.properties.path}
              element={<ext.properties.component />}
            />
          ))}
        </Route>
        <Route path="*" element={<Navigate to={defaultSetupPath} replace />} />
      </Route>
    </Routes>
  );
};

export default SetupRoutes;
