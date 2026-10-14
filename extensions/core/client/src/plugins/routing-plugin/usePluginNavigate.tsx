import { useScalprum } from "@scalprum/react-core";
import { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

interface FleetShiftApi {
  fleetshift: {
    getPluginPagePath: (scope: string, module: string) => string | undefined;
  };
}

interface PluginNavigateTo {
  pathname?: string;
  search?: string;
}

const usePluginNavigate = (scope: string, module: string) => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const navigate = useNavigate();

  const basePath = api.fleetshift.getPluginPagePath(scope, module);
  const available = basePath !== undefined;

  const navigateFn = useCallback(
    (to?: string | PluginNavigateTo) => {
      // Re-resolve at call time for freshness
      const resolvedBase = api.fleetshift.getPluginPagePath(scope, module);
      if (!resolvedBase) return;

      if (!to) {
        navigate(resolvedBase);
      } else if (typeof to === "string") {
        navigate(to ? `${resolvedBase}/${to}` : resolvedBase);
      } else {
        navigate({
          pathname: to.pathname
            ? `${resolvedBase}/${to.pathname}`
            : resolvedBase,
          search: to.search,
        });
      }
    },
    [api, navigate, scope, module],
  );

  return useMemo(
    () => ({ navigate: navigateFn, available }),
    [navigateFn, available],
  );
};

export default usePluginNavigate;
