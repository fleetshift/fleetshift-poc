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

interface ModuleTarget {
  scope: string;
  module: string;
}

interface PluginLinkEntry {
  navigate: (to?: string | PluginNavigateTo) => void;
  available: boolean;
}

const usePluginLinks = (
  targets: ModuleTarget[],
): Record<string, PluginLinkEntry> => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const navigate = useNavigate();

  const makeNavigate = useCallback(
    (scope: string, module: string) => (to?: string | PluginNavigateTo) => {
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
    [api, navigate],
  );

  return useMemo(() => {
    const result: Record<string, PluginLinkEntry> = {};
    for (const { scope, module } of targets) {
      const key = `${scope}/${module}`;
      const basePath = api.fleetshift.getPluginPagePath(scope, module);
      result[key] = {
        navigate: makeNavigate(scope, module),
        available: basePath !== undefined,
      };
    }
    return result;
  }, [api, makeNavigate, targets]);
};

export default usePluginLinks;
