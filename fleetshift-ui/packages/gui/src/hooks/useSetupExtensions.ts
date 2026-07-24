import {
  LoadedAndResolvedExtension,
  usePluginInfo,
  useResolvedExtensions,
} from "@openshift/dynamic-plugin-sdk";
import { useMemo } from "react";

import {
  isSetupExtension,
  SetupExtension,
} from "../extensions/isSetupExtension";
import {
  type ResolvedSetup,
  resolveSetupExtensions,
} from "../utils/setupExtensions";

export interface PreloadTarget {
  scope: string;
  module: string;
}

function buildPreloadMap(
  pluginInfo: ReturnType<typeof usePluginInfo>,
): Map<string, PreloadTarget> {
  const map = new Map<string, PreloadTarget>();
  for (const entry of pluginInfo) {
    if (entry.status !== "loaded") continue;
    const pluginName = entry.manifest.name;
    for (const ext of entry.manifest.extensions) {
      if (ext.type !== "fleetshift.setup") continue;
      const props = ext.properties as Record<string, unknown>;
      const id = props.id as string | undefined;
      const codeRef = (props.component as { $codeRef?: string })?.$codeRef;
      if (!id || !codeRef) continue;
      const moduleName = codeRef.split(".")[0];
      map.set(id, { scope: pluginName, module: moduleName });
    }
  }
  return map;
}

export function useSetupExtensions() {
  const [extensions, loaded] = useResolvedExtensions(isSetupExtension);
  const pluginInfo = usePluginInfo();

  const [authExtensions, nonAuthExtensions] = useMemo(
    () =>
      extensions.reduce<
        [
          LoadedAndResolvedExtension<SetupExtension>[],
          LoadedAndResolvedExtension<SetupExtension>[],
        ]
      >(
        (acc, ext) => {
          if (ext.properties.requiresAuth) {
            acc[0].push(ext);
          } else {
            acc[1].push(ext);
          }
          return acc;
        },
        [[], []],
      ),
    [extensions],
  );

  const authRoutes = useMemo<ResolvedSetup[]>(() => {
    if (!loaded) return [];
    return resolveSetupExtensions(authExtensions, extensions);
  }, [authExtensions, extensions, loaded]);

  const nonAuthRoutes = useMemo<ResolvedSetup[]>(() => {
    if (!loaded) return [];
    return resolveSetupExtensions(nonAuthExtensions, extensions);
  }, [nonAuthExtensions, extensions, loaded]);

  const preloadMap = useMemo(() => buildPreloadMap(pluginInfo), [pluginInfo]);

  return { authRoutes, nonAuthRoutes, loaded, preloadMap };
}
