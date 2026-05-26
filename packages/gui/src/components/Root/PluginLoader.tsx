import { useScalprum } from "@scalprum/react-core";
import { PropsWithChildren, useEffect, useState } from "react";

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
    Promise.all(loads)
      .catch(() => {})
      .finally(() => setInitialLoad(false));
  }, [config, pluginStore]);

  if (initialLoad) return null;
  return <>{children}</>;
};

export default PluginLoader;
