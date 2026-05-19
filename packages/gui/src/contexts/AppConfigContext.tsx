import {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";
import type { AppsConfig } from "@scalprum/core";
import type { PluginEntry } from "./PluginRegistryContext";

export interface PluginPage {
  id: string;
  title: string;
  path: string;
  scope: string;
  module: string;
  pluginKey: string;
}

export interface NavLayoutPage {
  type: "page";
  pageId: string;
}

export interface NavLayoutSection {
  type: "section";
  id: string;
  label: string;
  children: { pageId: string }[];
}

export type NavLayoutEntry = NavLayoutPage | NavLayoutSection;

interface AppConfigContextValue {
  scalprumConfig: AppsConfig<{ assetsHost: string }>;
  pluginPages: PluginPage[];
  navLayout: NavLayoutEntry[];
  pluginEntries: PluginEntry[];
  assetsHost: string;
}

const AppConfigContext = createContext<AppConfigContextValue | null>(null);

export function AppConfigProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<AppConfigContextValue | null>(null);

  useEffect(() => {
    fetch("/api/ui/user-config")
      .then((res) => {
        if (!res.ok) throw new Error(`${res.status}`);
        return res.json();
      })
      .then((data) => {
        setConfig({
          scalprumConfig: data.scalprumConfig,
          pluginPages: data.pluginPages,
          navLayout: data.navLayout,
          pluginEntries: data.pluginEntries,
          assetsHost: data.assetsHost,
        });
      })
      .catch((err) => {
        console.error("Failed to load app config:", err);
      });
  }, []);

  if (!config) return null;

  return (
    <AppConfigContext.Provider value={config}>
      {children}
    </AppConfigContext.Provider>
  );
}

export function useAppConfig(): AppConfigContextValue {
  const ctx = useContext(AppConfigContext);
  if (!ctx)
    throw new Error("useAppConfig must be used within an AppConfigProvider");
  return ctx;
}
