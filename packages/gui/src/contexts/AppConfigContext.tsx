import type { AppsConfig } from "@scalprum/core";
import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react";

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
  /** PF icon name override (e.g. "CogIcon"). Takes priority over plugin-defined icon. */
  iconOverride?: string;
}

export interface NavLayoutGroup {
  type: "group";
  groupId: string;
  pluginKey: string;
  label: string;
  children: NavLayoutPage[];
}

export interface NavLayoutSection {
  type: "section";
  id: string;
  label: string;
  children: { pageId: string }[];
}

export type NavLayoutEntry = NavLayoutPage | NavLayoutGroup | NavLayoutSection;

interface AppConfigContextValue {
  scalprumConfig: AppsConfig<{ assetsHost: string }>;
  pluginPages: PluginPage[];
  navLayout: NavLayoutEntry[];
  pluginEntries: PluginEntry[];
  assetsHost: string;
}

const AppConfigContext = createContext<AppConfigContextValue | null>(null);

const FALLBACK_CONFIG: AppConfigContextValue = {
  scalprumConfig: {},
  pluginPages: [],
  navLayout: [],
  pluginEntries: [],
  assetsHost: "",
};

export function AppConfigProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<AppConfigContextValue | null>(null);

  useEffect(() => {
    async function loadConfig() {
      // Global UI bootstrap data (scalprum, plugin pages, entries) is
      // served by /api/ui/config (unauthenticated). User-specific data
      // (navLayout) comes from /api/ui/user-config (authenticated when
      // OIDC is configured).
      //
      // Backward compatibility: older backends serve everything from
      // /api/ui/user-config. When /api/ui/config lacks scalprumConfig
      // we fall back to user-config for global fields too.
      const [configData, userConfigData] = await Promise.all([
        fetch("/api/ui/config")
          .then((res) => (res.ok ? res.json() : {}))
          .catch(() => ({})),
        fetch("/api/ui/user-config")
          .then((res) => (res.ok ? res.json() : {}))
          .catch(() => ({})),
      ]);

      setConfig({
        scalprumConfig:
          configData.scalprumConfig ??
          userConfigData.scalprumConfig ??
          FALLBACK_CONFIG.scalprumConfig,
        pluginPages:
          configData.pluginPages ??
          userConfigData.pluginPages ??
          FALLBACK_CONFIG.pluginPages,
        pluginEntries:
          configData.pluginEntries ??
          userConfigData.pluginEntries ??
          FALLBACK_CONFIG.pluginEntries,
        assetsHost:
          configData.assetsHost ??
          userConfigData.assetsHost ??
          FALLBACK_CONFIG.assetsHost,
        navLayout: userConfigData.navLayout ?? FALLBACK_CONFIG.navLayout,
      });
    }

    loadConfig().catch((err) => {
      console.error("Failed to load app config:", err);
      setConfig(FALLBACK_CONFIG);
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
