import { getUiConfig, getUiUserConfig } from "@fleetshift/common";
import type { NavLayoutEntry } from "@fleetshift/common/dynamic/navLayout";
import type { AppsConfig } from "@scalprum/core";
import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react";

import apiClient, { unwrap } from "../api/client";
import { normalizeNavLayout } from "../utils/normalizeNavLayout";
import type { PluginEntry } from "./PluginRegistryContext";

export interface PluginPage {
  id: string;
  title: string;
  path: string;
  scope: string;
  module: string;
  pluginKey: string;
}

interface AppConfigContextValue {
  scalprumConfig: AppsConfig;
  pluginPages: PluginPage[];
  navLayout: NavLayoutEntry[];
  pluginEntries: PluginEntry[];
  assetsHost: string;
  /** True when at least one OIDC auth method has been configured on
   *  the backend. Used by setup routes to gate steps that require
   *  authentication even when the outer AuthProvider is optional. */
  authConfigured: boolean;
}

const AppConfigContext = createContext<AppConfigContextValue | null>(null);

const FALLBACK_CONFIG: AppConfigContextValue = {
  scalprumConfig: {},
  pluginPages: [],
  navLayout: [],
  pluginEntries: [],
  assetsHost: "",
  authConfigured: false,
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
      const [configResult, userConfigResult] = await Promise.all([
        unwrap(getUiConfig({ client: apiClient })).catch(() => undefined),
        unwrap(getUiUserConfig({ client: apiClient })).catch(() => undefined),
      ]);

      setConfig({
        scalprumConfig:
          configResult?.scalprumConfig ??
          userConfigResult?.scalprumConfig ??
          FALLBACK_CONFIG.scalprumConfig,
        pluginPages:
          configResult?.pluginPages ??
          userConfigResult?.pluginPages ??
          FALLBACK_CONFIG.pluginPages,
        pluginEntries: (configResult?.pluginEntries ??
          userConfigResult?.pluginEntries ??
          FALLBACK_CONFIG.pluginEntries) as PluginEntry[],
        assetsHost:
          configResult?.assetsHost ??
          userConfigResult?.assetsHost ??
          FALLBACK_CONFIG.assetsHost,
        navLayout: normalizeNavLayout(userConfigResult?.navLayout ?? []),
        authConfigured: configResult?.authConfigured === true,
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
