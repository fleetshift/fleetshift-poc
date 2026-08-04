import type { PluginManifest } from "@openshift/dynamic-plugin-sdk";
import { createContext, ReactNode, useContext } from "react";

export interface PluginEntry {
  name: string;
  key: string;
  label: string;
  persona: "ops" | "dev";
  pluginManifest: PluginManifest;
}

export interface PluginRegistry {
  assetsHost: string;
  plugins: Record<string, PluginEntry>;
}

interface PluginRegistryContextValue {
  registry: PluginRegistry;
  pluginEntries: PluginEntry[];
}

const PluginRegistryContext = createContext<PluginRegistryContextValue | null>(
  null,
);

interface PluginRegistryProviderProps {
  pluginEntries: PluginEntry[];
  assetsHost: string;
  children: ReactNode;
}

export function PluginRegistryProvider({
  pluginEntries,
  assetsHost,
  children,
}: PluginRegistryProviderProps) {
  // Build the registry object from pluginEntries for backward compat
  const plugins: Record<string, PluginEntry> = {};
  for (const entry of pluginEntries) {
    plugins[entry.name] = entry;
  }
  const registry: PluginRegistry = { assetsHost, plugins };

  return (
    <PluginRegistryContext.Provider value={{ registry, pluginEntries }}>
      {children}
    </PluginRegistryContext.Provider>
  );
}

export function usePluginRegistry(): PluginRegistryContextValue {
  const ctx = useContext(PluginRegistryContext);
  if (!ctx)
    throw new Error(
      "usePluginRegistry must be used within a PluginRegistryProvider",
    );
  return ctx;
}
