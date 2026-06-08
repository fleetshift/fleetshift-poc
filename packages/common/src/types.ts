export interface User {
  id: string;
  username: string;
  display_name: string;
  role: string;
}

export interface PluginEntry {
  name: string;
  key: string;
  label: string;
  persona: "ops" | "dev";
}

export interface PluginRegistry {
  assetsHost: string;
  plugins: Record<string, PluginEntry>;
}

export interface ClusterProviderCardProps {
  onSelect: () => void;
}

export interface ClusterProviderWizardProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}
