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
  onClose?: () => void;
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

export interface OnboardingActionCardProps {
  completed: boolean;
  onConfigure: () => void;
}

export interface OnboardingActionFormProps {
  onComplete: () => void;
  onCancel: () => void;
}

export interface ClusterDetailTabProps {
  clusterId: string | undefined;
}

export interface SearchEntry {
  id: string;
  title: string;
  description: string;
  category: string;
  pathname: string;
  icon: string;
  status: string;
  meta: string;
  feature?: string;
}
