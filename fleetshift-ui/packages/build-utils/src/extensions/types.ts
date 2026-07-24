export type EncodedCodeRef = { $codeRef: string };

export type ExtensionPointDeclaration = {
  description: string;
  type: string;
};

export type BaseExtensionProperties = {
  id: string;
  label: string;
  description?: string;
  keywords?: string[];
  searchResult?: EncodedCodeRef;
  searchIcon?: EncodedCodeRef;
};

export const FLEETSHIFT_EXTENSION_TYPES = [
  "fleetshift.module-group",
  "fleetshift.module",
  "fleetshift.setup",
  "fleetshift.cluster-provider",
  "fleetshift.onboarding-action",
  "fleetshift.cluster-detail-tab",
] as const;

export type FleetshiftExtensionType =
  (typeof FLEETSHIFT_EXTENSION_TYPES)[number];

export type FleetshiftExtension<
  TType extends string = string,
  TExtra extends Record<string, unknown> = Record<string, unknown>,
> = {
  type: TType;
  properties: BaseExtensionProperties & TExtra;
};

export type ModuleGroupExtras = Record<string, never>;

export type ModuleExtras = {
  component: EncodedCodeRef;
  icon: EncodedCodeRef;
  extensionPoints?: Record<string, ExtensionPointDeclaration>;
  group?: string;
};

export type SetupExtras = {
  path: string;
  component: EncodedCodeRef;
  requires: string[];
  requiresAuth: boolean;
  priority?: number;
};

export type ClusterProviderExtras = {
  description: string;
  to?: { pathname?: string; search?: string; hash?: string };
  icon: EncodedCodeRef;
  card: EncodedCodeRef;
  wizard: EncodedCodeRef;
};

export type OnboardingActionExtras = {
  icon: EncodedCodeRef;
  card: EncodedCodeRef;
  form: EncodedCodeRef;
  overviewCta?: string;
  category?: FleetshiftExtensionType;
};

export type ModuleGroupProperties = BaseExtensionProperties & ModuleGroupExtras;
export type ModuleProperties = BaseExtensionProperties & ModuleExtras;
export type SetupProperties = BaseExtensionProperties & SetupExtras;
export type ClusterProviderProperties = BaseExtensionProperties &
  ClusterProviderExtras;
export type OnboardingActionProperties = BaseExtensionProperties &
  OnboardingActionExtras;
