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

export const OCM_FLEET_EXTENSION_TYPES = [
  "ocm-fleet.route-group",
  "ocm-fleet.route",
  "ocm-fleet.setup",
  "ocm-fleet.cluster-provider",
  "ocm-fleet.onboarding-action",
  "ocm-fleet.cluster-detail-tab",
] as const;

export type FleetExtensionType = (typeof OCM_FLEET_EXTENSION_TYPES)[number];

export type FleetExtension<
  TType extends string = string,
  TExtra extends Record<string, unknown> = Record<string, unknown>,
> = {
  type: TType;
  properties: BaseExtensionProperties & TExtra;
};

export type RouteGroupExtras = Record<never, never>;

export type RouteExtras = {
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
  category?: FleetExtensionType;
};

export type RouteGroupProperties = BaseExtensionProperties & RouteGroupExtras;
export type RouteProperties = BaseExtensionProperties & RouteExtras;
export type SetupProperties = BaseExtensionProperties & SetupExtras;
export type ClusterProviderProperties = BaseExtensionProperties &
  ClusterProviderExtras;
export type OnboardingActionProperties = BaseExtensionProperties &
  OnboardingActionExtras;
