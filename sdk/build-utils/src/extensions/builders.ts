import type {
  ClusterProviderExtras,
  ClusterProviderProperties,
  ModuleExtras,
  ModuleGroupExtras,
  ModuleGroupProperties,
  ModuleProperties,
  OnboardingActionExtras,
  OnboardingActionProperties,
  ResourceManagerExtension,
  SetupExtras,
  SetupProperties,
} from "./types";
import {
  validateClusterProviderProperties,
  validateModuleGroupProperties,
  validateModuleProperties,
  validateOnboardingActionProperties,
  validateSetupProperties,
} from "./validate";

function throwOnErrors(errors: string[], type: string): void {
  if (errors.length > 0) {
    throw new Error(
      `Invalid ${type} extension:\n${errors.map((e) => `  - ${e}`).join("\n")}`,
    );
  }
}

export function createModuleGroup(
  properties: ModuleGroupProperties,
): ResourceManagerExtension<
  "ocm-resource-manager.module-group",
  ModuleGroupExtras
> {
  throwOnErrors(
    validateModuleGroupProperties(properties),
    "ocm-resource-manager.module-group",
  );
  return { type: "ocm-resource-manager.module-group", properties };
}

export function createModule(
  properties: ModuleProperties,
): ResourceManagerExtension<"ocm-resource-manager.module", ModuleExtras> {
  throwOnErrors(
    validateModuleProperties(properties),
    "ocm-resource-manager.module",
  );
  return { type: "ocm-resource-manager.module", properties };
}

export function createSetup(
  properties: SetupProperties,
): ResourceManagerExtension<"ocm-resource-manager.setup", SetupExtras> {
  throwOnErrors(
    validateSetupProperties(properties),
    "ocm-resource-manager.setup",
  );
  return { type: "ocm-resource-manager.setup", properties };
}

export function createClusterProvider(
  properties: ClusterProviderProperties,
): ResourceManagerExtension<
  "ocm-resource-manager.cluster-provider",
  ClusterProviderExtras
> {
  throwOnErrors(
    validateClusterProviderProperties(properties),
    "ocm-resource-manager.cluster-provider",
  );
  return { type: "ocm-resource-manager.cluster-provider", properties };
}

export function createOnboardingAction(
  properties: OnboardingActionProperties,
): ResourceManagerExtension<
  "ocm-resource-manager.onboarding-action",
  OnboardingActionExtras
> {
  throwOnErrors(
    validateOnboardingActionProperties(properties),
    "ocm-resource-manager.onboarding-action",
  );
  return { type: "ocm-resource-manager.onboarding-action", properties };
}
