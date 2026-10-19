import type {
  ClusterProviderExtras,
  ClusterProviderProperties,
  FleetExtension,
  ModuleExtras,
  ModuleGroupExtras,
  ModuleGroupProperties,
  ModuleProperties,
  OnboardingActionExtras,
  OnboardingActionProperties,
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
): FleetExtension<"ocm-fleet.module-group", ModuleGroupExtras> {
  throwOnErrors(
    validateModuleGroupProperties(properties),
    "ocm-fleet.module-group",
  );
  return { type: "ocm-fleet.module-group", properties };
}

export function createModule(
  properties: ModuleProperties,
): FleetExtension<"ocm-fleet.module", ModuleExtras> {
  throwOnErrors(validateModuleProperties(properties), "ocm-fleet.module");
  return { type: "ocm-fleet.module", properties };
}

export function createSetup(
  properties: SetupProperties,
): FleetExtension<"ocm-fleet.setup", SetupExtras> {
  throwOnErrors(validateSetupProperties(properties), "ocm-fleet.setup");
  return { type: "ocm-fleet.setup", properties };
}

export function createClusterProvider(
  properties: ClusterProviderProperties,
): FleetExtension<"ocm-fleet.cluster-provider", ClusterProviderExtras> {
  throwOnErrors(
    validateClusterProviderProperties(properties),
    "ocm-fleet.cluster-provider",
  );
  return { type: "ocm-fleet.cluster-provider", properties };
}

export function createOnboardingAction(
  properties: OnboardingActionProperties,
): FleetExtension<"ocm-fleet.onboarding-action", OnboardingActionExtras> {
  throwOnErrors(
    validateOnboardingActionProperties(properties),
    "ocm-fleet.onboarding-action",
  );
  return { type: "ocm-fleet.onboarding-action", properties };
}
