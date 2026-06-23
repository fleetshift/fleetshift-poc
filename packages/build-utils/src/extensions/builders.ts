import type {
  ClusterProviderExtras,
  ClusterProviderProperties,
  FleetshiftExtension,
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
): FleetshiftExtension<"fleetshift.module-group", ModuleGroupExtras> {
  throwOnErrors(
    validateModuleGroupProperties(properties),
    "fleetshift.module-group",
  );
  return { type: "fleetshift.module-group", properties };
}

export function createModule(
  properties: ModuleProperties,
): FleetshiftExtension<"fleetshift.module", ModuleExtras> {
  throwOnErrors(validateModuleProperties(properties), "fleetshift.module");
  return { type: "fleetshift.module", properties };
}

export function createSetup(
  properties: SetupProperties,
): FleetshiftExtension<"fleetshift.setup", SetupExtras> {
  throwOnErrors(validateSetupProperties(properties), "fleetshift.setup");
  return { type: "fleetshift.setup", properties };
}

export function createClusterProvider(
  properties: ClusterProviderProperties,
): FleetshiftExtension<"fleetshift.cluster-provider", ClusterProviderExtras> {
  throwOnErrors(
    validateClusterProviderProperties(properties),
    "fleetshift.cluster-provider",
  );
  return { type: "fleetshift.cluster-provider", properties };
}

export function createOnboardingAction(
  properties: OnboardingActionProperties,
): FleetshiftExtension<"fleetshift.onboarding-action", OnboardingActionExtras> {
  throwOnErrors(
    validateOnboardingActionProperties(properties),
    "fleetshift.onboarding-action",
  );
  return { type: "fleetshift.onboarding-action", properties };
}
