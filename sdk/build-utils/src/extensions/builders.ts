import type {
  ClusterProviderExtras,
  ClusterProviderProperties,
  FleetExtension,
  OnboardingActionExtras,
  OnboardingActionProperties,
  RouteExtras,
  RouteGroupExtras,
  RouteGroupProperties,
  RouteProperties,
  SetupExtras,
  SetupProperties,
} from "./types";
import {
  validateClusterProviderProperties,
  validateOnboardingActionProperties,
  validateRouteGroupProperties,
  validateRouteProperties,
  validateSetupProperties,
} from "./validate";

function throwOnErrors(errors: string[], type: string): void {
  if (errors.length > 0) {
    throw new Error(
      `Invalid ${type} extension:\n${errors.map((e) => `  - ${e}`).join("\n")}`,
    );
  }
}

export function createRouteGroup(
  properties: RouteGroupProperties,
): FleetExtension<"ocm-fleet.route-group", RouteGroupExtras> {
  throwOnErrors(
    validateRouteGroupProperties(properties),
    "ocm-fleet.route-group",
  );
  return { type: "ocm-fleet.route-group", properties };
}

export function createRoute(
  properties: RouteProperties,
): FleetExtension<"ocm-fleet.route", RouteExtras> {
  throwOnErrors(validateRouteProperties(properties), "ocm-fleet.route");
  return { type: "ocm-fleet.route", properties };
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
