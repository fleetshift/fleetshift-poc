import type {
  OnboardingActionCardProps,
  OnboardingActionFormProps,
} from "@fleetshift/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

import type { SearchReservedProperties } from "./searchTypes";

export type OnboardingActionExtension = Extension<
  "fleetshift.onboarding-action",
  {
    id: string;
    label: string;
    description?: string;
    keywords?: string[];
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<OnboardingActionCardProps>>;
    form: CodeRef<ComponentType<OnboardingActionFormProps>>;
    overviewCta?: string;
  } & SearchReservedProperties
>;

export function isOnboardingActionExtension(
  e: Extension,
): e is OnboardingActionExtension {
  return e.type === "fleetshift.onboarding-action";
}
