import VsphereIcon from "../icons/VsphereIcon";
import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const VsphereOnboardingCard = createOnboardingCard({
  title: "vSphere connection demo",
  description:
    "Explore vSphere connection setup. Demonstration only; no connection is created.",
  icon: VsphereIcon,
});

export const VsphereOnboardingForm = createConnectionForm({
  providerLabel: "vSphere",
});
