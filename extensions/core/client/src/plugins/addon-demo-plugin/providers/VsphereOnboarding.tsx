import VsphereIcon from "../icons/VsphereIcon";
import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const VsphereOnboardingCard = createOnboardingCard({
  title: "Connect to vSphere",
  description:
    "Link your VMware vSphere environment to deploy and manage clusters.",
  icon: VsphereIcon,
});

export const VsphereOnboardingForm = createConnectionForm({
  providerLabel: "vSphere",
});
