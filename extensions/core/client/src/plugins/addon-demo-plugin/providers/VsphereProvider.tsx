import VsphereIcon from "../icons/VsphereIcon";
import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const VsphereProviderCard = createProviderCard({
  label: "vSphere",
  description:
    "Explore a Kubernetes cluster wizard for VMware vSphere. Demonstration only; provisioning is not implemented.",
  icon: VsphereIcon,
  ariaLabel: "Select vSphere provider",
});

export { default as VsphereIcon } from "../icons/VsphereIcon";

export const VsphereWizard = createProviderWizard({ providerName: "vSphere" });
