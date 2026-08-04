import { AzureIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const AroProviderCard = createProviderCard({
  label: "Azure Red Hat OpenShift",
  description:
    "Create a managed OpenShift cluster on Azure Red Hat OpenShift (ARO).",
  icon: AzureIcon,
  ariaLabel: "Select Azure Red Hat OpenShift provider",
});

export { AzureIcon as AroIcon } from "@patternfly/react-icons";

export const AroWizard = createProviderWizard({
  providerName: "Azure Red Hat OpenShift",
});
