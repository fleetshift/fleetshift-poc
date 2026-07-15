import { AzureIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const AksProviderCard = createProviderCard({
  label: "Azure AKS",
  description:
    "Create a managed Kubernetes cluster on Azure Kubernetes Service.",
  icon: AzureIcon,
  ariaLabel: "Select Azure AKS provider",
});

export { AzureIcon as AksIcon } from "@patternfly/react-icons";

export const AksWizard = createProviderWizard({ providerName: "Azure AKS" });
