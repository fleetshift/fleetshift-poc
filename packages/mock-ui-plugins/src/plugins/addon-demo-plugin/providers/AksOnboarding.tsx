import { AzureIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const AksOnboardingCard = createOnboardingCard({
  title: "Connect to Azure AKS",
  description:
    "Link your Azure subscription to import and manage AKS clusters.",
  icon: AzureIcon,
});

export const AksOnboardingForm = createConnectionForm({
  providerLabel: "Azure AKS",
});
