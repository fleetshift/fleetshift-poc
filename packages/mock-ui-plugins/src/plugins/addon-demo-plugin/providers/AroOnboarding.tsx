import { AzureIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const AroOnboardingCard = createOnboardingCard({
  title: "Connect to Azure Red Hat OpenShift",
  description:
    "Link your Azure subscription to deploy and manage ARO clusters.",
  icon: AzureIcon,
});

export const AroOnboardingForm = createConnectionForm({
  providerLabel: "Azure Red Hat OpenShift",
});
