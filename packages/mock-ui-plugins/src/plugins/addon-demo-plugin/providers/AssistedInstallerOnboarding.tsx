import { ServerIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const AssistedInstallerOnboardingCard = createOnboardingCard({
  title: "Connect to Assisted Installer",
  description:
    "Link your infrastructure to deploy and manage bare-metal OpenShift clusters.",
  icon: ServerIcon,
});

export const AssistedInstallerOnboardingForm = createConnectionForm({
  providerLabel: "Assisted Installer",
});
