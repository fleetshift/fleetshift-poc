import { ServerIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const AssistedInstallerProviderCard = createProviderCard({
  label: "Assisted Installer",
  description:
    "Create an OpenShift cluster on bare-metal or on-premise infrastructure using the Assisted Installer.",
  icon: ServerIcon,
  ariaLabel: "Select Assisted Installer provider",
});

export { ServerIcon as AssistedInstallerIcon } from "@patternfly/react-icons";

export const AssistedInstallerWizard = createProviderWizard({
  providerName: "Assisted Installer",
});
