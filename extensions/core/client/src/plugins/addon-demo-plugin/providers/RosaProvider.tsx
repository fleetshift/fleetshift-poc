import { OpenshiftIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const RosaProviderCard = createProviderCard({
  label: "ROSA",
  description:
    "Create a Red Hat OpenShift Service on AWS (ROSA) managed cluster.",
  icon: OpenshiftIcon,
  ariaLabel: "Select ROSA provider",
});

export { OpenshiftIcon as RosaIcon } from "@patternfly/react-icons";

export const RosaWizard = createProviderWizard({ providerName: "ROSA" });
