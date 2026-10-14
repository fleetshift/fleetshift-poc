import { AwsIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const EksProviderCard = createProviderCard({
  label: "Amazon EKS",
  description:
    "Explore an Amazon EKS cluster wizard. Demonstration only; provisioning is not implemented.",
  icon: AwsIcon,
  ariaLabel: "Select Amazon EKS provider",
});

export { AwsIcon as EksIcon } from "@patternfly/react-icons";

export const EksWizard = createProviderWizard({ providerName: "Amazon EKS" });
