import { AwsIcon } from "@patternfly/react-icons";

import { createProviderCard } from "../shared/GenericProviderCard";
import { createProviderWizard } from "../shared/GenericProviderWizard";

export const EksProviderCard = createProviderCard({
  label: "Amazon EKS",
  description:
    "Create a managed Kubernetes cluster on Amazon Elastic Kubernetes Service.",
  icon: AwsIcon,
  ariaLabel: "Select Amazon EKS provider",
});

export { AwsIcon as EksIcon } from "@patternfly/react-icons";

export const EksWizard = createProviderWizard({ providerName: "Amazon EKS" });
