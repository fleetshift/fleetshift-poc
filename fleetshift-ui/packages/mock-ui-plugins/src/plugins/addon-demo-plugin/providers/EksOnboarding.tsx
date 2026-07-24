import { AwsIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const EksOnboardingCard = createOnboardingCard({
  title: "Connect to Amazon EKS",
  description: "Link your AWS account to import and manage EKS clusters.",
  icon: AwsIcon,
});

export const EksOnboardingForm = createConnectionForm({
  providerLabel: "Amazon EKS",
});
