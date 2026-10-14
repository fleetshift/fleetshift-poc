import { AwsIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const EksOnboardingCard = createOnboardingCard({
  title: "Connect to Amazon EKS",
  description:
    "Explore EKS connection setup. Demonstration only; no connection is created.",
  icon: AwsIcon,
});

export const EksOnboardingForm = createConnectionForm({
  providerLabel: "Amazon EKS",
});
