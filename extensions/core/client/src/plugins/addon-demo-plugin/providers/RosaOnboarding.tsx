import { OpenshiftIcon } from "@patternfly/react-icons";

import { createConnectionForm } from "../shared/GenericConnectionForm";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const RosaOnboardingCard = createOnboardingCard({
  title: "Connect to ROSA",
  description:
    "Link your Red Hat OpenShift on AWS account to manage ROSA clusters.",
  icon: OpenshiftIcon,
});

export const RosaOnboardingForm = createConnectionForm({
  providerLabel: "ROSA",
});
