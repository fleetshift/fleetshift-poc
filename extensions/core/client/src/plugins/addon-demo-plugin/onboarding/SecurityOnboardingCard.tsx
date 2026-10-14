import { ShieldAltIcon } from "@patternfly/react-icons";

import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const SecurityOnboardingCard = createOnboardingCard({
  title: "Enable Security",
  description:
    "Scan images, enforce admission policies, and monitor compliance across your fleet.",
  icon: ShieldAltIcon,
});
