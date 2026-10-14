import { ClipboardCheckIcon } from "@patternfly/react-icons";

import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const ComplianceOnboardingCard = createOnboardingCard({
  title: "Enable Compliance",
  description:
    "Track regulatory compliance and audit readiness across your fleet.",
  icon: ClipboardCheckIcon,
});
