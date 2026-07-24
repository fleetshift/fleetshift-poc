import { MonitoringIcon } from "@patternfly/react-icons";

import { createOnboardingCard } from "../shared/GenericOnboardingCard";

export const ObservabilityOnboardingCard = createOnboardingCard({
  title: "Enable Observability",
  description:
    "Unified metrics, logs, and traces across your fleet from a single pane of glass.",
  icon: MonitoringIcon,
});
