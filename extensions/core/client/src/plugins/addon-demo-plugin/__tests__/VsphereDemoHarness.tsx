import { useState } from "react";

import {
  VsphereOnboardingCard,
  VsphereOnboardingForm,
} from "../providers/VsphereOnboarding";
import {
  VsphereProviderCard,
  VsphereWizard,
} from "../providers/VsphereProvider";

export function VsphereDemoHarness({ onboarding = false }) {
  const [open, setOpen] = useState(false);
  const [finished, setFinished] = useState(false);
  const finish = () => {
    setOpen(false);
    setFinished(true);
  };

  if (finished) return <p>Demo finished</p>;
  if (onboarding) {
    return open ? (
      <VsphereOnboardingForm onComplete={finish} onCancel={finish} />
    ) : (
      <VsphereOnboardingCard
        completed={false}
        onConfigure={() => setOpen(true)}
      />
    );
  }
  return open ? (
    <VsphereWizard onClose={finish} />
  ) : (
    <VsphereProviderCard onSelect={() => setOpen(true)} />
  );
}
