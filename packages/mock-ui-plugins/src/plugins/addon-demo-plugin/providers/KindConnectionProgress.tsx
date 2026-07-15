import {
  Bullseye,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";
import { useEffect, useRef, useState } from "react";

interface Step {
  label: string;
  delay: number;
}

const STEPS: Step[] = [
  { label: "Checking Docker/Podman runtime", delay: 900 },
  { label: "Pulling node image", delay: 1600 },
  { label: "Creating Kind cluster", delay: 1200 },
  { label: "Configuring kubeconfig", delay: 800 },
  { label: "Connected", delay: 500 },
];

interface KindConnectionProgressProps {
  onComplete: () => void;
}

function stepVariant(
  stepIndex: number,
  activeIndex: number,
): "success" | "info" | "pending" {
  if (stepIndex < activeIndex) return "success";
  if (stepIndex === activeIndex) return "info";
  return "pending";
}

export default function KindConnectionProgress({
  onComplete,
}: KindConnectionProgressProps) {
  const [activeStep, setActiveStep] = useState(0);
  const completeRef = useRef(onComplete);
  completeRef.current = onComplete;

  useEffect(() => {
    if (activeStep >= STEPS.length) {
      const done = setTimeout(() => completeRef.current(), 600);
      return () => clearTimeout(done);
    }
    const timer = setTimeout(() => {
      setActiveStep((prev) => prev + 1);
    }, STEPS[activeStep].delay);
    return () => clearTimeout(timer);
  }, [activeStep]);

  return (
    <Bullseye>
      <ProgressStepper
        isVertical
        aria-label="Kind connection progress"
        className="pf-v6-u-mt-lg"
      >
        {STEPS.map((step, idx) => (
          <ProgressStep
            key={step.label}
            id={`kind-connect-step-${idx}`}
            titleId={`kind-connect-step-title-${idx}`}
            variant={stepVariant(idx, activeStep)}
            isCurrent={idx === activeStep}
            aria-label={`${step.label}: ${stepVariant(idx, activeStep)}`}
          >
            {step.label}
          </ProgressStep>
        ))}
      </ProgressStepper>
    </Bullseye>
  );
}
