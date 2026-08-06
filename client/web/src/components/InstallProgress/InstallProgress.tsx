import "./InstallProgress.scss";

import { ProgressStep, ProgressStepper } from "@patternfly/react-core";
import { useEffect, useRef, useState } from "react";

interface Step {
  label: string;
  delay: number;
}

const STEPS: Step[] = [
  { label: "Fetching manifest", delay: 800 },
  { label: "Installing assets", delay: 1200 },
  { label: "Validating availability", delay: 900 },
  { label: "Installed", delay: 600 },
];

interface InstallProgressProps {
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

const InstallProgress = ({ onComplete }: InstallProgressProps) => {
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
    <div className="ome-install-progress">
      <ProgressStepper isVertical aria-label="Extension install progress">
        {STEPS.map((step, idx) => (
          <ProgressStep
            key={step.label}
            id={`install-step-${idx}`}
            titleId={`install-step-title-${idx}`}
            variant={stepVariant(idx, activeStep)}
            isCurrent={idx === activeStep}
            aria-label={`${step.label}: ${stepVariant(idx, activeStep)}`}
          >
            {step.label}
          </ProgressStep>
        ))}
      </ProgressStepper>
    </div>
  );
};

export default InstallProgress;
