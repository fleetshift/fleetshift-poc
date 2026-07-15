import type { OnboardingActionFormProps } from "@fleetshift/common";
import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
} from "@patternfly/react-core";

export interface ModuleFormConfig {
  moduleLabel: string;
}

export function createModuleOnboardingForm(config: ModuleFormConfig) {
  return function ModuleForm({
    onComplete,
    onCancel,
  }: OnboardingActionFormProps) {
    return (
      <div className="ome-setup-whats-next__body">
        <div className="ome-setup-whats-next__inner">
          <EmptyState
            headingLevel="h1"
            titleText={`Enable ${config.moduleLabel}`}
          >
            <EmptyStateBody>
              Click &quot;Enable&quot; to activate the {config.moduleLabel}{" "}
              extension for your fleet.
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="primary" onClick={onComplete}>
                  Enable
                </Button>
                <Button variant="link" onClick={onCancel}>
                  Cancel
                </Button>
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </div>
      </div>
    );
  };
}
