import type { ClusterProviderWizardProps } from "@ocm-fleet/common";
import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Wizard,
  WizardStep,
} from "@patternfly/react-core";

export interface ProviderWizardConfig {
  providerName: string;
}

export function createProviderWizard(config: ProviderWizardConfig) {
  return function ProviderWizard({ onClose }: ClusterProviderWizardProps) {
    return (
      <Wizard
        title={`Create ${config.providerName} Cluster`}
        onClose={onClose}
        height={600}
      >
        <WizardStep name="Configuration" id="config">
          <EmptyState headingLevel="h2" titleText="Configuration">
            <EmptyStateBody>
              This example illustrates configuration options for{" "}
              {config.providerName} clusters. It does not configure or provision
              resources.
            </EmptyStateBody>
          </EmptyState>
        </WizardStep>
        <WizardStep name="Networking" id="networking">
          <EmptyState headingLevel="h2" titleText="Networking">
            <EmptyStateBody>
              This example illustrates networking options for{" "}
              {config.providerName} clusters. It does not configure networking
              or provision resources.
            </EmptyStateBody>
          </EmptyState>
        </WizardStep>
        <WizardStep
          name="Review"
          id="review"
          footer={{
            nextButtonText: "Create cluster",
            onNext: onClose,
          }}
        >
          <EmptyState headingLevel="h2" titleText="Review">
            <EmptyStateBody>
              Review your {config.providerName} cluster settings before
              creating.
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="link" onClick={onClose}>
                  Cancel
                </Button>
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </WizardStep>
      </Wizard>
    );
  };
}
