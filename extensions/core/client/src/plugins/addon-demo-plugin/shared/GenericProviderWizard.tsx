import type { ClusterProviderWizardProps } from "@fleetshift/common";
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
        title={`${config.providerName} cluster demo`}
        onClose={onClose}
        height={600}
      >
        <WizardStep name="Configuration" id="config">
          <EmptyState headingLevel="h2" titleText="Configuration">
            <EmptyStateBody>
              This demonstrates {config.providerName} cluster configuration.
              Provisioning is not implemented.
            </EmptyStateBody>
          </EmptyState>
        </WizardStep>
        <WizardStep name="Networking" id="networking">
          <EmptyState headingLevel="h2" titleText="Networking">
            <EmptyStateBody>
              Network settings for {config.providerName} clusters are shown as a
              placeholder.
            </EmptyStateBody>
          </EmptyState>
        </WizardStep>
        <WizardStep
          name="Review"
          id="review"
          footer={{
            nextButtonText: "Close demo",
            onNext: onClose,
          }}
        >
          <EmptyState headingLevel="h2" titleText="Review">
            <EmptyStateBody>
              This {config.providerName} demonstration does not create a
              cluster.
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
