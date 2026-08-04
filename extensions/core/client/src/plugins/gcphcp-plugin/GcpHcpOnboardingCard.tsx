import type { OnboardingActionCardProps } from "@fleetshift/common";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  Icon,
  Split,
  SplitItem,
} from "@patternfly/react-core";
import { CheckCircleIcon } from "@patternfly/react-icons";

import gcpLogo from "./assets/gcp-icon.png";

export default function GcpHcpOnboardingCard({
  completed,
  onConfigure,
}: OnboardingActionCardProps) {
  return (
    <Card isCompact>
      <CardHeader>
        <CardTitle>
          <Split hasGutter>
            <SplitItem>
              <Icon size="xl">
                <img src={gcpLogo} alt="GCP Logo" width="100%" height="100%" />
              </Icon>
            </SplitItem>
            <SplitItem isFilled>Connect to GCP Hosted Control Plane</SplitItem>
            {completed && (
              <SplitItem>
                <Icon status="success">
                  <CheckCircleIcon />
                </Icon>
              </SplitItem>
            )}
          </Split>
        </CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="pf-v6-u-mb-md">
          Connect your GCP project to create managed HCP clusters.
        </Content>
        {completed ? (
          <Content component="small">Connected</Content>
        ) : (
          <Button variant="secondary" onClick={onConfigure}>
            Configure
          </Button>
        )}
      </CardBody>
    </Card>
  );
}
