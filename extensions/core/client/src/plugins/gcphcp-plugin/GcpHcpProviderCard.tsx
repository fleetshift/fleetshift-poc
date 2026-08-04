import type { ClusterProviderCardProps } from "@fleetshift/common";
import {
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  Icon,
  Split,
  SplitItem,
} from "@patternfly/react-core";

import gcpLogo from "./assets/gcp-icon.png";

export function GcpHcpIcon() {
  return <img src={gcpLogo} alt="GCP Logo" />;
}

export default function GcpHcpProviderCard({
  onSelect,
}: ClusterProviderCardProps) {
  return (
    <Card isClickable isCompact>
      <CardHeader
        selectableActions={{
          onClickAction: onSelect,
          selectableActionAriaLabel: "Select GCP Hosted Control Plane provider",
        }}
      >
        <CardTitle>
          <Split hasGutter>
            <SplitItem>
              <Icon size="xl">
                <GcpHcpIcon />
              </Icon>
            </SplitItem>
            <SplitItem isFilled>GCP Hosted Control Plane</SplitItem>
          </Split>
        </CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p">
          Create a managed OpenShift cluster on Google Cloud Platform.
        </Content>
      </CardBody>
    </Card>
  );
}
