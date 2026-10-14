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

import kindLogo from "./assets/kind-logo.png";

export function KindIcon() {
  return <img src={kindLogo} alt="Kind Logo" />;
}

export default function KindProviderCard({
  onSelect,
}: ClusterProviderCardProps) {
  return (
    <Card isClickable isCompact>
      <CardHeader
        selectableActions={{
          onClickAction: onSelect,
          selectableActionAriaLabel: "Select Kind provider",
        }}
      >
        <CardTitle>
          <Split hasGutter>
            <SplitItem>
              <Icon size="xl">
                <KindIcon />
              </Icon>
            </SplitItem>
            <SplitItem isFilled>Kind</SplitItem>
          </Split>
        </CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p">
          Create a local Kind cluster for development and testing.
        </Content>
      </CardBody>
    </Card>
  );
}
