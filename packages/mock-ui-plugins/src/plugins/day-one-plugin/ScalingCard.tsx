import {
  Button,
  Card,
  CardBody,
  Content,
  Icon,
  Split,
  SplitItem,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import NetworkWiredIcon from "@patternfly/react-icons/dist/dynamic/icons/network-wired-icon";

interface ScalingCardProps {
  onSetup?: () => void;
}

export default function ScalingCard({ onSetup }: ScalingCardProps) {
  return (
    <Card>
      <CardBody>
        <Split>
          <SplitItem className="pf-v6-u-mr-md">
            <span className="ome-day-one-welcome__icon-badge ome-day-one-welcome__icon-badge--blue">
              <Icon size="xl">
                <NetworkWiredIcon />
              </Icon>
            </span>
          </SplitItem>
          <SplitItem isFilled>
            <Stack hasGutter>
              <StackItem>
                <Title headingLevel="h3">
                  OpenShift Management Engine Cluster
                </Title>
              </StackItem>
              <StackItem>
                <Content component="p">
                  Offload your management engine and its add-ons to a dedicated,
                  high-performance cluster as your fleet grows.
                </Content>
              </StackItem>
              <StackItem>
                <Button variant="primary" onClick={onSetup}>
                  Setup a cluster for management
                </Button>
              </StackItem>
            </Stack>
          </SplitItem>
        </Split>
      </CardBody>
    </Card>
  );
}
