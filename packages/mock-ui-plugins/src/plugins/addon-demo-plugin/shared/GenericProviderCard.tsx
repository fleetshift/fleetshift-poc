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
import type { ComponentType } from "react";

export interface ProviderCardConfig {
  label: string;
  description: string;
  icon: ComponentType;
  ariaLabel: string;
}

export function createProviderCard(config: ProviderCardConfig) {
  const ProviderIcon = config.icon;

  return function ProviderCard({ onSelect }: ClusterProviderCardProps) {
    return (
      <Card isClickable isCompact>
        <CardHeader
          selectableActions={{
            onClickAction: onSelect,
            selectableActionAriaLabel: config.ariaLabel,
          }}
        >
          <CardTitle>
            <Split hasGutter>
              <SplitItem>
                <Icon size="xl">
                  <ProviderIcon />
                </Icon>
              </SplitItem>
              <SplitItem isFilled>{config.label}</SplitItem>
            </Split>
          </CardTitle>
        </CardHeader>
        <CardBody>
          <Content component="p">{config.description}</Content>
        </CardBody>
      </Card>
    );
  };
}
