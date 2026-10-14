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
import type { ComponentType } from "react";

export interface OnboardingCardConfig {
  title: string;
  description: string;
  icon: ComponentType;
}

export function createOnboardingCard(config: OnboardingCardConfig) {
  const CardIcon = config.icon;

  return function OnboardingCard({
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
                  <CardIcon />
                </Icon>
              </SplitItem>
              <SplitItem isFilled>{config.title}</SplitItem>
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
            {config.description}
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
  };
}
