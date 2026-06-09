import { type ReactNode } from "react";
import { Link } from "react-router-dom";
import {
  Card,
  CardBody,
  CardTitle,
  Content,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import clsx from "clsx";

export interface WorkloadOption {
  title: string;
  description: string;
  icon: ReactNode;
  badgeVariant: "blue" | "red" | "green" | "purple";
  href?: string;
}

interface WorkloadCardProps {
  workload: WorkloadOption;
}

export default function WorkloadCard({ workload }: WorkloadCardProps) {
  return (
    <Link
      to={workload.href ?? "#"}
      className="ome-day-one-welcome__workload-card"
    >
      <Card isClickable>
        <CardTitle>
          <Stack>
            <StackItem
              className={clsx(
                "ome-day-one-welcome__icon-badge",
                `ome-day-one-welcome__icon-badge--${workload.badgeVariant}`,
                "pf-v6-u-mb-md",
              )}
            >
              {workload.icon}
            </StackItem>
            <StackItem>{workload.title}</StackItem>
          </Stack>
        </CardTitle>
        <CardBody>
          <Content
            component="p"
            className="ome-day-one-welcome__workload-description"
          >
            {workload.description}
          </Content>
        </CardBody>
      </Card>
    </Link>
  );
}
