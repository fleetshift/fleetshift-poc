import "./ActiveIncidents.scss";

import {
  Card,
  CardBody,
  Label,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import {
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
  InfoCircleIcon,
} from "@patternfly/react-icons";

import { incidents } from "../mockData";

const severityIcon = (s: "critical" | "warning" | "info") => {
  switch (s) {
    case "critical":
      return (
        <ExclamationCircleIcon color="var(--pf-t--global--color--status--danger--default)" />
      );
    case "warning":
      return (
        <ExclamationTriangleIcon color="var(--pf-t--global--color--status--warning--default)" />
      );
    case "info":
      return (
        <InfoCircleIcon color="var(--pf-t--global--color--status--info--default)" />
      );
  }
};

const severityColor = (s: "critical" | "warning" | "info") =>
  s === "critical" ? "red" : s === "warning" ? "orange" : "blue";

export default function ActiveIncidents(_props: { widgetId: string }) {
  return (
    <Stack hasGutter className="pf-v6-u-p-md">
      {incidents.map((inc) => (
        <StackItem key={inc.id}>
          <Card isCompact>
            <CardBody>
              <Stack hasGutter>
                <StackItem>
                  <span className="ome-overview-incident-header">
                    {severityIcon(inc.severity)}
                    <strong>{inc.id}</strong>
                    <span>{inc.title}</span>
                    <Label color={severityColor(inc.severity)} isCompact>
                      {inc.duration}
                    </Label>
                  </span>
                </StackItem>
                <StackItem>
                  <span className="ome-overview-incident-badges">
                    <Label color="purple" isCompact>
                      Blast: {inc.blastRadius}
                    </Label>
                    <Label color="orange" isCompact>
                      Impact: {inc.customerImpact}
                    </Label>
                  </span>
                </StackItem>
              </Stack>
            </CardBody>
          </Card>
        </StackItem>
      ))}
    </Stack>
  );
}
