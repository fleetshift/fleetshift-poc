import { Stack, StackItem } from "@patternfly/react-core";
import { CheckCircleIcon } from "@patternfly/react-icons";
import { complianceStatuses } from "../mockData";
import "./ComplianceStatus.scss";

export default function ComplianceStatus(_props: { widgetId: string }) {
  return (
    <div className="ov-compliance-wrap">
      <Stack hasGutter>
        {complianceStatuses.map((c) => (
          <StackItem key={c.framework}>
            <div className="ov-compliance-row">
              <CheckCircleIcon color="var(--pf-t--global--color--status--success--default)" />
              <span className="ov-compliance-row__framework">
                {c.framework}
              </span>
              <span className="ov-compliance-row__audit">
                Last audit: {c.lastAudit}
              </span>
            </div>
          </StackItem>
        ))}
      </Stack>
    </div>
  );
}
