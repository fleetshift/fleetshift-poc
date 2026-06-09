import { useMemo } from "react";
import { Label, Stack, StackItem } from "@patternfly/react-core";
import {
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
} from "@patternfly/react-icons";
import { useFleetDataContext } from "../useFleetData";
import type { DashboardCluster } from "../useFleetData";
import "./ClustersAttention.scss";

interface AttentionItem {
  id: string;
  name: string;
  reason: string;
  severity: "danger" | "warning";
}

function deriveAttention(clusters: DashboardCluster[]): AttentionItem[] {
  const items: AttentionItem[] = [];
  for (const c of clusters) {
    if (c.status === "critical") {
      items.push({
        id: c.id,
        name: c.name,
        reason: `Critical — ${c.state}`,
        severity: "danger",
      });
    } else if (c.status === "degraded") {
      items.push({
        id: c.id,
        name: c.name,
        reason: c.reconciling ? "Reconciling" : `Degraded — ${c.state}`,
        severity: "warning",
      });
    }
  }
  return items;
}

export default function ClustersAttention(_props: { widgetId: string }) {
  const { clusters } = useFleetDataContext();
  const attentionClusters = useMemo(
    () => deriveAttention(clusters),
    [clusters],
  );

  if (attentionClusters.length === 0) {
    return (
      <div className="ome-overview-attention-wrap pf-v6-u-color-200 pf-v6-u-text-align-center pf-v6-u-pt-lg">
        All clusters healthy
      </div>
    );
  }

  return (
    <Stack hasGutter className="ome-overview-attention-wrap">
      {attentionClusters.map((c) => (
        <StackItem key={c.id}>
          <div className="ome-overview-attention-row">
            <div className="ome-overview-attention-row__name">{c.name}</div>
            <Label
              color={c.severity === "danger" ? "red" : "orange"}
              icon={
                c.severity === "danger" ? (
                  <ExclamationCircleIcon />
                ) : (
                  <ExclamationTriangleIcon />
                )
              }
              isCompact
            >
              {c.reason}
            </Label>
          </div>
        </StackItem>
      ))}
    </Stack>
  );
}
