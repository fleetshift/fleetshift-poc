import { useMemo } from "react";
import { Label, Stack, StackItem } from "@patternfly/react-core";
import {
  CheckCircleIcon,
  ExclamationTriangleIcon,
} from "@patternfly/react-icons";
import { useFleetDataContext } from "../useFleetData";
import "./FleetHealthGrouped.scss";

interface EnvGroup {
  name: string;
  total: number;
  healthy: number;
  degraded: number;
}

export default function FleetHealthGrouped(_props: { widgetId: string }) {
  const { clusters } = useFleetDataContext();

  const groups = useMemo(
    () =>
      Object.values(
        clusters.reduce<Record<string, EnvGroup>>((acc, c) => {
          const env = c.environment;
          if (!acc[env])
            acc[env] = { name: env, total: 0, healthy: 0, degraded: 0 };
          acc[env].total++;
          if (c.status === "healthy") acc[env].healthy++;
          else acc[env].degraded++;
          return acc;
        }, {}),
      ),
    [clusters],
  );

  return (
    <Stack hasGutter className="ome-overview-fleet-grouped-wrap">
      {groups.map((g) => (
        <StackItem key={g.name}>
          <div className="ome-overview-fleet-grouped-row">
            <span className="ome-overview-fleet-grouped-row__name">
              {g.name}
            </span>
            <span className="ome-overview-fleet-grouped-row__stats">
              <Label color="green" icon={<CheckCircleIcon />} isCompact>
                {g.healthy}
              </Label>
              {g.degraded > 0 && (
                <Label
                  color="orange"
                  icon={<ExclamationTriangleIcon />}
                  isCompact
                >
                  {g.degraded}
                </Label>
              )}
              <span className="ome-overview-fleet-grouped-row__total">
                {g.total} total
              </span>
            </span>
          </div>
        </StackItem>
      ))}
    </Stack>
  );
}
