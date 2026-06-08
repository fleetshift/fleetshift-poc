import { useMemo } from "react";
import {
  Progress,
  ProgressMeasureLocation,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import { useFleetDataContext } from "../useFleetData";
import "./FleetCapacity.scss";

const variant = (pct: number) =>
  pct >= 80 ? "danger" : pct >= 65 ? "warning" : undefined;

interface CapacityGroup {
  environment: string;
  cpuPercent: number;
  memoryPercent: number;
}

export default function FleetCapacity(_props: { widgetId: string }) {
  const { clusters } = useFleetDataContext();

  const groups = useMemo(() => {
    const acc: Record<string, { cpu: number[]; mem: number[] }> = {};
    for (const c of clusters) {
      if (!acc[c.environment]) acc[c.environment] = { cpu: [], mem: [] };
      acc[c.environment].cpu.push(c.cpuPercent);
      acc[c.environment].mem.push(c.memoryPercent);
    }
    const result: CapacityGroup[] = [];
    for (const [env, vals] of Object.entries(acc)) {
      const avgCpu = Math.round(
        vals.cpu.reduce((s, v) => s + v, 0) / vals.cpu.length,
      );
      const avgMem = Math.round(
        vals.mem.reduce((s, v) => s + v, 0) / vals.mem.length,
      );
      result.push({
        environment: env,
        cpuPercent: avgCpu,
        memoryPercent: avgMem,
      });
    }
    return result;
  }, [clusters]);

  return (
    <Stack hasGutter className="ov-capacity-wrap pf-v6-u-pb-lg">
      {groups.map((g) => (
        <StackItem key={g.environment}>
          <div className="ov-capacity-row__name">{g.environment}</div>
          <div className="ov-capacity-row__bars">
            <Progress
              value={g.cpuPercent}
              title="CPU"
              size="sm"
              measureLocation={ProgressMeasureLocation.inside}
              variant={variant(g.cpuPercent)}
            />
            <Progress
              value={g.memoryPercent}
              title="Memory"
              size="sm"
              measureLocation={ProgressMeasureLocation.inside}
              variant={variant(g.memoryPercent)}
            />
          </div>
        </StackItem>
      ))}
    </Stack>
  );
}
