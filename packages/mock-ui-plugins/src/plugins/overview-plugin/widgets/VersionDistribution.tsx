import { useMemo } from "react";
import { ChartDonut } from "@patternfly/react-charts/victory";
import { useFleetDataContext } from "../useFleetData";
import "./VersionDistribution.scss";

export default function VersionDistribution(_props: { widgetId: string }) {
  const { clusters } = useFleetDataContext();

  const { chartData, legendData } = useMemo(() => {
    const versionCounts = clusters.reduce<Record<string, number>>((acc, c) => {
      const minor = c.version.split(".").slice(0, 2).join(".");
      acc[minor] = (acc[minor] || 0) + 1;
      return acc;
    }, {});

    const data = Object.entries(versionCounts)
      .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true }))
      .map(([version, count]) => ({ x: version, y: count }));

    return {
      chartData: data,
      legendData: data.map((d) => ({ name: `${d.x} (${d.y})` })),
    };
  }, [clusters]);

  return (
    <div className="ome-overview-version-dist">
      <ChartDonut
        data={chartData}
        title={`${clusters.length}`}
        subTitle="clusters"
        labels={({ datum }) => `OCP ${datum.x}: ${datum.y}`}
        legendData={legendData}
        legendOrientation="vertical"
        legendPosition="right"
        height={200}
        width={350}
        padding={{ top: 20, bottom: 20, left: 20, right: 140 }}
      />
    </div>
  );
}
