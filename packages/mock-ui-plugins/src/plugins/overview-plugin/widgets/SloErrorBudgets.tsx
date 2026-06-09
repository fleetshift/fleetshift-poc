import {
  ChartDonutUtilization,
  ChartThemeColor,
} from "@patternfly/react-charts/victory";
import { useLiveSlos } from "../useLiveData";
import "./SloErrorBudgets.scss";

export default function SloErrorBudgets(_props: { widgetId: string }) {
  const liveSlos = useLiveSlos();

  return (
    <div className="ome-overview-slo">
      {liveSlos.map((slo) => (
        <div key={slo.service} className="ome-overview-slo__item">
          <div className="ome-overview-slo__chart">
            <ChartDonutUtilization
              data={{ x: "Budget used", y: 100 - slo.budgetRemaining }}
              height={150}
              width={150}
              themeColor={ChartThemeColor.green}
              title=" "
              subTitle=" "
              labels={({ datum }) =>
                datum.x ? `${datum.x}: ${datum.y.toFixed(0)}%` : null
              }
            />
            <div className="ome-overview-slo__overlay">
              <span className="ome-overview-slo__value">
                {Math.round(slo.budgetRemaining)}%
              </span>
              <span className="ome-overview-slo__subtitle">remaining</span>
            </div>
          </div>
          <div className="ome-overview-slo__label">{slo.service}</div>
        </div>
      ))}
    </div>
  );
}
