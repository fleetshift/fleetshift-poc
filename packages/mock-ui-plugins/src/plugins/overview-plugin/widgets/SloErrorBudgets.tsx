import {
  ChartDonutUtilization,
  ChartThemeColor,
} from "@patternfly/react-charts/victory";
import { useLiveSlos } from "../useLiveData";
import "./SloErrorBudgets.scss";

export default function SloErrorBudgets(_props: { widgetId: string }) {
  const liveSlos = useLiveSlos();

  return (
    <div className="ov-slo">
      {liveSlos.map((slo) => (
        <div key={slo.service} className="ov-slo__item">
          <div className="ov-slo__chart">
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
            <div className="ov-slo__overlay">
              <span className="ov-slo__value">
                {Math.round(slo.budgetRemaining)}%
              </span>
              <span className="ov-slo__subtitle">remaining</span>
            </div>
          </div>
          <div className="ov-slo__label">{slo.service}</div>
        </div>
      ))}
    </div>
  );
}
