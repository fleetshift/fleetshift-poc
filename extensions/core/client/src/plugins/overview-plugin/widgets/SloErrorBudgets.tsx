import "./SloErrorBudgets.scss";

import {
  ChartDonutUtilization,
  ChartThemeColor,
} from "@patternfly/react-charts/victory";

import { useLiveSlos } from "../useLiveData";

export default function SloErrorBudgets(_props: { widgetId: string }) {
  const liveSlos = useLiveSlos();

  return (
    <div className="ocm-rm-overview-slo">
      {liveSlos.map((slo) => (
        <div key={slo.service} className="ocm-rm-overview-slo__item">
          <div className="ocm-rm-overview-slo__chart">
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
            <div className="ocm-rm-overview-slo__overlay">
              <span className="ocm-rm-overview-slo__value">
                {Math.round(slo.budgetRemaining)}%
              </span>
              <span className="ocm-rm-overview-slo__subtitle">remaining</span>
            </div>
          </div>
          <div className="ocm-rm-overview-slo__label">{slo.service}</div>
        </div>
      ))}
    </div>
  );
}
