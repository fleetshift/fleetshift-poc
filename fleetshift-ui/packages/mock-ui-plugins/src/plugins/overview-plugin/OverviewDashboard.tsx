import "./overview-dashboard.scss";
import "@patternfly/widgetized-dashboard/dist/esm/styles.css";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

import {
  BundleIcon,
  ChartLineIcon,
  CogIcon,
  ExclamationTriangleIcon,
  GlobeIcon,
  MonitoringIcon,
  SecurityIcon,
  ShieldAltIcon,
  TachometerAltIcon,
} from "@patternfly/react-icons";
import { WidgetLayout, WidgetMapping } from "@patternfly/widgetized-dashboard";

import { defaultTemplate, loadTemplate, saveTemplate } from "./dashboardLayout";
import SetupChecklist from "./SetupChecklist";
import { FleetDataContext, useFleetData } from "./useFleetData";
import ActiveIncidents from "./widgets/ActiveIncidents";
import ClustersAttention from "./widgets/ClustersAttention";
import ComplianceStatus from "./widgets/ComplianceStatus";
import FleetCapacity from "./widgets/FleetCapacity";
import FleetHealthGrouped from "./widgets/FleetHealthGrouped";
import GlobalMap from "./widgets/GlobalMap";
import MttrTrend from "./widgets/MttrTrend";
import SloErrorBudgets from "./widgets/SloErrorBudgets";
import VersionDistribution from "./widgets/VersionDistribution";

const widgetMapping: WidgetMapping = {
  "slo-error-budgets": {
    defaults: { w: 2, h: 3, maxH: 5, minH: 2 },
    config: { title: "SLO Error Budgets", icon: <TachometerAltIcon /> },
    renderWidget: (id) => <SloErrorBudgets widgetId={id} />,
  },
  "fleet-health-grouped": {
    defaults: { w: 1, h: 3, maxH: 5, minH: 2 },
    config: { title: "Fleet Health by Group", icon: <CogIcon /> },
    renderWidget: (id) => <FleetHealthGrouped widgetId={id} />,
  },
  "compliance-status": {
    defaults: { w: 1, h: 2, maxH: 4, minH: 2 },
    config: { title: "Compliance Status", icon: <SecurityIcon /> },
    renderWidget: (id) => <ComplianceStatus widgetId={id} />,
  },
  "global-map": {
    defaults: { w: 2, h: 3, maxH: 6, minH: 2 },
    config: { title: "Global Cluster Map", icon: <GlobeIcon /> },
    renderWidget: (id) => <GlobalMap widgetId={id} />,
  },
  "version-distribution": {
    defaults: { w: 2, h: 3, maxH: 5, minH: 2 },
    config: { title: "Version Distribution", icon: <BundleIcon /> },
    renderWidget: (id) => <VersionDistribution widgetId={id} />,
  },
  "active-incidents": {
    defaults: { w: 2, h: 3, maxH: 6, minH: 2 },
    config: { title: "Active Incidents", icon: <ShieldAltIcon /> },
    renderWidget: (id) => <ActiveIncidents widgetId={id} />,
  },
  "clusters-attention": {
    defaults: { w: 2, h: 3, maxH: 6, minH: 2 },
    config: {
      title: "Clusters Needing Attention",
      icon: <ExclamationTriangleIcon />,
    },
    renderWidget: (id) => <ClustersAttention widgetId={id} />,
  },
  "fleet-capacity": {
    defaults: { w: 2, h: 3, maxH: 6, minH: 2 },
    config: { title: "Fleet Capacity", icon: <MonitoringIcon /> },
    renderWidget: (id) => <FleetCapacity widgetId={id} />,
  },
  "mttr-trend": {
    defaults: { w: 2, h: 3, maxH: 5, minH: 2 },
    config: { title: "Mean Time to Resolution", icon: <ChartLineIcon /> },
    renderWidget: (id) => <MttrTrend widgetId={id} />,
  },
};

export default function OverviewDashboard() {
  const initialTemplate = loadTemplate() ?? defaultTemplate;
  const fleetData = useFleetData();
  return (
    <FleetDataContext.Provider value={fleetData}>
      <SetupChecklist />
      <WidgetLayout
        widgetMapping={widgetMapping}
        initialTemplate={initialTemplate}
        onTemplateChange={saveTemplate}
        showDrawer={false}
        isLayoutLocked={false}
      />
    </FleetDataContext.Provider>
  );
}
