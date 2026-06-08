import "./overview-dashboard.scss";
import MttrTrend from "./widgets/MttrTrend";
import ActiveIncidents from "./widgets/ActiveIncidents";
import FleetHealthGrouped from "./widgets/FleetHealthGrouped";
import GlobalMap from "./widgets/GlobalMap";
import ComplianceStatus from "./widgets/ComplianceStatus";
import VersionDistribution from "./widgets/VersionDistribution";
import ClustersAttention from "./widgets/ClustersAttention";
import FleetCapacity from "./widgets/FleetCapacity";
import {
  WidgetLayout,
  WidgetMapping,
  type ExtendedTemplateConfig,
} from "@patternfly/widgetized-dashboard";
import "@patternfly/widgetized-dashboard/dist/esm/styles.css";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";
import SloErrorBudgets from "./widgets/SloErrorBudgets";
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
import { FleetDataContext, useFleetData } from "./useFleetData";

const LAYOUT_VERSION = 8;
const STORAGE_KEY = "fleetshift:dashboard-layout";
const VERSION_KEY = "fleetshift:dashboard-layout-version";

function loadTemplate(): ExtendedTemplateConfig | null {
  try {
    const ver = localStorage.getItem(VERSION_KEY);
    if (Number(ver) !== LAYOUT_VERSION) return null;
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function saveTemplate(template: ExtendedTemplateConfig) {
  localStorage.setItem(VERSION_KEY, String(LAYOUT_VERSION));
  localStorage.setItem(STORAGE_KEY, JSON.stringify(template));
}

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

// Grid columns: xl=4, lg=3, md=2, sm=1
// rowHeight = 56px
const defaultTemplate: ExtendedTemplateConfig = {
  xl: [
    {
      i: "slo-error-budgets#1",
      x: 0,
      y: 0,
      w: 2,
      h: 6,
      widgetType: "slo-error-budgets",
      title: "SLO Error Budgets",
    },
    {
      i: "fleet-health-grouped#1",
      x: 2,
      y: 0,
      w: 1,
      h: 6,
      widgetType: "fleet-health-grouped",
      title: "Fleet Health by Group",
    },
    {
      i: "compliance-status#1",
      x: 3,
      y: 0,
      w: 1,
      h: 6,
      widgetType: "compliance-status",
      title: "Compliance Status",
    },
    {
      i: "global-map#1",
      x: 0,
      y: 6,
      w: 2,
      h: 6,
      widgetType: "global-map",
      title: "Global Cluster Map",
    },
    {
      i: "version-distribution#1",
      x: 2,
      y: 6,
      w: 2,
      h: 6,
      widgetType: "version-distribution",
      title: "Version Distribution",
    },
    {
      i: "fleet-capacity#1",
      x: 0,
      y: 12,
      w: 2,
      h: 5,
      widgetType: "fleet-capacity",
      title: "Fleet Capacity",
    },
    {
      i: "active-incidents#1",
      x: 2,
      y: 12,
      w: 2,
      h: 5,
      widgetType: "active-incidents",
      title: "Active Incidents",
    },
    {
      i: "clusters-attention#1",
      x: 0,
      y: 17,
      w: 2,
      h: 4,
      widgetType: "clusters-attention",
      title: "Clusters Needing Attention",
    },
    {
      i: "mttr-trend#1",
      x: 2,
      y: 17,
      w: 2,
      h: 4,
      widgetType: "mttr-trend",
      title: "Mean Time to Resolution",
    },
  ],
  lg: [
    {
      i: "slo-error-budgets#1",
      x: 0,
      y: 0,
      w: 2,
      h: 6,
      widgetType: "slo-error-budgets",
      title: "SLO Error Budgets",
    },
    {
      i: "fleet-health-grouped#1",
      x: 2,
      y: 0,
      w: 1,
      h: 6,
      widgetType: "fleet-health-grouped",
      title: "Fleet Health by Group",
    },
    {
      i: "global-map#1",
      x: 0,
      y: 6,
      w: 2,
      h: 6,
      widgetType: "global-map",
      title: "Global Cluster Map",
    },
    {
      i: "version-distribution#1",
      x: 2,
      y: 6,
      w: 1,
      h: 6,
      widgetType: "version-distribution",
      title: "Version Distribution",
    },
    {
      i: "compliance-status#1",
      x: 0,
      y: 12,
      w: 1,
      h: 4,
      widgetType: "compliance-status",
      title: "Compliance Status",
    },
    {
      i: "active-incidents#1",
      x: 1,
      y: 12,
      w: 2,
      h: 4,
      widgetType: "active-incidents",
      title: "Active Incidents",
    },
    {
      i: "fleet-capacity#1",
      x: 0,
      y: 16,
      w: 2,
      h: 5,
      widgetType: "fleet-capacity",
      title: "Fleet Capacity",
    },
    {
      i: "clusters-attention#1",
      x: 2,
      y: 16,
      w: 1,
      h: 5,
      widgetType: "clusters-attention",
      title: "Clusters Needing Attention",
    },
    {
      i: "mttr-trend#1",
      x: 0,
      y: 21,
      w: 3,
      h: 5,
      widgetType: "mttr-trend",
      title: "Mean Time to Resolution",
    },
  ],
  md: [
    {
      i: "slo-error-budgets#1",
      x: 0,
      y: 0,
      w: 2,
      h: 6,
      widgetType: "slo-error-budgets",
      title: "SLO Error Budgets",
    },
    {
      i: "fleet-health-grouped#1",
      x: 0,
      y: 6,
      w: 1,
      h: 5,
      widgetType: "fleet-health-grouped",
      title: "Fleet Health by Group",
    },
    {
      i: "compliance-status#1",
      x: 1,
      y: 6,
      w: 1,
      h: 5,
      widgetType: "compliance-status",
      title: "Compliance Status",
    },
    {
      i: "global-map#1",
      x: 0,
      y: 11,
      w: 2,
      h: 6,
      widgetType: "global-map",
      title: "Global Cluster Map",
    },
    {
      i: "version-distribution#1",
      x: 0,
      y: 17,
      w: 2,
      h: 5,
      widgetType: "version-distribution",
      title: "Version Distribution",
    },
    {
      i: "fleet-capacity#1",
      x: 0,
      y: 22,
      w: 2,
      h: 5,
      widgetType: "fleet-capacity",
      title: "Fleet Capacity",
    },
    {
      i: "active-incidents#1",
      x: 0,
      y: 27,
      w: 2,
      h: 4,
      widgetType: "active-incidents",
      title: "Active Incidents",
    },
    {
      i: "clusters-attention#1",
      x: 0,
      y: 31,
      w: 2,
      h: 5,
      widgetType: "clusters-attention",
      title: "Clusters Needing Attention",
    },
    {
      i: "mttr-trend#1",
      x: 0,
      y: 36,
      w: 2,
      h: 5,
      widgetType: "mttr-trend",
      title: "Mean Time to Resolution",
    },
  ],
  sm: [
    {
      i: "slo-error-budgets#1",
      x: 0,
      y: 0,
      w: 1,
      h: 5,
      widgetType: "slo-error-budgets",
      title: "SLO Error Budgets",
    },
    {
      i: "fleet-health-grouped#1",
      x: 0,
      y: 5,
      w: 1,
      h: 5,
      widgetType: "fleet-health-grouped",
      title: "Fleet Health by Group",
    },
    {
      i: "compliance-status#1",
      x: 0,
      y: 10,
      w: 1,
      h: 3,
      widgetType: "compliance-status",
      title: "Compliance Status",
    },
    {
      i: "global-map#1",
      x: 0,
      y: 13,
      w: 1,
      h: 6,
      widgetType: "global-map",
      title: "Global Cluster Map",
    },
    {
      i: "version-distribution#1",
      x: 0,
      y: 19,
      w: 1,
      h: 5,
      widgetType: "version-distribution",
      title: "Version Distribution",
    },
    {
      i: "fleet-capacity#1",
      x: 0,
      y: 24,
      w: 1,
      h: 5,
      widgetType: "fleet-capacity",
      title: "Fleet Capacity",
    },
    {
      i: "active-incidents#1",
      x: 0,
      y: 29,
      w: 1,
      h: 4,
      widgetType: "active-incidents",
      title: "Active Incidents",
    },
    {
      i: "clusters-attention#1",
      x: 0,
      y: 33,
      w: 1,
      h: 5,
      widgetType: "clusters-attention",
      title: "Clusters Needing Attention",
    },
    {
      i: "mttr-trend#1",
      x: 0,
      y: 38,
      w: 1,
      h: 5,
      widgetType: "mttr-trend",
      title: "Mean Time to Resolution",
    },
  ],
};

export default function OverviewDashboard() {
  const initialTemplate = loadTemplate() ?? defaultTemplate;
  const fleetData = useFleetData();
  return (
    <FleetDataContext.Provider value={fleetData}>
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
