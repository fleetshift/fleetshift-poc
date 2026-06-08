export interface MockIncident {
  id: string;
  severity: "critical" | "warning" | "info";
  title: string;
  blastRadius: string;
  customerImpact: string;
  duration: string;
  affectedClusters: string[];
}

export interface MockMttrPoint {
  month: string;
  minutes: number;
}

export interface MockCompliance {
  framework: string;
  status: "compliant" | "warning" | "non-compliant";
  lastAudit: string;
}

export const incidents: MockIncident[] = [
  {
    id: "INC-4821",
    severity: "warning",
    title: "Elevated latency on Edge-Site-Chicago",
    blastRadius: "Isolated to Chicago metro area",
    customerImpact: "~2,100 edge subscribers experiencing degraded throughput",
    duration: "14m",
    affectedClusters: ["edge-chicago"],
  },
  {
    id: "INC-4819",
    severity: "info",
    title: "Scheduled certificate rotation — aws-us-west-2",
    blastRadius: "Single cluster, zero downtime",
    customerImpact: "0 users affected",
    duration: "Scheduled",
    affectedClusters: ["ocp-aws-west2"],
  },
];

export const mttrHistory: MockMttrPoint[] = [
  { month: "Jan", minutes: 240 },
  { month: "Feb", minutes: 195 },
  { month: "Mar", minutes: 142 },
  { month: "Apr", minutes: 98 },
  { month: "May", minutes: 62 },
  { month: "Jun", minutes: 45 },
];

export const complianceStatuses: MockCompliance[] = [
  { framework: "SOC 2 Type II", status: "compliant", lastAudit: "2026-04-15" },
  { framework: "PCI-DSS v4.0", status: "compliant", lastAudit: "2026-03-22" },
  {
    framework: "HIPAA",
    status: "compliant",
    lastAudit: "2026-05-01",
  },
];
