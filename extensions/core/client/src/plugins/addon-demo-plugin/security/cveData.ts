export type CveSeverity = "critical" | "important" | "moderate" | "low";
export type CveStatus = "new" | "deferred" | "false-positive";

export interface CveRow {
  id: string;
  cve: string;
  severity: CveSeverity;
  status: CveStatus;
  imagesBySeverity: {
    critical: number;
    important: number;
    moderate: number;
    low: number;
  };
  topCvss: number;
  topCvssVersion: string;
  topNvdCvss: number | null;
  topNvdCvssVersion: string;
  epssProbability: number;
  firstDiscovered: string;
  summary: string;
}

export const CVE_DATA: CveRow[] = [
  {
    id: "1",
    cve: "CVE-2026-39830",
    severity: "critical",
    status: "new",
    imagesBySeverity: { critical: 81, important: 0, moderate: 0, low: 0 },
    topCvss: 9.1,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00533,
    firstDiscovered: "1 month ago",
    summary: "Heap buffer overflow in gRPC C++ allows remote code execution",
  },
  {
    id: "2",
    cve: "CVE-2026-46595",
    severity: "critical",
    status: "new",
    imagesBySeverity: { critical: 81, important: 0, moderate: 0, low: 0 },
    topCvss: 9.9,
    topCvssVersion: "V3",
    topNvdCvss: 9.9,
    topNvdCvssVersion: "V3",
    epssProbability: 0.0044,
    firstDiscovered: "18 days ago",
    summary:
      "Use-after-free in container runtime allows container escape to host",
  },
  {
    id: "3",
    cve: "CVE-2026-46594",
    severity: "critical",
    status: "new",
    imagesBySeverity: { critical: 81, important: 0, moderate: 0, low: 0 },
    topCvss: 10.0,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00429,
    firstDiscovered: "1 day ago",
    summary: "Critical authentication bypass in API gateway proxy handler",
  },
  {
    id: "4",
    cve: "CVE-2026-46593",
    severity: "critical",
    status: "deferred",
    imagesBySeverity: { critical: 81, important: 0, moderate: 0, low: 0 },
    topCvss: 9.8,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00412,
    firstDiscovered: "1 day ago",
    summary: "Path traversal in S3-compatible object storage allows data leak",
  },
  {
    id: "5",
    cve: "CVE-2026-46592",
    severity: "critical",
    status: "new",
    imagesBySeverity: { critical: 81, important: 0, moderate: 0, low: 0 },
    topCvss: 9.7,
    topCvssVersion: "V3",
    topNvdCvss: 9.7,
    topNvdCvssVersion: "V3",
    epssProbability: 0.00398,
    firstDiscovered: "2 days ago",
    summary:
      "Privilege escalation in kernel namespace handling allows root access",
  },
  {
    id: "6",
    cve: "CVE-2026-46591",
    severity: "critical",
    status: "new",
    imagesBySeverity: { critical: 74, important: 12, moderate: 0, low: 0 },
    topCvss: 9.5,
    topCvssVersion: "V3",
    topNvdCvss: 9.5,
    topNvdCvssVersion: "V3",
    epssProbability: 0.00387,
    firstDiscovered: "3 days ago",
    summary: "Remote code execution via deserialization in Java message broker",
  },
  {
    id: "7",
    cve: "CVE-2026-46590",
    severity: "important",
    status: "new",
    imagesBySeverity: { critical: 68, important: 15, moderate: 3, low: 0 },
    topCvss: 9.4,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00375,
    firstDiscovered: "4 days ago",
    summary: "SQL injection in ORM query builder allows data exfiltration",
  },
  {
    id: "8",
    cve: "CVE-2026-46589",
    severity: "important",
    status: "deferred",
    imagesBySeverity: { critical: 62, important: 20, moderate: 5, low: 0 },
    topCvss: 9.3,
    topCvssVersion: "V3",
    topNvdCvss: 9.3,
    topNvdCvssVersion: "V3",
    epssProbability: 0.00361,
    firstDiscovered: "5 days ago",
    summary: "Memory corruption in TLS handshake allows man-in-the-middle",
  },
  {
    id: "9",
    cve: "CVE-2026-46588",
    severity: "important",
    status: "new",
    imagesBySeverity: { critical: 55, important: 22, moderate: 8, low: 1 },
    topCvss: 9.1,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00348,
    firstDiscovered: "6 days ago",
    summary: "SSRF in webhook handler allows internal network scanning",
  },
  {
    id: "10",
    cve: "CVE-2026-46587",
    severity: "moderate",
    status: "false-positive",
    imagesBySeverity: { critical: 49, important: 18, moderate: 10, low: 2 },
    topCvss: 8.9,
    topCvssVersion: "V3",
    topNvdCvss: 8.9,
    topNvdCvssVersion: "V3",
    epssProbability: 0.00334,
    firstDiscovered: "1 week ago",
    summary: "Improper certificate validation in mutual TLS implementation",
  },
  {
    id: "11",
    cve: "CVE-2026-46586",
    severity: "moderate",
    status: "new",
    imagesBySeverity: { critical: 42, important: 25, moderate: 6, low: 3 },
    topCvss: 8.8,
    topCvssVersion: "V3",
    topNvdCvss: null,
    topNvdCvssVersion: "",
    epssProbability: 0.00321,
    firstDiscovered: "1 week ago",
    summary: "Integer overflow in image processing library allows DoS",
  },
  {
    id: "12",
    cve: "CVE-2026-46585",
    severity: "low",
    status: "deferred",
    imagesBySeverity: { critical: 38, important: 28, moderate: 12, low: 4 },
    topCvss: 8.6,
    topCvssVersion: "V3",
    topNvdCvss: 8.6,
    topNvdCvssVersion: "V3",
    epssProbability: 0.00307,
    firstDiscovered: "2 weeks ago",
    summary: "Cross-site scripting in admin dashboard template rendering",
  },
];

export const TOTAL_CVE_COUNT = 1767;
export const TOTAL_IMAGE_COUNT = 174;
export const TOTAL_DEPLOYMENT_COUNT = 245;
