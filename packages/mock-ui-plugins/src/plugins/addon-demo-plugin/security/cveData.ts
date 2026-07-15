export interface CveRow {
  id: string;
  cve: string;
  imagesBySeverity: {
    critical: number;
    important: number;
    moderate: number;
    low: number;
  };
  topCvss: number;
  topNvdCvss: number;
  epssProbability: number;
  firstDiscovered: string;
  summary: string;
}

export const CVE_DATA: CveRow[] = [
  {
    id: "1",
    cve: "CVE-2024-21626",
    imagesBySeverity: { critical: 12, important: 4, moderate: 0, low: 0 },
    topCvss: 8.6,
    topNvdCvss: 8.6,
    epssProbability: 0.042,
    firstDiscovered: "2024-01-31",
    summary:
      "runc container breakout through leaked file descriptor in /sys/fs/cgroup",
  },
  {
    id: "2",
    cve: "CVE-2024-3094",
    imagesBySeverity: { critical: 3, important: 8, moderate: 2, low: 0 },
    topCvss: 10.0,
    topNvdCvss: 10.0,
    epssProbability: 0.087,
    firstDiscovered: "2024-03-29",
    summary:
      "XZ Utils backdoor affecting liblzma resulting in unauthorized SSH access",
  },
  {
    id: "3",
    cve: "CVE-2024-6387",
    imagesBySeverity: { critical: 7, important: 15, moderate: 3, low: 1 },
    topCvss: 8.1,
    topNvdCvss: 8.1,
    epssProbability: 0.031,
    firstDiscovered: "2024-07-01",
    summary:
      "RegreSSHion: RCE in OpenSSH server through signal handler race condition",
  },
  {
    id: "4",
    cve: "CVE-2023-44487",
    imagesBySeverity: { critical: 0, important: 22, moderate: 11, low: 3 },
    topCvss: 7.5,
    topNvdCvss: 7.5,
    epssProbability: 0.72,
    firstDiscovered: "2023-10-10",
    summary: "HTTP/2 Rapid Reset DDoS attack vector affecting web servers",
  },
  {
    id: "5",
    cve: "CVE-2024-32002",
    imagesBySeverity: { critical: 2, important: 6, moderate: 1, low: 0 },
    topCvss: 9.0,
    topNvdCvss: 9.0,
    epssProbability: 0.0052,
    firstDiscovered: "2024-05-14",
    summary:
      "Git RCE via recursive clone with case-insensitive symlink handling",
  },
  {
    id: "6",
    cve: "CVE-2024-4577",
    imagesBySeverity: { critical: 5, important: 0, moderate: 4, low: 2 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.93,
    firstDiscovered: "2024-06-06",
    summary:
      "PHP CGI argument injection allowing remote code execution on Windows",
  },
  {
    id: "7",
    cve: "CVE-2023-50164",
    imagesBySeverity: { critical: 1, important: 3, moderate: 0, low: 0 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.039,
    firstDiscovered: "2023-12-07",
    summary:
      "Apache Struts path traversal to RCE via manipulated file upload parameters",
  },
  {
    id: "8",
    cve: "CVE-2024-23897",
    imagesBySeverity: { critical: 4, important: 1, moderate: 7, low: 0 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.18,
    firstDiscovered: "2024-01-24",
    summary:
      "Jenkins arbitrary file read via CLI command parser argument handling",
  },
  {
    id: "9",
    cve: "CVE-2024-1086",
    imagesBySeverity: { critical: 0, important: 9, moderate: 14, low: 5 },
    topCvss: 7.8,
    topNvdCvss: 7.8,
    epssProbability: 0.0063,
    firstDiscovered: "2024-01-31",
    summary:
      "Linux kernel nf_tables use-after-free allowing local privilege escalation",
  },
  {
    id: "10",
    cve: "CVE-2024-27198",
    imagesBySeverity: { critical: 2, important: 0, moderate: 1, low: 0 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.96,
    firstDiscovered: "2024-03-04",
    summary:
      "JetBrains TeamCity authentication bypass allowing admin account creation",
  },
  {
    id: "11",
    cve: "CVE-2024-0204",
    imagesBySeverity: { critical: 1, important: 2, moderate: 0, low: 0 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.44,
    firstDiscovered: "2024-01-22",
    summary:
      "GoAnywhere MFT authentication bypass via InitialAccountSetup path",
  },
  {
    id: "12",
    cve: "CVE-2023-46747",
    imagesBySeverity: { critical: 6, important: 0, moderate: 2, low: 1 },
    topCvss: 9.8,
    topNvdCvss: 9.8,
    epssProbability: 0.67,
    firstDiscovered: "2023-10-26",
    summary:
      "F5 BIG-IP unauthenticated remote code execution via request smuggling",
  },
];
