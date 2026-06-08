import { createContext, useContext, useEffect, useState } from "react";
import type { GcpHcpCluster } from "../gcphcp-plugin/api";
import { listGcpHcpClusters, extractClusterId } from "../gcphcp-plugin/api";

export interface DashboardCluster {
  id: string;
  name: string;
  provider: string;
  region: string;
  environment: string;
  status: "healthy" | "degraded" | "critical";
  version: string;
  state: string;
  reconciling: boolean;
  lat: number;
  lng: number;
  cpuPercent: number;
  memoryPercent: number;
}

export interface FleetData {
  clusters: DashboardCluster[];
  loading: boolean;
  error: string | null;
}

const STORAGE_KEY = "fleetshift:cluster-enrichment";

interface EnrichmentData {
  region: string;
  lat: number;
  lng: number;
  environment: string;
  cpuPercent: number;
  memoryPercent: number;
}

const GEO_REGIONS: { region: string; lat: number; lng: number }[] = [
  { region: "US-Central (Iowa)", lat: 41.9, lng: -93.1 },
  { region: "US-East (South Carolina)", lat: 33.8, lng: -81.1 },
  { region: "US-East (Virginia)", lat: 39.0, lng: -77.5 },
  { region: "US-West (Oregon)", lat: 45.6, lng: -121.2 },
  { region: "US-West (Los Angeles)", lat: 34.1, lng: -118.2 },
  { region: "EU-West (Belgium)", lat: 50.4, lng: 3.7 },
  { region: "EU-Central (Frankfurt)", lat: 50.1, lng: 8.7 },
  { region: "EU-West (Netherlands)", lat: 53.4, lng: 6.6 },
  { region: "APAC (Taiwan)", lat: 23.7, lng: 121.0 },
  { region: "APAC (Tokyo)", lat: 35.7, lng: 139.7 },
  { region: "APAC (Singapore)", lat: 1.3, lng: 103.8 },
  { region: "APAC (Sydney)", lat: -33.9, lng: 151.2 },
];

const REGION_KEYWORDS: Record<string, number> = {
  "us-central1": 0,
  "us-east1": 1,
  "us-east4": 2,
  "us-west1": 3,
  "us-west2": 4,
  "europe-west1": 5,
  "europe-west3": 6,
  "europe-west4": 7,
  "asia-east1": 8,
  "asia-northeast1": 9,
  "asia-southeast1": 10,
  "australia-southeast1": 11,
};

const ENVIRONMENTS = ["Production", "Development", "Edge", "Infrastructure"];

function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function loadEnrichmentStore(): Record<string, EnrichmentData> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function saveEnrichmentStore(store: Record<string, EnrichmentData>) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(store));
}

function geoFromName(name: string): {
  region: string;
  lat: number;
  lng: number;
} {
  for (const [keyword, idx] of Object.entries(REGION_KEYWORDS)) {
    if (name.includes(keyword)) return GEO_REGIONS[idx];
  }
  const h = hashStr(name);
  return GEO_REGIONS[h % GEO_REGIONS.length];
}

function generateEnrichment(clusterId: string): EnrichmentData {
  const geo = geoFromName(clusterId);
  const h = hashStr(clusterId);
  return {
    region: geo.region,
    lat: geo.lat,
    lng: geo.lng,
    environment: ENVIRONMENTS[h % ENVIRONMENTS.length],
    cpuPercent: 30 + (h % 50),
    memoryPercent: 25 + ((h >> 4) % 55),
  };
}

function getOrCreateEnrichment(
  store: Record<string, EnrichmentData>,
  clusterId: string,
): EnrichmentData {
  if (store[clusterId]) return store[clusterId];
  const data = generateEnrichment(clusterId);
  store[clusterId] = data;
  return data;
}

function deriveStatus(
  cluster: GcpHcpCluster,
): "healthy" | "degraded" | "critical" {
  if (cluster.reconciling) return "degraded";
  const s = cluster.state?.toLowerCase();
  if (s === "running" || s === "ready" || s === "provisioned") return "healthy";
  if (s === "error" || s === "failed" || s === "deleting") return "critical";
  if (
    s === "provisioning" ||
    s === "updating" ||
    s === "degraded" ||
    s === "resuming"
  )
    return "degraded";
  return "healthy";
}

function mapClusters(raw: GcpHcpCluster[]): DashboardCluster[] {
  const store = loadEnrichmentStore();
  const clusters = raw.map((c): DashboardCluster => {
    const id = extractClusterId(c.name);
    const enrichment = getOrCreateEnrichment(store, id);
    return {
      id,
      name: id,
      provider: "gcp",
      region: enrichment.region,
      environment: enrichment.environment,
      status: deriveStatus(c),
      version: c.spec?.releaseVersion ?? "unknown",
      state: c.state ?? "unknown",
      reconciling: c.reconciling ?? false,
      lat: enrichment.lat,
      lng: enrichment.lng,
      cpuPercent: enrichment.cpuPercent,
      memoryPercent: enrichment.memoryPercent,
    };
  });
  // prune stale entries
  const liveIds = new Set(clusters.map((c) => c.id));
  for (const key of Object.keys(store)) {
    if (!liveIds.has(key)) delete store[key];
  }
  saveEnrichmentStore(store);
  return clusters;
}

export function useFleetData(): FleetData {
  const [clusters, setClusters] = useState<DashboardCluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const raw = await listGcpHcpClusters();
        if (!cancelled) {
          setClusters(mapClusters(raw));
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    const interval = setInterval(load, 30_000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return { clusters, loading, error };
}

export const FleetDataContext = createContext<FleetData>({
  clusters: [],
  loading: true,
  error: null,
});

export function useFleetDataContext(): FleetData {
  return useContext(FleetDataContext);
}
