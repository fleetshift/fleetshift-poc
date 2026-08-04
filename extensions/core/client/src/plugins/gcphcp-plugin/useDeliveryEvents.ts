import { useCallback, useEffect, useRef, useState } from "react";

export interface DeliveryEvent {
  type: string;
  deliveryId: string;
  eventKind: string;
  message: string;
  timestamp: number;
}

export interface UseDeliveryEventsReturn {
  events: DeliveryEvent[];
  connected: boolean;
}

const STORAGE_KEY_PREFIX = "fleetshift:delivery-events:";
const MAX_PERSISTED = 500;
const BACKOFF_MS = [1000, 2000, 4000, 8000, 15000];

function storageKey(clusterId: string): string {
  return `${STORAGE_KEY_PREFIX}${clusterId}`;
}

function loadPersistedEvents(clusterId: string | undefined): DeliveryEvent[] {
  if (!clusterId) return [];
  try {
    const raw = localStorage.getItem(storageKey(clusterId));
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as DeliveryEvent[]) : [];
  } catch {
    return [];
  }
}

function persistEvents(
  clusterId: string | undefined,
  events: DeliveryEvent[],
): void {
  if (!clusterId) return;
  try {
    const trimmed =
      events.length > MAX_PERSISTED ? events.slice(-MAX_PERSISTED) : events;
    localStorage.setItem(storageKey(clusterId), JSON.stringify(trimmed));
  } catch {
    // quota exceeded — silently skip
  }
}

export function useDeliveryEvents(
  clusterId: string | undefined,
): UseDeliveryEventsReturn {
  const [events, setEvents] = useState<DeliveryEvent[]>(() =>
    loadPersistedEvents(clusterId),
  );
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const attemptRef = useRef(0);
  const clusterIdRef = useRef(clusterId);
  clusterIdRef.current = clusterId;

  const connect = useCallback(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(
      `${proto}//${window.location.host}/api/ui/events/ws`,
    );
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      attemptRef.current = 0;
    };

    ws.onmessage = (e) => {
      try {
        const event: DeliveryEvent = JSON.parse(e.data as string);
        if (!event.deliveryId?.includes("gcphcp")) return;
        const isDuplicate = events.some((e) => e.message === event.message);
        if (isDuplicate) {
          return;
        }
        setEvents((prev) => {
          const next = [...prev, event];
          return next.length > MAX_PERSISTED
            ? next.slice(-MAX_PERSISTED)
            : next;
        });
      } catch (e) {
        console.error("Unable to parsel cluster event: ", e);
        // ignore malformed messages
      }
    };

    const scheduleReconnect = () => {
      setConnected(false);
      wsRef.current = null;
      const delay =
        BACKOFF_MS[Math.min(attemptRef.current, BACKOFF_MS.length - 1)];
      attemptRef.current += 1;
      reconnectRef.current = setTimeout(() => {
        reconnectRef.current = null;
        connect();
      }, delay);
    };

    ws.onclose = scheduleReconnect;
    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectRef.current) clearTimeout(reconnectRef.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.onerror = null;
        wsRef.current.close();
      }
      wsRef.current = null;
    };
  }, [connect]);

  const prevClusterIdRef = useRef(clusterId);

  useEffect(() => {
    if (prevClusterIdRef.current !== clusterId) {
      prevClusterIdRef.current = clusterId;
      setEvents(loadPersistedEvents(clusterId));
      return;
    }
    persistEvents(clusterId, events);
  }, [clusterId, events]);

  return { events, connected };
}
