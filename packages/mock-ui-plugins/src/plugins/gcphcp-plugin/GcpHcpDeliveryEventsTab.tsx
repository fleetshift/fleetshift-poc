import { useState, useEffect, useRef, useMemo } from "react";
import { LogViewer, LogViewerSearch } from "@patternfly/react-log-viewer";
import {
  Label,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  ToolbarGroup,
  EmptyState,
  EmptyStateBody,
} from "@patternfly/react-core";

interface DeliveryEvent {
  type: string;
  deliveryId: string;
  eventKind: string;
  message: string;
  timestamp: number;
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString();
}

export default function GcpHcpDeliveryEventsTab() {
  const [events, setEvents] = useState<DeliveryEvent[]>([]);
  const [wsConnected, setWsConnected] = useState(false);
  const logViewerRef = useRef<{ scrollToBottom: () => void }>(null);

  useEffect(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${proto}//${window.location.host}/api/ui/events/ws`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);
    ws.onerror = () => setWsConnected(false);

    ws.onmessage = (e) => {
      try {
        const event: DeliveryEvent = JSON.parse(e.data);
        if (event.deliveryId?.includes("gcphcp")) {
          setEvents((prev) => {
            const last = prev[prev.length - 1];
            if (last && last.message === event.message) return prev;
            return [...prev, event];
          });
        }
      } catch {
        // ignore malformed messages
      }
    };

    return () => ws.close();
  }, []);

  useEffect(() => {
    logViewerRef.current?.scrollToBottom();
  }, [events.length]);

  const logData = useMemo(
    () =>
      events.length > 0
        ? events
            .map(
              (ev) =>
                `[${formatTimestamp(ev.timestamp)}] [${ev.eventKind}] ${ev.message}`,
            )
            .join("\n")
        : "Waiting for delivery events...",
    [events],
  );

  if (events.length === 0 && !wsConnected) {
    return (
      <EmptyState titleText="No events" headingLevel="h2">
        <EmptyStateBody>
          WebSocket is not connected. Events will appear here when the
          connection is re-established.
        </EmptyStateBody>
      </EmptyState>
    );
  }

  return (
    <LogViewer
      data={logData}
      hasLineNumbers
      isTextWrapped
      height={400}
      theme="dark"
      scrollToRow={events.length || undefined}
      innerRef={logViewerRef}
      toolbar={
        <Toolbar>
          <ToolbarContent>
            <ToolbarGroup>
              <ToolbarItem>
                <LogViewerSearch
                  placeholder="Search events..."
                  minSearchChars={2}
                />
              </ToolbarItem>
            </ToolbarGroup>
            <ToolbarItem align={{ default: "alignEnd" }}>
              <Label color={wsConnected ? "green" : "red"} isCompact>
                {wsConnected ? "Live" : "Disconnected"}
              </Label>
            </ToolbarItem>
          </ToolbarContent>
        </Toolbar>
      }
    />
  );
}
