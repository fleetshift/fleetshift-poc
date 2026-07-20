import "./delivery-events.scss";

import type { ClusterDetailTabProps } from "@fleetshift/common";
import {
  Label,
  SearchInput,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import { useMemo, useState } from "react";

import EventLogTerminal from "./EventLogTerminal";
import { useAutoScroll } from "./useAutoScroll";
import { useDeliveryEvents } from "./useDeliveryEvents";

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString();
}

export default function GcpHcpDeliveryEventsTab({
  clusterId,
}: ClusterDetailTabProps) {
  const { events, connected } = useDeliveryEvents(clusterId);
  const [searchTerm, setSearchTerm] = useState("");

  const lines = useMemo(
    () =>
      events.map(
        (ev) =>
          `[${formatTimestamp(ev.timestamp)}] [${ev.eventKind}] ${ev.message}`,
      ),
    [events],
  );

  const { containerRef } = useAutoScroll([lines.length]);

  return (
    <div>
      <Toolbar>
        <ToolbarContent>
          <ToolbarGroup>
            <ToolbarItem>
              <SearchInput
                placeholder="Search events..."
                value={searchTerm}
                onChange={(_e, val) => setSearchTerm(val)}
                onClear={() => setSearchTerm("")}
                aria-label="Search delivery events"
              />
            </ToolbarItem>
          </ToolbarGroup>
          <ToolbarItem align={{ default: "alignEnd" }}>
            <Label color={connected ? "green" : "red"} isCompact>
              {connected ? "Live" : "Disconnected"}
            </Label>
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>
      <EventLogTerminal
        lines={lines}
        searchTerm={searchTerm}
        containerRef={containerRef}
      />
    </div>
  );
}
