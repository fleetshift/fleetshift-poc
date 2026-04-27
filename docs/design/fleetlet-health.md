# Fleetlet Health & Liveness Detection

How the platform knows whether each fleetlet and its addons are alive.

## Full Lifecycle: Fleetlet ↔ Platform ↔ Browser

The complete flow from fleetlet boot through connection, steady-state operation, delivery, and disconnection.

```mermaid
sequenceDiagram
    participant F as Fleetlet
    participant C as Control Stream
    participant D as Deliver Stream
    participant S as FleetletServer
    participant B as ConditionBroadcaster
    participant W as Browser (WebSocket)

    Note over F: Fleetlet starts, connects to platform

    rect rgb(230, 245, 230)
    Note right of F: Phase 1 — Registration
    F->>C: RegisterTarget{target_id, target_type, name}
    C->>S: Store session in sessions map
    S->>C: TargetAccepted{target_id}
    end

    rect rgb(230, 235, 250)
    Note right of F: Phase 2 — Deliver Stream
    F->>D: Open stream (target-id in gRPC metadata)
    D->>S: setDeliverStream(stream)
    S->>B: publishHealthEvent (connected, awaiting heartbeat)
    B->>W: FleetletHealth{status=False}
    end

    rect rgb(245, 245, 230)
    Note right of F: Phase 3 — Steady State
    loop Every 30s
        F->>D: Heartbeat{target_id, addon_health[]}
        D->>S: Update lastHeartbeat, addonHealth, healthy=true
        S->>B: publishHealthEvent
        B->>W: FleetletHealth{status=True, reason=Healthy}
        B->>W: AddonHealth{name, connected, message}
    end

    loop Every 30s (if monitoring addon connected)
        F->>D: ConditionReport{ClusterMetrics, NodeMetrics[]}
        D->>S: Forward to broadcaster
        S->>B: Publish metrics conditions
        B->>W: ClusterMetrics + NodeMetrics data
    end
    end

    rect rgb(250, 240, 230)
    Note right of F: Phase 4 — Delivery (on-demand)
    S->>D: DeliveryRequest{manifests[]}
    D->>F: Apply via SSA
    F->>D: DeliveryAccepted
    F->>D: DeliveryCompleted{state=DELIVERED}
    end
```

## Health State Machine

Each fleetlet session transitions through these states based on heartbeat presence and shutdown signals.

```mermaid
stateDiagram-v2
    [*] --> Connected: Deliver stream opened

    Connected --> Healthy: First heartbeat received
    Healthy --> Healthy: Heartbeat within 90s

    Healthy --> HeartbeatExpired: No heartbeat for 90s\n(HealthChecker goroutine, 15s tick)
    HeartbeatExpired --> Healthy: Heartbeat resumes

    Healthy --> GracefulShutdown: Goodbye on Control stream\n(SIGTERM received)
    GracefulShutdown --> Disconnected: Stream closes

    HeartbeatExpired --> Disconnected: Stream closes
    Healthy --> Disconnected: Stream breaks unexpectedly
    Connected --> Disconnected: Stream breaks before first heartbeat

    Disconnected --> [*]: Session removed\nBroadcaster.Remove(targetID)
    Disconnected --> Connected: Fleetlet reconnects\n(exponential backoff 1s → 60s)
```

## Health Detection Timeline

Concrete example showing normal operation, heartbeat expiry, recovery, and graceful shutdown.

```mermaid
sequenceDiagram
    participant F as Fleetlet
    participant S as FleetletServer
    participant HC as HealthChecker (15s)
    participant B as Broadcaster → WS

    Note over F,B: Normal operation
    F->>S: Heartbeat (t=0s)
    S->>B: FleetletHealth: Healthy ✅
    F->>S: Heartbeat (t=30s)
    S->>B: FleetletHealth: Healthy ✅

    Note over F,B: Fleetlet becomes unresponsive
    F--xS: No heartbeat (t=60s)
    HC->>S: Check: last heartbeat 30s ago — OK
    F--xS: No heartbeat (t=90s)
    HC->>S: Check: last heartbeat 60s ago — OK
    F--xS: No heartbeat (t=120s)
    HC->>S: Check: last heartbeat 90s ago — EXPIRED
    S->>B: FleetletHealth: HeartbeatExpired 🔴

    Note over F,B: Fleetlet recovers
    F->>S: Heartbeat (t=150s)
    S->>B: FleetletHealth: Healthy ✅

    Note over F,B: Graceful shutdown (SIGTERM)
    F->>S: Goodbye{reason=SIGTERM} (Control stream)
    S->>S: session.graceful = true
    Note over F: 500ms grace period
    F->>S: Stream closes
    S->>B: GracefulShutdown
    S->>S: Remove session + broadcaster entry
```

## Addon Health Tracking

Each fleetlet can have sidecar addons connected via go-plugin over Unix sockets. Addon health status is embedded in every heartbeat message and broadcast to the UI alongside the fleetlet's own health.

```mermaid
flowchart LR
    subgraph cluster["Kind Cluster — Fleetlet Pod"]
        MON["Monitoring Sidecar\n(go-plugin server)"]
        FL["Fleetlet Container\n(go-plugin client)"]
        UDS[("Unix Socket\n/run/plugins/plugin*")]
        MON ---|plugin.Serve| UDS
        UDS ---|ReattachConfig| FL
    end

    subgraph platform["Platform"]
        FS["FleetletServer"]
        HC["HealthChecker\nevery 15s"]
        BC["ConditionBroadcaster"]
    end

    subgraph browser["Browser"]
        HD["Health Dashboard"]
        MD["Metrics Dashboard"]
    end

    FL -- "Heartbeat (30s)\nAddonHealth[]" --> FS
    FL -- "ConditionReport (30s)\nClusterMetrics, NodeMetrics" --> FS
    FS --> BC
    HC -- "check lastHeartbeat > 90s" --> FS
    BC -- "WebSocket" --> HD
    BC -- "WebSocket" --> MD
```

### Addon health data flow

The `addonStatus` struct in the fleetlet tracks each addon with thread-safe accessors, shared between the metrics collection goroutine and the heartbeat goroutine.

| Field | Updated by | Read by |
|---|---|---|
| `connected` | `connectMonitoringPlugin()` | `heartbeatLoop()` via `toProto()` |
| `lastCollectionAt` | `metricsLoop()` on success | `heartbeatLoop()` via `toProto()` |
| `lastError` | `metricsLoop()` on failure | `heartbeatLoop()` via `toProto()` |

Each heartbeat message includes `AddonHealth[]` with the current snapshot. The platform stores this on the session and broadcasts it as separate `AddonHealth` conditions alongside `FleetletHealth`.

## Reconnection & Backoff

The fleetlet wraps its entire `run()` function in a retry loop with exponential backoff. Each `run()` is a fresh gRPC connection: register → deliver stream → steady state.

```mermaid
flowchart TD
    START([Fleetlet starts]) --> CONNECT[Connect to platform gRPC]
    CONNECT --> REG[Register target via Control stream]
    REG --> DELIVER[Open Deliver stream]
    DELIVER --> LOOP["Steady state:\nheartbeat + metrics + delivery"]

    LOOP -->|Stream error| BACKOFF["Exponential backoff\n1s → 2s → 4s → ... → 60s max"]
    LOOP -->|SIGTERM| GOODBYE[Send Goodbye on Control stream]
    GOODBYE --> GRACE[500ms grace period]
    GRACE --> EXIT([Clean exit])

    BACKOFF -->|ctx not canceled| CONNECT
    BACKOFF -->|SIGTERM during wait| EXIT
```

The fleetlet uses two contexts to make graceful shutdown work:

- **Signal context** — canceled on SIGTERM. Triggers the Goodbye flow.
- **Run context** — controls the gRPC streams. Canceled 500ms after Goodbye is sent, giving the platform time to receive it before the streams close.

## Condition Types

All health data flows through the existing `ConditionBroadcaster` → WebSocket pipeline. The UI filters by condition type.

| Condition Type | Status | Reason | Message (JSON) |
|---|---|---|---|
| `FleetletHealth` | `True` | `Healthy` | `{"healthy":true,"graceful":false,"lastHeartbeat":"RFC3339"}` |
| `FleetletHealth` | `False` | `HeartbeatExpired` | `{"healthy":false,"graceful":false,"lastHeartbeat":"RFC3339"}` |
| `FleetletHealth` | `False` | `GracefulShutdown` | `{"healthy":false,"graceful":true,"lastHeartbeat":"RFC3339"}` |
| `FleetletHealth` | `False` | `Disconnected` | `{"healthy":false,"graceful":false}` |
| `AddonHealth` | `True`/`False` | addon name | error message (empty if healthy) |
| `ClusterMetrics` | `True` | `Collected` | `{"nodes":N,"pods":N,"cluster":"id"}` |
| `NodeMetrics` | `True` | node name | `{"cpuCapacity":N,"cpuUsage":N,"memCapacity":N,...}` |

## Timing Parameters

| Parameter | Value | Rationale |
|---|---|---|
| Heartbeat interval | 30s | Matches metrics collection interval |
| Health check tick | 15s | Half the heartbeat interval for responsive detection |
| Heartbeat expiry | 90s | 3× heartbeat interval — tolerates 2 missed heartbeats |
| Reconnect backoff | 1s → 60s | Exponential with 60s cap |
| Goodbye grace | 500ms | Enough for one gRPC round trip |

## Proto Messages

```protobuf
// On Deliver stream (DeliverEvent oneof field 6)
message Heartbeat {
  string target_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  repeated AddonHealth addon_health = 3;
}

message AddonHealth {
  string name = 1;
  bool connected = 2;
  google.protobuf.Timestamp last_collection_time = 3;
  string message = 4;  // error message if unhealthy
}

// On Control stream (ControlEvent oneof field 2)
message Goodbye {
  string target_id = 1;
  string reason = 2;
  google.protobuf.Timestamp timestamp = 3;
}
```
