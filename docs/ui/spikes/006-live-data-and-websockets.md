# SPIKE: Live Cluster Data and Real-Time Updates

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

FleetShift connects to clusters it does not own and needs to show their current state — what pods are running, what nodes are available, what is healthy, what is not. This data changes constantly. Showing stale data is not useful for operations — if a pod crashed 30 seconds ago, the user needs to know now, not after they manually refresh.

The question is how to pull live data from real Kubernetes clusters and deliver it to the browser in real time, across multiple clusters simultaneously.

## What was explored

### K8s informers for live data

Kubernetes has a watch mechanism — instead of polling the API repeatedly, you can open a long-lived connection that streams changes as they happen. When a pod is created, modified, or deleted, the API server pushes an event. K8s client libraries wrap this in an "informer" pattern that handles reconnection, caching, and event delivery.

The server creates informers per cluster for the resource types the UI cares about — pods, nodes, namespaces, deployments, events, services, persistent volumes, persistent volume claims, and ingresses. Each informer watches its resource type on a specific cluster and emits add, update, and delete events.

Not everything supports watch. The metrics API (pod and node resource usage) does not have a watch endpoint — it is a snapshot API. For these, the server falls back to polling on a fixed interval.

### Handling permissions

Not every cluster grants access to every resource type. A service account might have permission to list pods but not nodes, or to read deployments but not persistent volumes. When the server tries to start an informer and gets a 403 Forbidden, it records that the resource type is not available on that cluster and moves on. The UI does not show an error — it just does not display data it cannot access. This keeps the system working even with limited permissions rather than failing entirely.

### Derived data

Some data the UI needs does not come directly from a K8s API. Alerts are synthesized by the server from the state of pods and nodes — if a pod is in CrashLoopBackOff or a node is NotReady, the server generates an alert event. This avoids requiring a separate alerting system to be installed on every cluster.

Pod logs are streamed from the K8s log API. The server manages a buffer per pod to handle partial lines (log chunks can split mid-line), parses timestamps and log levels from each line, and caps the number of concurrent log streams to avoid overwhelming the cluster API.

### WebSocket delivery

The live data needs to get from the server to the browser. HTTP request-response is not suitable — the server needs to push updates as they happen, not wait for the client to ask.

The server uses WebSocket for this. Authenticating a WebSocket connection is not straightforward — the browser's WebSocket API does not allow setting custom headers, so you cannot send an `Authorization` header like you would with a regular HTTP request. This is a limitation of the spec. The common workarounds are: passing a short-lived ticket as a query parameter, relying on cookies (which the browser sends automatically with the upgrade request), or sending an auth message as the first frame after connecting. Cookies work but tie you to cookie-based auth, which is not always an option (especially in cross-origin or API-first setups).

The POC uses the ticket approach, which is a well-established pattern. The client first requests a one-time ticket via a regular authenticated HTTP call, then presents that ticket as a query parameter when opening the WebSocket connection. The ticket expires after a short window to prevent reuse. Each connection gets a session ID for tracking.

Once connected, the server broadcasts events to all connected clients by topic. Topics correspond to resource types — "pods", "nodes", "deployments", "pod-metrics", "node-metrics", "alerts", "logs", and others. The server also tracks which session originated an event so it can exclude the sender from receiving their own changes back (for user-initiated actions like layout changes).

Some events are global (K8s resource changes affect all connected users) while others are user-scoped (navigation layout changes only matter to the user who made them). The WebSocket layer handles both.

### Frontend consumption

On the frontend, the shell opens a single WebSocket connection and runs a centralized event bus. The event bus is a simple publish-subscribe system — when a message arrives over the WebSocket, the shell dispatches it to all subscribers registered for that topic. Plugins subscribe to topics through a shared API that the shell provides. When a plugin's store subscribes to "pods" events, it gets notified every time any cluster reports a pod change. The store then updates its singleton state, and all components subscribed to that store re-render with the new data.

This is the approach used in the POC but it is not the only option. An alternative is to let each plugin open its own connection. This is generally simpler on the UI side — each plugin manages its own connection lifecycle without depending on a shared event bus. The tradeoff is on the server side, which has to maintain many more concurrent connections and track which ones need which events. A single shared connection is more efficient but adds complexity to the frontend routing layer. The right choice depends on the backend implementation and how many concurrent connections it can handle comfortably.

### Current state (proof of concept)

The live mode works end to end in the POC:

- Informers run per cluster for 9 resource types with automatic reconnection.
- Metrics are polled on a fixed interval since the metrics API does not support watch.
- Alerts are derived from pod and node state without requiring an external alerting system.
- Pod logs are streamed with buffering and log level detection.
- All updates flow through a single WebSocket connection per client.
- The event bus dispatches updates by topic to the right plugin stores.

Limitations of the POC:

- There is no backpressure handling. If a cluster generates a very high volume of events, the WebSocket can get congested. A production implementation would need to batch or throttle events.
- Log streaming is capped at 20 concurrent pods. This is a practical limit but not a real solution — it needs a proper resource management strategy.
- The ticket-based WebSocket auth is simple. It does not handle token refresh or session expiration during a long-lived connection.
- Informer error recovery is basic. If a watch connection drops and reconnection fails repeatedly, there is no circuit breaker or exponential backoff.

### Backend is provisional

The TypeScript server used in the POC exists only because a backend was needed to develop and test the frontend against. It is entirely throwaway. The real backend will be built likely in a different language with different tooling. The WebSocket protocol and real-time delivery approach explored here will need to be adapted to whatever the actual backend looks like. For the WebSocket layer specifically, the choice of library or protocol (raw WebSocket, Socket.IO, SSE, gRPC streaming, etc.) will depend entirely on the backend implementation.

### Open questions

- How should backpressure be handled when clusters generate high event volumes?
- Should the server filter events before broadcasting (e.g., only send events for resources the user has access to)?
- How do we handle WebSocket reconnection on the frontend without losing events during the gap?
- Is there a need for event history or replay when a client reconnects?

## Key files

- K8s informers: `packages/mock-servers/src/k8s/informers.ts`
- K8s data transforms: `packages/mock-servers/src/k8s/transforms.ts`
- Log streamer: `packages/mock-servers/src/k8s/logStreamer.ts`
- WebSocket server: `packages/mock-servers/src/ws.ts`
- Frontend event bus: `packages/gui/src/hooks/useInvalidationSocket.ts`
- Shell API (plugin subscription): `packages/gui/src/App.tsx`
