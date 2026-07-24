# Kind Deployment & OIDC Authentication

## Status: Working (with caveats)

Kind cluster provisioning from the UI works end-to-end. Clusters are created via the Go backend's orchestration workflow using kind's Go library. Several issues were discovered and fixed during initial testing.

## How to Run (Local Dev)

### 1. Start the Go backend

```bash
cd ../fleetshift-poc
make build
CONTAINER_HOST=host.docker.internal ./bin/fleetshift serve --http-addr :8085 --log-level debug
```

`CONTAINER_HOST` rewrites `localhost` in OIDC issuer URLs to `host.docker.internal` so kind containers can reach host services. Without this, OIDC-authenticated deployments fail because `localhost` inside a Docker container doesn't reach the host.

### 2. Register auth method

```bash
./bin/fleetctl auth setup \
  --issuer-url http://localhost:8180/realms/fleetshift \
  --client-id fleetshift-ui \
  --audience fleetshift-ui
```

### 3. Create a deployment from the UI

Navigate to **Orchestration** in the Control Plane section. Click "Create deployment", enter a cluster name, and submit. Leave the Kind Config field empty — the backend handles config generation.

### 4. Monitor progress

```bash
# Watch Go backend logs (best source of truth)
# Look for "delivery event" lines with progress/error

# Check if kind container is running
docker ps --filter "label=io.x-k8s.kind.cluster"

# Inspect kubeadm config (while container exists)
docker exec <cluster-name>-control-plane cat /kind/kubeadm.conf

# Verify cluster after provisioning
kind get kubeconfig --name <cluster-name> | kubectl --kubeconfig /dev/stdin get nodes
```

---

## Issues Found & Fixes Applied

### Issue 1: UI sent `config` field — rejected when authenticated

**Symptom:** `kind cluster spec cannot have both config and an authenticated caller`

**Root cause:** The kind agent (`agent.go:188-189`) enforces mutual exclusivity between custom `config` and an authenticated caller. When logged in via OIDC, the agent auto-generates the kind config with OIDC API server flags. Custom config is rejected because it could override security-critical OIDC settings.

**Fix (UI):** The deployment form now defaults the Kind Config field to empty. The manifest sent is just `{"name": "cluster-name"}` — no `config` field. The backend generates the config automatically.

**File:** `packages/mock-ui-plugins/src/plugins/management-plugin/DeploymentsPage.tsx`

### Issue 2: OIDC issuer URL unreachable from kind containers

**Symptom:** `kubeadm init` fails with exit status 1 during "Starting control-plane". The API server can't start because it tries to reach the OIDC issuer at `http://localhost:8180` — which is unreachable from inside the Docker container.

**Root cause:** `BuildKindOIDCConfig()` injects the OIDC issuer URL from the caller's token directly into the kubeadm config. When the issuer is on `localhost` (e.g., local Keycloak), `localhost` inside the kind container refers to the container itself, not the host machine.

**Fix (Go backend):** Added `CONTAINER_HOST` env var support. When set, the kind agent rewrites `localhost`/`127.0.0.1` in the issuer URL to the configured host before injecting into the kubeadm config. Token validation on the platform side continues using the original URL.

**Files changed:**
- `fleetshift-server/internal/addon/kind/agent.go` — Added `WithContainerHost` option and `rewriteIssuerForDocker` method
- `fleetshift-server/internal/cli/serve.go` — Reads `CONTAINER_HOST` env var

### Issue 3: Workflow replay with stale payloads

**Symptom:** After fixing the manifest format, reusing the same deployment ID still fails. The Go backend logs show `workflows.is_replaying=true` and the old broken payload is replayed.

**Root cause:** FleetShift uses a durable workflow engine (`go-workflows`). Failed workflow activities persist in SQLite and are replayed with the original payload when the server restarts or the workflow is re-triggered.

**Workaround:** Use a different deployment ID, or delete the database and restart:

```bash
rm ../fleetshift-poc/fleetshift.db
# Restart the server, then re-register auth method
```

### Issue 4: `subject_id` is empty in auth logs

**Symptom:** Auth logs show `subject_id=""` even though authentication passes. This means `auth.Caller` is nil, so the OIDC config path (auto-generate kind config with OIDC flags, bootstrap RBAC) is **not taken**. Clusters are created with default kind config — no OIDC auth on the API server.

**Root cause:** Not yet investigated. Likely a Keycloak realm configuration issue — the token may not carry the expected `sub` claim, or the Go backend's subject extraction logic doesn't find it.

**Impact:** Clusters work but don't have OIDC authentication configured. Users can't `kubectl` into them with their OIDC identity. They work with the kind-generated client certificate kubeconfig instead.

**TODO:** Investigate why `subject_id` is empty. Check:
1. Keycloak realm config — does the token include a `sub` claim?
2. Go backend auth middleware — how does it extract `SubjectClaims` from the validated token?
3. Whether the Express proxy strips or modifies the Authorization header

---

## Architecture: How Kind Deployment Works

### Request Flow

```
Browser → Express (:4000) → proxy /api/v1/management/* → Go backend (:8085) gRPC-gateway
```

### Manifest Format

The UI sends a `CreateDeploymentRequest` with the manifest base64-encoded:

```json
{
  "deployment_id": "my-cluster",
  "deployment": {
    "manifest_strategy": {
      "type": "TYPE_INLINE",
      "manifests": [{
        "resource_type": "api.kind.cluster",
        "raw": "<base64 of ClusterSpec JSON>"
      }]
    },
    "placement_strategy": { "type": "TYPE_STATIC", "target_ids": ["kind-local"] },
    "rollout_strategy": { "type": "TYPE_IMMEDIATE" }
  }
}
```

The `ClusterSpec` JSON (inside the base64):

```json
{ "name": "my-cluster" }
```

### Config Resolution (3 modes)

| Condition | Config Source | What happens |
|-----------|-------------|--------------|
| `auth.Caller != nil` (authenticated with subject) | `oidc` | Auto-generates kind config with OIDC API server flags + bootstraps RBAC |
| `config` field present (no caller) | `custom` | Uses provided config as-is |
| Neither | `default` | Plain kind cluster with default config |

### Orchestration Workflow

The Go backend runs a durable workflow (`orchestrate-deployment`) with these activities:

1. `load-deployment-and-pool` — Load deployment spec from store
2. `resolve-placement` — Resolve target(s) from placement strategy
3. `plan-rollout` — Plan the rollout strategy
4. `generate-manifests` — Filter manifests for target
5. `deliver-to-target` — Call the kind delivery agent

The kind agent validates the manifest, then runs `provider.Create()` asynchronously. Progress events flow back via `DeliverySignaler`.

### Target Registration

The `kind-local` target auto-registers on server startup (`serve.go:156-163`). No manual target creation needed.

---

## Customization Limitations (When Authenticated)

When `auth.Caller` is set (OIDC with subject), the only customization available is via the `oidc` field:

```json
{
  "name": "my-cluster",
  "oidc": {
    "usernameClaim": "email",
    "groupsClaim": "roles"
  }
}
```

Everything else (node count, networking, extra mounts) is locked down. To support authenticated + custom topology, the Go backend would need either:

1. **Merge approach** — Accept partial config and merge with auto-generated OIDC flags
2. **Structured fields** — Add `nodes`, `networking` as top-level ClusterSpec fields, injected safely into the generated config

Both require Go backend changes in `resolveConfig()` and `BuildKindOIDCConfig()`.

---

## Live Updates: Current State & Future Direction

**Current (polling):** The UI polls `ListDeployments` / `GetDeployment` every 5 seconds while any deployment is in a transient state (`STATE_CREATING`, `STATE_DELETING`). Polling stops once state settles. This is a stopgap.

**Target architecture: gRPC server-side streaming**

The Go backend should expose a `WatchDeployment` or `WatchDeployments` server-streaming RPC. The proto would look something like:

```protobuf
rpc WatchDeployments(WatchDeploymentsRequest) returns (stream DeploymentEvent);

message WatchDeploymentsRequest {
  string deployment_id = 1;  // optional: filter to single deployment
}

message DeploymentEvent {
  enum EventType {
    STATE_CHANGED = 0;
    PROGRESS = 1;
    DELIVERY_EVENT = 2;
  }
  EventType type = 1;
  Deployment deployment = 2;  // full snapshot on state change
  string message = 3;         // progress/delivery event message
}
```

The kind agent already emits progress events via `DeliverySignaler` (e.g., "Ensuring node image", "Preparing nodes", "Starting control-plane"). These could be surfaced to the UI through this stream, giving users real-time provisioning progress — not just state transitions.

**Integration path:**

1. Add the streaming RPC to `deployment_service.proto`
2. Implement in the Go backend — subscribe to workflow signals and delivery events
3. Expose via gRPC-gateway as SSE (`text/event-stream`) or via a WebSocket bridge
4. Express server proxies the stream or bridges to its existing WebSocket
5. UI subscribes on mount, replaces polling

The gRPC-gateway supports server-side streaming via newline-delimited JSON (`application/x-ndjson`) or SSE. The Express proxy would need to handle streaming responses (pass-through, not buffer).

## Debugging Cheat Sheet

| Problem | Check |
|---------|-------|
| Deployment stuck in "Creating" | Go backend logs — look for `delivery event` lines |
| `kubeadm init` fails | `docker exec <name>-control-plane cat /kind/kubeadm.conf` — check for bad OIDC URLs |
| Same deployment ID keeps failing | Delete `fleetshift.db` and restart — workflow replay issue |
| No OIDC on created cluster | Check `subject_id` in auth logs — if empty, `auth.Caller` is nil |
| Container missing after failure | Kind cleans up on failure — need to inspect during "Starting control-plane" window |
| OIDC issuer unreachable from container | Set `CONTAINER_HOST=host.docker.internal` when starting the Go backend |
