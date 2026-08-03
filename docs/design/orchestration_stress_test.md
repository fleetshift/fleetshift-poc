# Orchestration Stress Test

## Context

The orchestration loop is the central control plane path: every deployment
create, update, pause, resume, and delete flows through it. Under fleet-scale
conditions the loop must handle concurrent mutations (many deployments being
updated while earlier generations are still in-flight) without leaking
resources, losing updates, or degrading latency beyond acceptable bounds.

We needed a reproducible, single-process stress test that could exercise
generation pressure -- where mutations arrive faster than deliveries
complete -- so we could observe how the system degrades, where the
bottlenecks are, and validate the correctness of generation skipping.

## Test design

### Architecture

The test runs entirely inside `go test` with a Postgres testcontainer
(no external services). Following files were added:

| File | Responsibility |
|------|----------------|
| `fleetshift-server/internal/application/stress_test.go` | Test entry point, workload runner, drain phase, convergence loop |
| `fleetshift-server/internal/application/stress_harness_test.go` | Configuration, mock delivery agent, fulfillment observer, Pareto distribution, pool stats sampler |
| `fleetshift-server/internal/application/stress_report_test.go` | Latency distributions, generation convergence, JSON results dump |
| `scripts/plot_stress.py` | Plots Mutation_to_Dispatch latency from JSON results dump |


### Workload model

Operations fire at a configurable rate (`STRESS_RATE`) for a configurable
duration (`STRESS_DURATION`). Each tick selects a deployment from a pool
(`STRESS_DEPLOYMENTS`) using a **Pareto distribution** (alpha = 7.21),
which concentrates 80% of selections on the bottom 20% of indices -- creating
"hotspot" deployments that receive many updates while their earlier
generations are still being delivered.

- **Create**: if the selected deployment slot hasn't been used yet, create
  it via `DeploymentService.Create` with a round-robin target assignment
  across 50 fixed targets.
- **Update**: if the deployment exists, directly mutate its fulfillment's
  manifest strategy in the store and trigger an async convergence loop.
  This bumps the generation, exercising the orchestration's
  generation-advancement code path.

### Mock delivery agent

The agent simulates realistic addon behavior with configurable delays:

- **Ack delay**: Gaussian-distributed between `STRESS_ACK_DELAY_{MIN,MAX}`.
  After sleeping, sends a `ReportEvent` (ack signal).
- **Completion delay**: Gaussian-distributed between
  `STRESS_COMPLETION_DELAY_{MIN,MAX}`. After the remaining time, sends a
  `ReportResult` (delivered or failed).
- **Failure rate**: a configurable fraction of deliveries report failure 
  (Not exercised at the time of the writing).

The agent records its chosen delays per delivery so the report can isolate
system overhead from agent-induced latency.

### Measured metrics

| Metric | What it measures |
|--------|-----------------|
| Reconciliation latency | `RunStarted` to `End` per orchestration run |
| Ack latency | Dispatch to ack received per delivery |
| Completion latency | Dispatch to completed per delivery |
| Ack / completion overhead | Observed latency minus the agent's chosen delay |
| Mutation-to-dispatch latency | Mutation commit to delivery dispatch -- isolates system queueing |
| Runs per fulfillment | How many times each fulfillment's orchestration ran |
| Restarts per run | Inner-loop restarts of the orchestration run due to generation advancement |
| Generation convergence | Final snapshot: converged vs lagged fulfillments, skip counts |
| Connection pool utilization | Time-series of in-use / max-open for app and workflow pools |

Results are printed to the test log and dumped to a JSON file for
post-hoc analysis with `scripts/plot_stress.py`.

## Key findings

### 1. memworkflow cannot sustain high concurrency

The in-memory workflow engine (`memworkflow`) spawns one goroutine per
workflow and activity. Under sustained load (100 ops/s, thousands of
concurrent deliveries with multi-minute delays), this led to:

- Goroutine counts in the millions, eventually hitting process limits.
- Workflows blocked indefinitely on `Await` with no backpressure.
- Memory consumption growing without bound.

**Decision**: the stress test uses only `goworkflows` (backed by Postgres),
which uses a bounded poller pool and can persist workflow state across
restarts.

### 2. Connection pool sizing matters

The stress test and production system share a single Postgres database for
both the application store and the go-workflows engine. Pool sizing requires
careful budgeting:

- **60/40 split**: 60% of the Go-side connection budget goes to the
  application store, 40% to go-workflows. Orchestration activity DB
  operations are lighter than the app store's read-modify-write cycles.
- **Postgres max_connections = budget + 20**: 20 connections reserved for
  admin, migration, and monitoring.

Over-provisioning connections causes OOM (each Postgres backend consumes
~10 MB), while under-provisioning causes `context deadline exceeded` as
operations queue behind connection waits. See
[stress_results.md](stress_results.md) for host-specific tuning
baselines.

### 3. Testcontainer must be explicitly tuned

The default `postgres:18` image starts with 128 MB `shared_buffers`
regardless of container memory. The stress test's working set exceeds
this, causing excessive buffer eviction. The test configures:

- `shared_buffers = 1 GB`
- `max_wal_size = 4 GB` (so checkpoint frequency doesn't become a
  bottleneck)
- `/dev/shm` = 1 GiB (PostgreSQL uses `/dev/shm` for dynamic shared
  memory allocated by parallel query workers; sorts and hash joins use
  private `work_mem`. The default 64 MB container limit causes
  parallel-query failures.)
- `max_connections` derived from the pool budget

### 4. Deployment update via store mutation + convergence loop

The stress test doesn't use the service layer for updates (no
`UpdateDeployment` API exists yet). Instead, it directly mutates the
fulfillment's manifest strategy in the store, then runs an async
convergence loop.

This mirrors `ResumeDeploymentWorkflowSpec`: commit the mutation first,
then use a convergence loop that handles the race where orchestration may
already be running. The two-step pattern ensures:

1. The mutation is durable (committed before orchestration starts).
2. Concurrent orchestration runs don't lose the update
   (`ErrAlreadyRunning` is handled with retry).
3. Superseded generations exit immediately (no wasted work).

Transient `duplicate key value violates unique constraint
"manifest_strategies_pkey"` errors are expected and retried (up to 5
times). They occur when two concurrent activities try to advance the
same fulfillment row.

### 5. Mutation-to-dispatch latency is dominated by in-flight delivery wait, not resource contention

Mutation-to-dispatch latency (mutation commit → dispatch of the final
generation) is **structural, not resource-bound**. When a new generation
arrives for a hotspot deployment, the orchestration loop cannot dispatch
it until the current `dispatchAndAwait` cycle for the previous generation
completes. The final generation must wait for:

1. The in-flight delivery of generation N-x to complete.
2. The orchestration to restart, detect the new generation, and
   dispatch it.

This multi-hop wait dominates the drain tail. Comparative runs with
4x different resource budgets show nearly identical latency profiles,
confirming the bottleneck is structural (see
[stress_results.md](stress_results.md) for data).

**We chose not to preempt `dispatchAndAwait`** (e.g. with a periodic
timeout that would abandon the in-flight delivery and redispatch the
newer generation). While preemption would reduce latency for the latest
generation, it would push multiple generations to the agent
simultaneously when the previous ones haven't completed. In some use
cases this is desirable (fast rollforward), but in others it is not
(an agent that can only handle one version at a time). This is a
policy decision that should be configurable, not hardcoded.

### 6. Generation skipping works correctly

The orchestration loop correctly detects generation advancement
mid-rollout via the `GenerationAdvancedMidRollout` probe. When a newer
generation supersedes a running rollout:

1. The current dispatch cycle completes (doesn't abandon in-flight
   deliveries).
2. The inner loop restarts from the new generation.
3. Intermediate generations that were never independently dispatched
   are skipped -- visible in the "gen skips" convergence report.

Under Pareto-distributed load, hotspot deployments routinely reach
generation 50+ while still processing generation 1. The system
converges correctly despite this extreme pressure.

### 7. Pareto distribution concentrates realistic pressure

Using `alpha = 7.21` (derived from `ln(1-0.8)/ln(1-0.2)`) makes 80%
of selections hit the bottom 20% of the pool. This creates a realistic
hotspot pattern: a small number of deployments receive intensive update
traffic while the majority see only their initial creation. This is more
representative of real fleet behavior than uniform random selection.

## Running the test

```sh
STRESS_RATE=100 \
STRESS_DURATION=15m \
STRESS_DEPLOYMENTS=2000 \
STRESS_ACK_DELAY_MIN=1s \
STRESS_ACK_DELAY_MAX=5s \
STRESS_COMPLETION_DELAY_MIN=5m \
STRESS_COMPLETION_DELAY_MAX=10m \
STRESS_FAILURE_RATE=0.05 \
STRESS_POSTGRES_MAX_CONNS=200 \
STRESS_WORKFLOW_POLLERS=16 \
task test:stress
```

## Visualizing results

```sh
# Mutation-to-dispatch latency over time
uv run --with matplotlib python3 scripts/plot_stress.py /tmp/stress-results-*.json

# With connection pool overlay
uv run --with matplotlib python3 scripts/plot_stress.py /tmp/stress-results-*.json --pool-stats 

# Highlight last-generation deliveries, showing superseded in gray
uv run --with matplotlib python3 scripts/plot_stress.py /tmp/stress-results-*.json --last-gen
```

## Open questions

- **SELECT ... FOR UPDATE on fulfillment rows**: read-modify-write cycles
  on the fulfillment table are the primary source of contention. Row-level
  locking could eliminate the duplicate-key retries but would introduce
  lock-wait latency. Needs benchmarking. See [OME-187](https://redhat.atlassian.net/browse/OME-187)
- **Service-layer update path**: the direct store mutation is a stress-test
  shortcut. A proper `UpdateDeployment` service method would use a durable
  workflow (like create and resume do), which would change the concurrency
  profile. The stress test should be updated when that API exists.
- **Hardware-specific pool tuning**: the 200-connection default and 60/40
  split were empirically derived on a 16-core / 64 GB machine. Different
  hardware profiles may have different optimal points.
- **Configurable dispatch preemption**: the current `dispatchAndAwait`
  blocks until the in-flight delivery completes, which dominates
  mutation-to-dispatch latency under generation pressure (see finding 5).
  A configurable preemption policy (e.g. periodic timeout that abandons
  the current delivery and redispatches the latest generation) could
  reduce convergence latency for use cases that tolerate multiple
  generations in-flight on the agent side.