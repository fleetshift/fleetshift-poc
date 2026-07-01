# ACM Search — Inventory Write Path Deep Dive

This document investigates **where and how ACM Search inventory data is written to
the database**, what makes writing a batch of inventory expensive, and what
optimizations `search-indexer` applies (or doesn't) to control that cost.

It complements [`ARCHITECTURE.md`](./ARCHITECTURE.md) with concrete code/SQL detail
and a cost analysis, rather than replacing it.

## 1. Ownership summary

A common assumption is that `search-v2-api` owns storage, since it's the
GraphQL-facing service. It doesn't — it's read-only against the same database.

| Repo | Role | Talks to Postgres? |
|---|---|---|
| `search-collector` | Runs per managed cluster (and the hub). Watches K8s resources via informers, transforms them, diffs against previous state, and **POSTs** JSON sync payloads over HTTPS. | No DB code at all. |
| **`search-indexer`** | Hub-side HTTPS service. Receives sync payloads and **owns every INSERT / UPDATE / DELETE**. | **Yes — this is the write path.** |
| `search-v2-api` | GraphQL read API for the UI/CLI. | Yes, but only `SELECT` + Postgres `LISTEN/NOTIFY` for live-watch subscriptions. |

Confirmed directly in the indexer's own architecture doc:

> "search-indexer is a Go HTTPS service running inside the ACM hub cluster. It is
> the write path for the ACM Search datastore."
> — `search-indexer/docs/ARCHITECTURE.md:5`

## 2. Data flow

```text
search-collector (per managed cluster, and the hub itself)
  Informer   → watches pods, nodes, deployments, etc. via the K8s API
  Transformer→ extracts searchable properties + edges (relationships)
  Reconciler → keeps in-memory state, computes add/update/delete diff
  Sender     → HTTPS POST JSON payload (whole diff/state in ONE request, no client-side chunking)
        │
        │  POST /aggregator/clusters/{clusterName}/sync
        │  Header: X-Overwrite-State: true|false
        ▼
search-indexer  (hub cluster)
  server.SyncResources          → decode request, dispatch
  database.DAO.SyncData         → delta sync (adds/updates/deletes)
  database.DAO.ResyncData       → full resync (complete overwrite)
  batchWithRetry → pgx.Batch → pool.SendBatch(ctx, batch)
        │
        ▼
PostgreSQL (schema: search)
  search.resources (uid TEXT PK, cluster TEXT, data JSONB)
  search.edges     (sourceId, sourceKind, destId, destKind, edgeType, cluster)
        │
        ▼
search-v2-api
  GraphQL SELECT queries
  LISTEN search_resources_notify  (Postgres trigger fires on writes made by the indexer)
```

Two request types, distinguished by the `X-Overwrite-State` header:

1. **Delta sync** (`X-Overwrite-State: false`) — normal steady-state traffic.
   Small payload containing only what changed since the last successful send.
2. **Full resync** (`X-Overwrite-State: true`) — the collector's *entire* current
   state. Can be very large (the indexer has a dedicated 20MB+ "large request"
   concurrency limiter for this case). Triggered when the collector has never
   sent before, or the previous send cycle failed.

## 3. Code walkthrough

### 3.1 Entry point — `pkg/server/syncHandler.go`

```go
func (s *ServerConfig) SyncResources(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	clusterName := params["id"]

	var syncEvent model.SyncEvent
	bodyBytes, err := io.ReadAll(r.Body)
	// ...

	overwriteStateHeader := r.Header.Get("X-Overwrite-State")
	overwriteState, overwriteStateErr := strconv.ParseBool(overwriteStateHeader)
	// ...

	// The collector sends 2 types of requests with the header:
	// 1. ReSync [X-Overwrite-State=true]  - It has the complete current state. It must overwrite any previous state.
	// 2. Sync   [X-Overwrite-State=false] - This is the delta changes from the previous state.
	if overwriteState {
		err = s.Dao.ResyncData(r.Context(), clusterName, syncResponse, bodyBytes)
	} else {
		// we can decode the entire request for non resync requests because they are significantly smaller
		err = json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&syncEvent)
		if err == nil {
			err = s.Dao.SyncData(r.Context(), syncEvent, clusterName, syncResponse)
		}
	}
	// ...

	// Get the total cluster resources for validation by the collector.
	totalResources, totalEdges, validateErr := s.Dao.ClusterTotals(r.Context(), clusterName)
	// ...
}
```
*(`pkg/server/syncHandler.go:21-100`)*

Every request — including no-op heartbeats — ends with a call to `ClusterTotals`,
which the collector uses to validate its local count matches the server's. This
runs **two `COUNT(*)` queries** regardless of whether anything changed.

### 3.2 Delta sync — `pkg/database/sync.go`

```go
func (dao *DAO) SyncData(ctx context.Context, event model.SyncEvent,
	clusterName string, syncResponse *model.SyncResponse) error {

	batch := NewBatchWithRetry(ctx, dao, syncResponse)

	// ADD RESOURCES
	// In case of conflict update only if data has changed
	for _, resource := range event.AddResources {
		data, _ := json.Marshal(resource.Properties)
		queueErr = batch.Queue(batchItem{
			action: "addResource",
			query: `INSERT into search.resources as r values($1,$2,$3) ON CONFLICT (uid) 
			DO UPDATE SET data=$3 WHERE r.uid=$1 and r.data IS DISTINCT FROM $3`,
			uid:  resource.UID,
			args: []interface{}{resource.UID, clusterName, string(data)},
		})
	}

	// UPDATE RESOURCES
	for _, resource := range event.UpdateResources {
		data, _ := json.Marshal(resource.Properties)
		queueErr = batch.Queue(batchItem{
			action: "updateResource",
			query:  "UPDATE search.resources SET data=$2 WHERE uid=$1",
			uid:    resource.UID,
			args:   []interface{}{resource.UID, string(data)},
		})
	}

	// DELETE RESOURCES and all edges pointing to the resource.
	if len(event.DeleteResources) > 0 {
		// builds "DELETE from search.resources WHERE uid IN ($1,$2,...)"
		// plus a matching delete against search.edges (sourceId OR destId)
	}

	// ADD EDGES — nothing to update on conflict, resource kind cannot change
	for _, edge := range event.AddEdges {
		queueErr = batch.Queue(batchItem{
			action: "addEdge",
			query:  "INSERT into search.edges values($1,$2,$3,$4,$5,$6) ON CONFLICT (sourceid, destid, edgetype) DO NOTHING",
			args:   []interface{}{edge.SourceUID, edge.SourceKind, edge.DestUID, edge.DestKind, edge.EdgeType, clusterName},
		})
	}

	// DELETE EDGES
	for _, edge := range event.DeleteEdges {
		queueErr = batch.Queue(batchItem{
			action: "deleteEdge",
			query:  "DELETE from search.edges WHERE sourceId=$1 AND destId=$2 AND edgeType=$3",
			args:   []interface{}{edge.SourceUID, edge.DestUID, edge.EdgeType}})
	}

	batch.flush()
	batch.wg.Wait()
	// ...
}
```
*(`pkg/database/sync.go:16-119`, comments/structure preserved, some loops abbreviated)*

Key SQL shapes used here:

```sql
-- Add resource (upsert, but only writes if data actually changed)
INSERT into search.resources as r values($1,$2,$3) ON CONFLICT (uid)
DO UPDATE SET data=$3 WHERE r.uid=$1 and r.data IS DISTINCT FROM $3

-- Update resource (uid/cluster never change once set)
UPDATE search.resources SET data=$2 WHERE uid=$1

-- Delete resources + their edges
DELETE from search.resources WHERE uid IN ($1,$2,...)
DELETE from search.edges WHERE sourceId IN (...) OR destId IN (...)

-- Add edge (no-op on conflict — edge properties never change)
INSERT into search.edges values($1,$2,$3,$4,$5,$6)
ON CONFLICT (sourceid, destid, edgetype) DO NOTHING

-- Delete edge
DELETE from search.edges WHERE sourceId=$1 AND destId=$2 AND edgeType=$3
```

### 3.3 Full resync — `pkg/database/resync.go`

```go
// Reset data for the cluster to the incoming state.
func (dao *DAO) ResyncData(ctx context.Context, clusterName string,
	syncResponse *model.SyncResponse, requestBody []byte) error {

	lastUpsertResource, err := dao.resetResources(ctx, clusterName, syncResponse, requestBody)
	// ...
	err = dao.resetEdges(ctx, clusterName, syncResponse, requestBody)
	// ...
}
```
*(`pkg/database/resync.go:22-48`)*

`resetResources` streams the request body **token-by-token** instead of decoding
it all at once, so a multi-hundred-MB full-state payload never gets fully
buffered:

```go
func (dao *DAO) upsertResources(ctx context.Context, resyncBody []byte, clusterName string,
	syncResponse *model.SyncResponse, batch *batchWithRetry) ([]interface{}, model.Resource, error) {
	dec := json.NewDecoder(bytes.NewReader(resyncBody))
	incomingUIDs := make([]interface{}, 0)
	for {
		field, err := dec.Token()
		if err == io.EOF {
			break
		}
		if field == "addResources" {
			if _, err = dec.Token(); err != nil { /* consume opening [ */ }
			for dec.More() {
				resource := model.Resource{}
				if err = dec.Decode(&resource); err != nil { /* ... */ }
				data, _ := json.Marshal(resource.Properties)
				query, params, err := useGoqu(
					"INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO UPDATE SET data=$3 WHERE data!=$3",
					[]interface{}{resource.UID, clusterName, string(data)})
				if err == nil {
					batch.Queue(batchItem{action: "addResource", query: query, uid: resource.UID, args: params})
					syncResponse.TotalAdded++
				}
				incomingUIDs = append(incomingUIDs, resource.UID)
			}
			return incomingUIDs, resource, err
		}
	}
	return incomingUIDs, resource, nil
}
```
*(`pkg/database/resync.go:177-221`)*

After upserting, stale rows not present in the incoming set are bulk-deleted:

```sql
DELETE from search.resources WHERE cluster=$1 AND uid NOT IN ($2)
DELETE from search.edges WHERE cluster=$1 AND sourceid NOT IN ($2) OR destid NOT IN ($2)
```
*(`pkg/database/resync.go:65-94`)*

**Edge resync diffs instead of delete-all/insert-all.** It first loads all
existing edges for the cluster into an in-memory map, then only inserts what's
missing and deletes what's now orphaned:

```go
func (dao *DAO) resetEdges(ctx context.Context, clusterName string,
	syncResponse *model.SyncResponse, resyncRequest []byte) error {

	batch := NewBatchWithRetry(ctx, dao, syncResponse)
	existingEdgesMap := make(map[string]model.Edge)

	// 1. Get all existing edges for the cluster (excluding interCluster edges).
	query, params, _ := useGoqu(
		"SELECT sourceid, edgetype, destid FROM search.edges WHERE edgetype!='interCluster' AND cluster=$1",
		[]interface{}{clusterName})
	edgeRow, _ := dao.pool.Query(ctx, query, params...)
	for edgeRow.Next() {
		edge := model.Edge{}
		edgeRow.Scan(&edge.SourceUID, &edge.EdgeType, &edge.DestUID)
		existingEdgesMap[edge.SourceUID+edge.EdgeType+edge.DestUID] = edge
	}

	// 2. Insert edges from the request that don't already exist (addEdges removes
	//    matches from existingEdgesMap as it goes).
	addErr := addEdges(resyncRequest, &existingEdgesMap, clusterName, syncResponse, &batch)

	// 3. Whatever remains in existingEdgesMap no longer exists upstream — delete it.
	for _, edge := range existingEdgesMap {
		query, params, _ := useGoqu(
			"DELETE from search.edges WHERE sourceid=$1 AND destid=$2 AND edgetype=$3",
			[]interface{}{edge.SourceUID, edge.DestUID, edge.EdgeType})
		batch.Queue(batchItem{action: "deleteEdge", query: query, args: params})
		syncResponse.TotalEdgesDeleted++
	}

	batch.flush()
	batch.wg.Wait()
	return batch.connError
}
```
*(`pkg/database/resync.go:110-175`, condensed)*

This means resync edge cost scales with **existing edge count for the cluster**,
not just the incoming payload size — there's an upfront read before any write.

### 3.4 The batching engine — `pkg/database/batch.go`

Everything above funnels through this wrapper around `pgx.Batch`:

```go
// This is a wrapper for pgx.Batch. It adds the following:
//  - The Queue() function checks the size of the queued items and automatically triggers the batch processing.
//  - Retry after a batch operation fails. It sends smaller batches to isolate the query producing the error.
//  - Report queries that resulted in errors.

func (b *batchWithRetry) Queue(item batchItem) error {
	if b.connError != nil { // Can't queue more items after DB connection error.
		return b.connError
	}
	b.items = append(b.items, item)

	if len(b.items) >= b.dao.batchSize {
		items := b.items               // Create a snapshot of the items to process.
		b.items = make([]batchItem, 0) // Reset the queue.
		b.wg.Add(1)
		go b.sendBatch(items)
	}
	return nil
}

// Sends a batch to the database. If the batch results in an error, we divide
// the batch into smaller batches and retry until we isolate the erroring query.
func (b *batchWithRetry) sendBatch(items []batchItem) error {
	defer b.wg.Done()

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(item.query, item.args...)
	}
	br := b.dao.pool.SendBatch(b.ctx, batch)
	_, execErr := br.Exec()
	closeErr := br.Close()
	// ... connection-loss detection omitted ...

	// pgx.Batch is processed as a transaction, so in case of an error, the entire batch will fail.
	if execErr != nil && len(items) == 1 {
		// Record the single failing item against the right *Errors slice in syncResponse
		// (AddErrors / UpdateErrors / DeleteErrors / AddEdgeErrors / DeleteEdgeErrors).
		return nil
	} else if execErr != nil {
		// Binary search recursively until we find the error.
		b.wg.Add(2)
		err1 := b.sendBatch(items[:len(items)/2])
		err2 := b.sendBatch(items[len(items)/2:])
		if err1 != nil && err2 != nil {
			return nil
		}
	}
	return execErr
}

// Process all queued items.
func (b *batchWithRetry) flush() {
	if len(b.items) > 0 {
		items := b.items
		b.items = make([]batchItem, 0)
		b.wg.Add(1)
		go b.sendBatch(items)
	}
}
```
*(`pkg/database/batch.go:16-138`, condensed)*

Important properties:

- `Queue()` auto-flushes once `batchSize` (default **2500**) items accumulate.
- Each flush spawns a **goroutine** that sends its batch independently and
  concurrently with other in-flight batches from the same request.
- **A `pgx.Batch` is executed as a single implicit transaction.** If any
  statement in it fails, the *entire batch* fails.
- On failure, the batch is recursively bisected and resent until the offending
  statement is isolated — this can multiply round trips in the failure case.

### 3.5 Parameterized SQL via `goqu` — `pkg/database/goquHelper.go`

Resync-path queries are built with `goqu` (`Prepared(true)`) instead of raw
string formatting, e.g.:

```go
case "INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO UPDATE SET data=$3 WHERE data!=$3":
	q, p, er = dialect.From(resources).Prepared(true).
		Insert().Rows(goqu.Record{"uid": params[0], "cluster": params[1], "data": params[2]}).
		OnConflict(goqu.DoUpdate("uid", goqu.C("data").Set(params[2])).
			Where(resources.Col("data").Neq(params[2]))).ToSQL()
```
*(`pkg/database/goquHelper.go:34-41`)*

### 3.6 Schema & indexes — `pkg/database/connection.go`

```go
func (dao *DAO) InitializeTables(ctx context.Context) {
	dao.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS search")
	dao.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS search.resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)")
	dao.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS search.edges (sourceId TEXT, sourceKind TEXT, destId TEXT, destKind TEXT, edgeType TEXT, cluster TEXT, PRIMARY KEY(sourceId, destId, edgeType))")

	// Jsonb indexing data keys:
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS data_kind_idx ON search.resources USING GIN ((data -> 'kind'))")
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS data_namespace_idx ON search.resources USING GIN ((data -> 'namespace'))")
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS data_name_idx ON search.resources USING GIN ((data ->  'name'))")
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS data_cluster_idx ON search.resources USING btree (cluster)")
	dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_composite_idx ON search.resources USING GIN "+
			"((data -> '_hubClusterResource'::text), (data -> 'namespace'::text), "+
			"(data -> 'apigroup'::text), (data -> 'kind_plural'::text))")
	dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_hubCluster_idx ON search.resources USING GIN "+
			"((data ->  '_hubClusterResource')) WHERE data ? '_hubClusterResource'")

	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS edges_sourceid_idx ON search.edges USING btree (sourceid)")
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS edges_destid_idx ON search.edges USING btree (destid)")
	dao.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS edges_cluster_idx ON search.edges USING btree (cluster)")
}
```
*(`pkg/database/connection.go:115-170`, condensed — `checkError` calls omitted)*

**`search.resources` carries 7 indexes total**: the PK btree on `uid`, a btree
on `cluster`, and 5 GIN indexes over expressions on the `data` JSONB column
(one of which — `data_hubCluster_idx` — is a **partial** index, `WHERE data ?
'_hubClusterResource'`, keeping it small since only cluster pseudo-nodes qualify).

Connection pooling (`pgxpool`):

```go
config.AfterConnect = afterConnect   // Ping new connections before use.
config.BeforeAcquire = beforeAcquire // Ping idle connections before reuse.
config.MaxConnLifetimeJitter = time.Duration(cfg.DBMaxConnLifeJitter) * time.Millisecond // avoid thundering-herd reconnects
config.MaxConns = cfg.DBMaxConns             // default 10
config.MaxConnIdleTime = ...                 // default 5 min
config.MaxConnLifetime = ...                 // default 5 min
config.MinConns = cfg.DBMinConns             // default 2
```
*(`pkg/database/connection.go:83-90`)*

### 3.7 Config knobs — `pkg/config/config.go`

```go
DBBatchSize: getEnvAsInt("DB_BATCH_SIZE", 2500),
DBHost:      getEnv("DB_HOST", "localhost"),
// Postgres has 100 conns by default. Using 10 allows scaling indexer and api.
DBMaxConns:          getEnvAsInt32("DB_MAX_CONNS", int32(10)),
DBMaxConnIdleTime:   getEnvAsInt("DB_MAX_CONN_IDLE_TIME", 5*60*1000),
DBMaxConnLifeJitter: getEnvAsInt("DB_MAX_CONN_LIFE_JITTER", 1*60*1000),
DBMaxConnLifeTime:   getEnvAsInt("DB_MAX_CONN_LIFE_TIME", 5*60*1000),
DBMinConns:          getEnvAsInt32("DB_MIN_CONNS", int32(2)),
RequestLimit:      getEnvAsInt("REQUEST_LIMIT", 25),           // concurrent request cap
LargeRequestLimit: getEnvAsInt("LARGE_REQUEST_LIMIT", 5),      // concurrent cap for large bodies
LargeRequestSize:  getEnvAsInt("LARGE_REQUEST_SIZE", 1024*1024*20), // 20 MB threshold
SlowLog:           getEnvAsInt("SLOW_LOG", 1000), // log ops slower than 1s
```
*(`pkg/config/config.go:55-81`, selected fields)*

### 3.8 Rate limiting ahead of the DB — `pkg/server/requestLimiter.go`, `largeRequestLimiter.go`

Two independent semaphore middlewares protect Postgres from overload:

```go
// requestLimiterMiddleware
if foundClusterProcessing {
	// Reject: a previous request from this cluster is still processing.
	http.Error(w, "A previous request from this cluster is processing, retry later.", http.StatusTooManyRequests)
	return
}
// Give higher priority to requests from the collector in the hub cluster.
hubClusterReq := r.Host == "search-indexer.open-cluster-management.svc:3010"
if requestCount >= config.Cfg.RequestLimit && !hubClusterReq {
	http.Error(w, "Indexer has too many pending requests, retry later.", http.StatusTooManyRequests)
	return
}
```
*(`pkg/server/requestLimiter.go:32-47`, condensed)*

```go
// largeRequestLimiterMiddleware
if r.ContentLength > int64(config.Cfg.LargeRequestSize) {
	if largeRequestCount >= config.Cfg.LargeRequestLimit {
		http.Error(w, "Too many large requests currently processing, retry later.", http.StatusTooManyRequests)
		return
	}
	// track/untrack largeRequestCountTracker for the life of the request
}
```
*(`pkg/server/largeRequestLimiter.go:18-46`, condensed)*

The collector treats HTTP 429 specially as `"indexer busy"` and retries with
exponential backoff + jitter (`search-collector/pkg/send/sender.go:155-180,
221-223, 351-364`) rather than treating it as a hard failure.

### 3.9 Per-request validation query — `pkg/database/dataValidation.go`

```go
// Query resource and edge count for a cluster. Used for data validation.
func (dao *DAO) ClusterTotals(ctx context.Context, clusterName string) (resources int, edges int, e error) {
	batch := &pgx.Batch{}

	// SELECT count(*) FROM search.resources WHERE cluster=$1
	resourceCountSql, params, _ := goqu.From(goqu.S("search").Table("resources")).
		Select(goqu.COUNT("*")).
		Where(goqu.C("cluster").Eq(clusterName), goqu.C("uid").Neq("cluster__"+clusterName)).
		ToSQL()
	batch.Queue(resourceCountSql, params...)

	// SELECT count(*) FROM search.edges WHERE cluster=$1 and edgetype<>'interCluster'
	edgeCountSql, params, _ := goqu.From(goqu.S("search").Table("edges")).
		Select(goqu.COUNT("*")).
		Where(goqu.C("cluster").Eq(clusterName), goqu.C("edgetype").Neq("interCluster")).
		ToSQL()
	batch.Queue(edgeCountSql, params...)

	br := dao.pool.SendBatch(ctx, batch)
	defer br.Close()
	resourcesRow := br.QueryRow()
	resourcesRow.Scan(&resources)
	edgesRow := br.QueryRow()
	edgesRow.Scan(&edges)
	return resources, edges, nil
}
```
*(`pkg/database/dataValidation.go:15-62`, condensed)*

This runs after **every** sync request (delta, resync, or heartbeat), so it's a
small fixed cost layered on top of every write.

## 4. Cost analysis — what makes a batch write expensive

1. **Index maintenance dominates, not network round trips.** `search.resources`
   has 7 indexes (1 PK btree, 1 btree on `cluster`, 5 GIN over `data`
   expressions). Because every write touches `data`, and `data` is what most of
   those GIN indexes are built on, Postgres **cannot use HOT (Heap-Only-Tuple)
   updates** — any accepted change creates a new tuple version and a new entry
   in *all* 7 indexes. GIN index maintenance is inherently more CPU-costly per
   entry than btree (mitigated somewhat by GIN's default "fastupdate" pending
   list, which defers/batches insertion into the main posting tree at the cost
   of a later autovacuum bill).

2. **A whole 2500-item batch is one implicit transaction.** If one statement in
   a batch is malformed, the *entire batch* aborts, triggering the binary-search
   retry in `sendBatch`, which resubmits progressively smaller sub-batches
   (each its own transaction) until the bad statement is isolated. This can
   multiply round trips well above the happy-path cost when errors are present.

3. **Full resync pays extra, unavoidable up-front reads.** `resetEdges` loads
   *every existing edge for the cluster* into memory before it can diff against
   the incoming set — cost here scales with the current DB state, not just the
   size of the incoming payload.

4. **Fixed per-request overhead.** `ClusterTotals`'s two `COUNT(*)` queries run
   after every request — including pure heartbeats with zero changes.

5. **Concurrency vs. pool size.** Each 2500-item batch is sent from its own
   goroutine, and multiple batches from a single large sync run concurrently.
   The connection pool is deliberately small (`MaxConns=10`, shared budget
   against Postgres's own ~100-connection default), so a sync large enough to
   need >10 batches will have goroutines piling up waiting for a free
   connection — the pool acts as an implicit throttle, but at the cost of
   goroutines (and their captured batch payloads) sitting in memory meanwhile.

## 5. Optimizations applied

| Optimization | Where | Effect |
|---|---|---|
| Protocol-level pipelining via `pgx.Batch`/`SendBatch` | `batch.go:69-73` | Statements ship together over the wire without waiting for each individual response — the main defense against per-statement network latency. |
| Change-guarded upserts (`IS DISTINCT FROM` / `!=`) | `sync.go:29-30`, `goquHelper.go:34-41`, `resync.go:200` | Skips the write (and therefore all 7-index maintenance) entirely when incoming data matches what's stored — critical for resync, where most resources are typically unchanged between cycles. |
| Partial GIN index for `_hubClusterResource` | `connection.go:154-157` | Keeps that index small since only cluster pseudo-nodes qualify, rather than indexing every row. |
| Configurable batch granularity (default 2500, `DB_BATCH_SIZE`) | `batch.go:55`, `config.go:23,55` | Balances round-trip savings against blast radius of a single bad statement aborting the whole batch. |
| Diff-based edge resync (read existing, insert delta, delete orphans) | `resync.go:110-175` | Avoids naive delete-all/insert-all, minimizing index churn during a full resync. |
| Streaming JSON decode for resync bodies | `resync.go:177-221`, `resync.go:311-357` | Bounds *memory* for multi-hundred-MB full-state payloads — doesn't reduce DB cost, but prevents the indexer itself from OOMing while building the batch. |
| Per-cluster single-flight + global concurrency cap (default 25, hub-prioritized) | `requestLimiter.go:29-47` | Stops many simultaneous expensive syncs (esp. resyncs) from overwhelming Postgres at once. |
| Separate concurrency cap for large (>20MB) requests (default 5) | `largeRequestLimiter.go` | Extra protection specifically against memory/DB pressure from full-state resyncs. |
| Small, tuned connection pool with health checks + lifetime jitter | `connection.go:46-90`, `config.go:57-61` | Shares Postgres's limited connection budget across indexer + api replicas; jitter avoids simultaneous mass reconnects. |
| `goqu` with `Prepared(true)` + pgx's default per-connection statement cache (not disabled here, unlike `search-v2-api`) | `goquHelper.go` | Amortizes parse/plan cost for repeated identical query shapes within a connection's lifetime. |
| Delta vs. full-resync split | `syncHandler.go:56-69` | Steady-state traffic only ships small deltas; the expensive full-overwrite path is reserved for cold-start/failure recovery. The log message *"This is normal, but it could be a problem if it happens often"* (`resync.go:26`) signals the team is aware resync is the costly path. |

## 6. Notable gaps / things you might expect but aren't there

- **No `COPY` anywhere.** Even full resyncs — which can carry an entire
  cluster's inventory — go through row-at-a-time `INSERT ... ON CONFLICT`
  statements pipelined in batches of 2500, not Postgres's bulk-load `COPY`
  protocol, which would be substantially faster for that specific case.
- **No multi-row `VALUES` lists.** Each statement inserts/updates exactly one
  row; statements aren't consolidated into multi-row inserts even within a
  batch.
- **`ClusterTotals`'s two `COUNT(*)` queries run unconditionally**, including on
  pure heartbeats, adding fixed overhead to every request regardless of
  whether anything changed.
- **`DBHealthCkeckPeriod` config field is defined but unused** — `config.go:24`
  documents it as overriding `pgxpool.Config{ HealthCheckPeriod }`, but
  `initializePool` (`connection.go:63-113`) never actually sets it on the pool
  config. Minor, unrelated to write cost, but worth knowing if tuning health
  checks.

## 7. File map (for further reading)

| File | Contains |
|---|---|
| `pkg/server/syncHandler.go` | HTTP entry point, dispatch to sync vs. resync |
| `pkg/database/sync.go` | Delta sync SQL construction |
| `pkg/database/resync.go` | Full resync orchestration, streaming decode, edge diffing |
| `pkg/database/batch.go` | `pgx.Batch` wrapper: auto-flush, retry/bisect on error |
| `pkg/database/goquHelper.go` | Parameterized SQL builder for resync-path queries |
| `pkg/database/connection.go` | Schema/index DDL, `pgxpool` setup |
| `pkg/database/dataValidation.go` | `ClusterTotals` post-request count validation |
| `pkg/database/upsertCluster.go` | Cluster pseudo-node upsert; transactional cluster delete |
| `pkg/config/config.go` | All environment-driven tuning knobs |
| `pkg/server/requestLimiter.go` | Per-cluster + global concurrent request cap |
| `pkg/server/largeRequestLimiter.go` | Concurrency cap for >20MB request bodies |
| `pkg/metrics/slowLog.go` | `SlowLog`/`LogStepDuration` timing instrumentation |
| `search-collector/pkg/send/sender.go` | Collector-side payload construction + HTTP send/retry |
| `docs/ARCHITECTURE.md` | High-level architecture (this doc's companion) |
