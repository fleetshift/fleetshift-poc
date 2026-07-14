package kubernetes

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// edgeKey identifies a unique edge in the diff map.
// No current code path needs O(1) lookup by source UID alone (e.g. "delete all
// edges from source X"). If that changes, switch to map[string]map[edgeKey]Edge
// keyed by sourceUID for the outer map, or add a secondary index.
type edgeKey struct {
	SourceUID string
	DestUID   string
	EdgeType  EdgeType
}

// gvrState is the writer's per-GVR process-generation state. ReportedUIDs
// is the persistence acknowledgement baseline: it changes only after a
// successful mixed ReplaceBatch. Generation fencing rejects late events
// after RemoveGVR closes the generation.
type gvrState struct {
	Generation   uint64
	ReportedUIDs map[string]struct{}
}

// pendingResync holds a failed LIST/resync mixed batch retained until it
// succeeds or the GVR generation ends. Unlike watch flushes (which stay
// in pendingUpserts/pendingDeletes), resync has no natural retry from a
// healthy watch, so the writer must retain and retry here. Ack is signaled
// only after success or generation end so the informer can start WATCH.
type pendingResync struct {
	generation    uint64
	delta         InventoryDeltaReport
	nextNodes     map[string]inventoryNode
	nextEdgeFuncs map[string]func(NodeStore) []Edge
	staleUIDs     []string
	currentUIDs   map[string]struct{}
	ack           chan<- error
}

// errResyncGenerationClosed is sent on ResyncEvent.Ack when the GVR
// generation ends before the LIST write commits.
var errResyncGenerationClosed = errors.New("inventory resync generation closed")

// ackResync delivers a single result on ack if present. Non-blocking:
// a canceled informer may no longer be waiting.
func ackResync(ack chan<- error, err error) {
	if ack == nil {
		return
	}
	select {
	case ack <- err:
	default:
	}
}

// Writer batches informer events and reports them through an
// [InventoryReporter]. Topology edge deltas are computed in memory and
// delivered to an [EdgeSink] (typically [NoopEdgeSink]); they never
// flow through the inventory reporter.
type Writer struct {
	targetID      string
	reporter      InventoryReporter
	edgeSink      EdgeSink
	gvrStates     map[schema.GroupVersionResource]*gvrState
	pendingResync map[schema.GroupVersionResource]*pendingResync
	// closedGens records the highest generation closed per GVR so late
	// events for a closed generation cannot re-open it.
	closedGens    map[schema.GroupVersionResource]uint64
	schema        map[schema.GroupVersionResource]SchemaEntry
	eventCh       chan ResourceEvent
	resyncCh      chan ResyncEvent
	removeCh      chan RemoveGVREvent
	batchInterval time.Duration
	currentNodes  map[string]inventoryNode
	edgeFuncs     map[string]func(NodeStore) []Edge
	previousEdges map[edgeKey]Edge
	logger        *slog.Logger

	// stopCh requests a shutdown flush under a caller-provided context.
	// Buffered so Stop does not block if Run has already exited.
	stopCh chan context.Context
}

// defaultWriterShutdownFlushTimeout is used when Run's context is canceled
// without an explicit [Writer.Stop] flush context (and without a deadline).
const defaultWriterShutdownFlushTimeout = 5 * time.Second

// NewWriter creates a Writer that batches events over batchInterval and
// reports them via reporter. If edgeSink is nil, [NoopEdgeSink] is used.
func NewWriter(
	targetID string,
	reporter InventoryReporter,
	edgeSink EdgeSink,
	schemaEntries map[schema.GroupVersionResource]SchemaEntry,
	batchInterval time.Duration,
	logger *slog.Logger,
) *Writer {
	if edgeSink == nil {
		edgeSink = NoopEdgeSink{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Writer{
		targetID:      targetID,
		reporter:      reporter,
		edgeSink:      edgeSink,
		schema:        schemaEntries,
		eventCh:       make(chan ResourceEvent, 256),
		resyncCh:      make(chan ResyncEvent, 16),
		removeCh:      make(chan RemoveGVREvent, 16),
		batchInterval: batchInterval,
		currentNodes:  make(map[string]inventoryNode),
		edgeFuncs:     make(map[string]func(NodeStore) []Edge),
		previousEdges: make(map[edgeKey]Edge),
		gvrStates:     make(map[schema.GroupVersionResource]*gvrState),
		pendingResync: make(map[schema.GroupVersionResource]*pendingResync),
		closedGens:    make(map[schema.GroupVersionResource]uint64),
		logger:        logger,
		stopCh:        make(chan context.Context, 1),
	}
}

// EventCh returns the channel callers use to submit resource events.
func (w *Writer) EventCh() chan<- ResourceEvent { return w.eventCh }

// ResyncCh returns the channel callers use to submit resync events.
func (w *Writer) ResyncCh() chan<- ResyncEvent { return w.resyncCh }

// RemoveCh returns the channel callers use to signal that a GVR is no
// longer being indexed. Removal is non-destructive to persisted
// inventory: the writer drops in-memory state for that GVR only.
func (w *Writer) RemoveCh() chan<- RemoveGVREvent { return w.removeCh }

// Stop requests that Run perform a final flush under flushCtx and then
// return. It is safe to call Stop more than once; only the first request
// is delivered. Prefer Stop over canceling Run's context when a shared
// shutdown deadline must govern the final flush.
func (w *Writer) Stop(flushCtx context.Context) {
	if flushCtx == nil {
		flushCtx = context.Background()
	}
	select {
	case w.stopCh <- flushCtx:
	default:
	}
}

// Run starts the event loop. It blocks until Stop is called or ctx is
// cancelled, flushing any remaining batch before returning. Informer
// shutdown flushes real pending object events only; it does not turn
// local cache eviction into persisted deletes. RemoveGVR also leaves
// persisted inventory unchanged (in-memory drop only).
func (w *Writer) Run(ctx context.Context) {
	batchTicker := time.NewTicker(w.batchInterval)
	defer batchTicker.Stop()

	// Pending batch state.
	pendingUpserts := make(map[string]*unstructured.Unstructured) // UID -> resource
	pendingUpsertGVR := make(map[string]schema.GroupVersionResource)
	pendingDeletes := make(map[string]schema.GroupVersionResource) // UID -> GVR

	// Dedup: tracks UID -> last-sent resourceVersion.
	sentVersions := make(map[string]string)

	flushAndReturn := func(flushCtx context.Context) {
		if flushCtx == nil {
			flushCtx = context.Background()
		}
		// Best-effort only: Run is exiting and has no caller to return to.
		// applyDeltaWithRetry already logs; a still-failing batch stays in
		// pendingResync until process teardown drops it.
		_ = w.retryPendingResyncs(flushCtx)
		w.flush(flushCtx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions)
	}

	for {
		select {
		case flushCtx := <-w.stopCh:
			flushAndReturn(flushCtx)
			return

		case <-ctx.Done():
			flushCtx, flushCancel := writerShutdownFlushContext(ctx)
			flushAndReturn(flushCtx)
			flushCancel()
			return

		case ev := <-w.eventCh:
			if !w.acceptGeneration(ev.GVR, ev.Generation) {
				continue
			}
			uid := string(ev.Resource.GetUID())

			switch ev.Op {
			case EventAdd, EventUpdate:
				// Late-delete protection: if this UID was deleted in the
				// current batch, drop the add/update.
				if _, deleted := pendingDeletes[uid]; deleted {
					continue
				}
				pendingUpserts[uid] = ev.Resource
				pendingUpsertGVR[uid] = ev.GVR

			case EventDelete:
				pendingDeletes[uid] = ev.GVR
				// Remove any pending upsert for this UID — the delete wins.
				delete(pendingUpserts, uid)
				delete(pendingUpsertGVR, uid)
				// Clear sent version so a future add for this UID is not
				// deduped against the deleted resource.
				delete(sentVersions, uid)
				// Drop local edge state immediately so topology tracks the
				// watch tombstone; ReportedUIDs still waits for flush
				// success before removing the UID.
				delete(w.currentNodes, uid)
				delete(w.edgeFuncs, uid)
			}

		case rs := <-w.resyncCh:
			if !w.acceptGeneration(rs.GVR, rs.Generation) {
				ackResync(rs.Ack, errResyncGenerationClosed)
				continue
			}
			w.sendResync(ctx, rs)

		case rm := <-w.removeCh:
			// Drop any pending work for this GVR so a later flush cannot
			// resurrect objects after the GVR generation is closed.
			for uid, gvr := range pendingUpsertGVR {
				if gvr == rm.GVR {
					delete(pendingUpserts, uid)
					delete(pendingUpsertGVR, uid)
				}
			}
			for uid, gvr := range pendingDeletes {
				if gvr == rm.GVR {
					delete(pendingDeletes, uid)
				}
			}
			w.closeGeneration(rm.GVR, rm.Generation)

		case <-batchTicker.C:
			// Failure keeps the entry in pendingResync for the next tick;
			// the returned error does not change Run's control flow.
			_ = w.retryPendingResyncs(ctx)
			if err := w.flush(ctx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions); err == nil {
				pendingUpserts = make(map[string]*unstructured.Unstructured)
				pendingUpsertGVR = make(map[string]schema.GroupVersionResource)
				pendingDeletes = make(map[string]schema.GroupVersionResource)
			}
		}
	}
}

// writerShutdownFlushContext derives a flush context after Run's context is
// canceled. Prefer any remaining deadline on ctx; otherwise use the default
// shutdown flush timeout.
func writerShutdownFlushContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok {
		return context.WithDeadline(context.Background(), dl)
	}
	return context.WithTimeout(context.Background(), defaultWriterShutdownFlushTimeout)
}

// acceptGeneration returns whether events for (gvr, gen) should be
// processed. Generation 0 is untagged (tests) and always accepted for
// an open GVR slot. A newer generation replaces an older open one; a
// stale or closed generation is rejected.
func (w *Writer) acceptGeneration(gvr schema.GroupVersionResource, gen uint64) bool {
	if gen != 0 {
		if closed, ok := w.closedGens[gvr]; ok && gen <= closed {
			return false
		}
	}
	st := w.gvrStates[gvr]
	if gen == 0 {
		if st == nil {
			w.gvrStates[gvr] = &gvrState{
				Generation:   0,
				ReportedUIDs: make(map[string]struct{}),
			}
		}
		return true
	}
	if st == nil {
		w.gvrStates[gvr] = &gvrState{
			Generation:   gen,
			ReportedUIDs: make(map[string]struct{}),
		}
		return true
	}
	if st.Generation == gen {
		return true
	}
	if gen > st.Generation {
		// Fast re-add installed a newer generation before/without a
		// matching RemoveGVR for the old one; adopt the new baseline.
		w.dropGVRMemory(gvr)
		w.gvrStates[gvr] = &gvrState{
			Generation:   gen,
			ReportedUIDs: make(map[string]struct{}),
		}
		if p, ok := w.pendingResync[gvr]; ok {
			ackResync(p.ack, errResyncGenerationClosed)
			delete(w.pendingResync, gvr)
		}
		return true
	}
	return false
}

// closeGeneration discards in-memory state for gen. If the writer has
// already moved to a newer generation for gvr, the close is ignored.
func (w *Writer) closeGeneration(gvr schema.GroupVersionResource, gen uint64) {
	st := w.gvrStates[gvr]
	if st == nil {
		if gen != 0 {
			if closed, ok := w.closedGens[gvr]; !ok || gen > closed {
				w.closedGens[gvr] = gen
			}
		}
		if p, ok := w.pendingResync[gvr]; ok {
			ackResync(p.ack, errResyncGenerationClosed)
			delete(w.pendingResync, gvr)
		}
		return
	}
	if gen != 0 && st.Generation != gen {
		return
	}
	if gen != 0 {
		w.closedGens[gvr] = gen
	} else if st.Generation != 0 {
		w.closedGens[gvr] = st.Generation
	}
	w.dropGVRMemory(gvr)
	delete(w.gvrStates, gvr)
	if p, ok := w.pendingResync[gvr]; ok {
		ackResync(p.ack, errResyncGenerationClosed)
		delete(w.pendingResync, gvr)
	}
}

// dropGVRMemory clears edge/node state for gvr without touching
// persisted inventory.
func (w *Writer) dropGVRMemory(gvr schema.GroupVersionResource) {
	for uid, node := range w.currentNodes {
		if node.GVR == gvr {
			delete(w.currentNodes, uid)
			delete(w.edgeFuncs, uid)
		}
	}
}

// flush sends the accumulated batch as a single ApplyDelta call. It applies
// dedup by skipping upserts whose resourceVersion has not changed.
// Returns error if the write fails; ReportedUIDs / sentVersions advance
// only on success.
func (w *Writer) flush(
	ctx context.Context,
	upserts map[string]*unstructured.Unstructured,
	upsertGVR map[string]schema.GroupVersionResource,
	deletes map[string]schema.GroupVersionResource,
	sentVersions map[string]string,
) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	var reports []InventoryObjectReport
	newSentVersions := make(map[string]string)
	upsertedUIDs := make(map[string]schema.GroupVersionResource)
	prevNodes := make(map[string]inventoryNode)
	prevEdgeFuncs := make(map[string]func(NodeStore) []Edge)
	hadPrevNode := make(map[string]bool)
	hadPrevEdgeFn := make(map[string]bool)

	for uid, r := range upserts {
		rv := r.GetResourceVersion()
		// Dedup: skip if we already sent this exact version.
		if lastRV, ok := sentVersions[uid]; ok && lastRV == rv {
			continue
		}

		gvr := upsertGVR[uid]
		entry := w.schemaEntry(gvr)
		report, node, err := ExtractObservedResource(r, entry, w.targetID)
		if err != nil {
			w.logger.Warn("skipping upsert; extraction failed",
				"uid", uid,
				"gvr", gvr.String(),
				"error", err)
			continue
		}
		node.GVR = gvr
		reports = append(reports, report)
		newSentVersions[uid] = rv
		upsertedUIDs[uid] = gvr

		// Uncommitted edge-state update for diffEdges; restored on write
		// failure so a retry does not lose a previously acknowledged node.
		if existing, ok := w.currentNodes[uid]; ok {
			prevNodes[uid] = existing
			hadPrevNode[uid] = true
		}
		if existing, ok := w.edgeFuncs[uid]; ok {
			prevEdgeFuncs[uid] = existing
			hadPrevEdgeFn[uid] = true
		}
		w.currentNodes[uid] = node
		if entry.BuildEdges != nil {
			w.edgeFuncs[uid] = entry.BuildEdges(r, uid)
		} else {
			delete(w.edgeFuncs, uid)
		}
	}

	var deleteReports []InventoryObjectReport
	deletedUIDs := make(map[string]schema.GroupVersionResource)
	for uid, gvr := range deletes {
		report, err := w.deleteReport(gvr, uid)
		if err != nil {
			w.logger.Warn("skipping delete; resource name construction failed",
				"uid", uid,
				"gvr", gvr.String(),
				"error", err)
			continue
		}
		deleteReports = append(deleteReports, report)
		deletedUIDs[uid] = gvr
	}

	if len(reports) == 0 && len(deleteReports) == 0 {
		return nil
	}

	edgeAdds, edgeDels, newEdges := w.diffEdges()

	if w.reporter == nil {
		w.acknowledgeFlush(upsertedUIDs, deletedUIDs)
		maps.Copy(sentVersions, newSentVersions)
		w.previousEdges = newEdges
		return nil
	}

	delta := InventoryDeltaReport{
		Upserts: reports,
		Deletes: deleteReports,
	}
	if err := w.applyDeltaWithRetry(ctx, delta); err != nil {
		w.restoreUncommittedNodes(upsertedUIDs, prevNodes, prevEdgeFuncs, hadPrevNode, hadPrevEdgeFn)
		return err
	}

	if len(edgeAdds) > 0 || len(edgeDels) > 0 {
		if err := w.edgeSink.ApplyEdgeDelta(ctx, domain.TargetID(w.targetID), EdgeDelta{
			Adds:    edgeAdds,
			Deletes: edgeDels,
		}); err != nil {
			w.logger.Warn("edge sink ApplyEdgeDelta failed", "error", err)
			w.restoreUncommittedNodes(upsertedUIDs, prevNodes, prevEdgeFuncs, hadPrevNode, hadPrevEdgeFn)
			return err
		}
	}

	w.acknowledgeFlush(upsertedUIDs, deletedUIDs)
	maps.Copy(sentVersions, newSentVersions)
	w.previousEdges = newEdges
	return nil
}

// restoreUncommittedNodes rolls back currentNodes/edgeFuncs for UIDs that
// were updated before a failed ApplyDelta/ApplyEdgeDelta so a later retry
// still diffs from the last acknowledged edge state.
func (w *Writer) restoreUncommittedNodes(
	upsertedUIDs map[string]schema.GroupVersionResource,
	prevNodes map[string]inventoryNode,
	prevEdgeFuncs map[string]func(NodeStore) []Edge,
	hadPrevNode map[string]bool,
	hadPrevEdgeFn map[string]bool,
) {
	for uid := range upsertedUIDs {
		if hadPrevNode[uid] {
			w.currentNodes[uid] = prevNodes[uid]
		} else {
			delete(w.currentNodes, uid)
		}
		if hadPrevEdgeFn[uid] {
			w.edgeFuncs[uid] = prevEdgeFuncs[uid]
		} else {
			delete(w.edgeFuncs, uid)
		}
	}
}

// acknowledgeFlush advances ReportedUIDs after a successful watch flush:
// upserted UIDs are added, deleted UIDs are removed. Call only after the
// mixed ReplaceBatch (and any edge sink write) has succeeded.
func (w *Writer) acknowledgeFlush(
	upsertedUIDs map[string]schema.GroupVersionResource,
	deletedUIDs map[string]schema.GroupVersionResource,
) {
	for uid, gvr := range upsertedUIDs {
		st := w.gvrStates[gvr]
		if st == nil {
			st = &gvrState{ReportedUIDs: make(map[string]struct{})}
			w.gvrStates[gvr] = st
		}
		st.ReportedUIDs[uid] = struct{}{}
	}
	for uid, gvr := range deletedUIDs {
		if st := w.gvrStates[gvr]; st != nil {
			delete(st.ReportedUIDs, uid)
		}
	}
}

// applyDeltaWithRetry applies a delta with exponential backoff retry.
// It retries up to 3 times with 1s, 2s, 4s backoff. Returns the error
// if all retries fail. Empty deltas are not sent — idle flushes are not
// heartbeats.
func (w *Writer) applyDeltaWithRetry(ctx context.Context, delta InventoryDeltaReport) error {
	if w.reporter == nil {
		return nil
	}
	if len(delta.Upserts) == 0 && len(delta.Deletes) == 0 {
		return nil
	}

	var err error
	for attempt := range 3 {
		err = w.reporter.ApplyDelta(ctx, delta)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		w.logger.Warn("ApplyDelta failed, retrying",
			"attempt", attempt+1,
			"backoff", backoff,
			"error", err)

		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
	}

	w.logger.Error("ApplyDelta failed after retries", "error", err)
	return err
}

// sendResync upserts the LIST snapshot and deletes only UIDs in this
// generation's ReportedUIDs that are absent from the LIST. That is
// same-process omission reconciliation; it does not discover
// database-only rows from an earlier process. ReportedUIDs and
// in-memory membership update only after the write succeeds. On
// failure the mixed batch is retained and retried until success or
// generation end; ResyncEvent.Ack is signaled only then so the
// informer does not start WATCH from an uncommitted LIST cursor.
func (w *Writer) sendResync(ctx context.Context, rs ResyncEvent) {
	entry := w.schemaEntry(rs.GVR)

	resyncUIDs := make(map[string]struct{})
	var upserts []InventoryObjectReport
	nextNodes := make(map[string]inventoryNode)
	nextEdgeFuncs := make(map[string]func(NodeStore) []Edge)

	for _, r := range rs.Resources {
		report, node, err := ExtractObservedResource(r, entry, w.targetID)
		if err != nil {
			w.logger.Warn("skipping resync item; extraction failed",
				"uid", string(r.GetUID()),
				"gvr", rs.GVR.String(),
				"error", err)
			continue
		}
		node.GVR = rs.GVR
		upserts = append(upserts, report)
		uid := string(r.GetUID())
		resyncUIDs[uid] = struct{}{}
		nextNodes[uid] = node
		if entry.BuildEdges != nil {
			nextEdgeFuncs[uid] = entry.BuildEdges(r, uid)
		}
	}

	st := w.gvrStates[rs.GVR]
	var deletes []InventoryObjectReport
	var staleUIDs []string
	if st != nil {
		for uid := range st.ReportedUIDs {
			if _, exists := resyncUIDs[uid]; exists {
				continue
			}
			report, err := w.deleteReport(rs.GVR, uid)
			if err != nil {
				w.logger.Warn("skipping resync delete; resource name construction failed",
					"uid", uid,
					"gvr", rs.GVR.String(),
					"error", err)
				continue
			}
			deletes = append(deletes, report)
			staleUIDs = append(staleUIDs, uid)
		}
	}

	// A newer LIST for the same GVR replaces any retained prior batch;
	// nack the old waiter so it can relist rather than hang.
	if prev, ok := w.pendingResync[rs.GVR]; ok {
		ackResync(prev.ack, errResyncGenerationClosed)
	}

	pending := &pendingResync{
		generation:    rs.Generation,
		delta:         InventoryDeltaReport{Upserts: upserts, Deletes: deletes},
		nextNodes:     nextNodes,
		nextEdgeFuncs: nextEdgeFuncs,
		staleUIDs:     staleUIDs,
		currentUIDs:   resyncUIDs,
		ack:           rs.Ack,
	}

	if err := w.applyDeltaWithRetry(ctx, pending.delta); err != nil {
		// Retain for ticker retry; do not advance ReportedUIDs or ack yet.
		w.pendingResync[rs.GVR] = pending
		return
	}
	delete(w.pendingResync, rs.GVR)
	w.applyResyncSuccess(rs.GVR, pending)
	ackResync(pending.ack, nil)
}

// applyResyncSuccess commits in-memory membership and ReportedUIDs for a
// successful LIST/resync write. Call only after ApplyDelta succeeds (or
// when there is no reporter).
func (w *Writer) applyResyncSuccess(gvr schema.GroupVersionResource, pending *pendingResync) {
	maps.Copy(w.currentNodes, pending.nextNodes)
	maps.Copy(w.edgeFuncs, pending.nextEdgeFuncs)
	for _, uid := range pending.staleUIDs {
		delete(w.currentNodes, uid)
		delete(w.edgeFuncs, uid)
	}

	st := w.gvrStates[gvr]
	if st == nil {
		st = &gvrState{
			Generation:   pending.generation,
			ReportedUIDs: make(map[string]struct{}),
		}
		w.gvrStates[gvr] = st
	}
	st.ReportedUIDs = maps.Clone(pending.currentUIDs)
	if st.ReportedUIDs == nil {
		st.ReportedUIDs = make(map[string]struct{})
	}
}

// retryPendingResyncs retries retained LIST writes. Membership /
// ReportedUIDs advance only on success. RemoveGVR / generation close
// drops pending entries without synthesizing deletes and nacks waiters.
func (w *Writer) retryPendingResyncs(ctx context.Context) error {
	var firstErr error
	for gvr, pending := range w.pendingResync {
		st := w.gvrStates[gvr]
		if st == nil || (pending.generation != 0 && st.Generation != pending.generation) {
			ackResync(pending.ack, errResyncGenerationClosed)
			delete(w.pendingResync, gvr)
			continue
		}
		if err := w.applyDeltaWithRetry(ctx, pending.delta); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		delete(w.pendingResync, gvr)
		w.applyResyncSuccess(gvr, pending)
		ackResync(pending.ack, nil)
	}
	return firstErr
}

func (w *Writer) schemaEntry(gvr schema.GroupVersionResource) SchemaEntry {
	entry := w.schema[gvr]
	// Missing schema entries still need the watched GVR on the entry so
	// extraction can build ObjectResourceName / labels correctly.
	if entry.GVR.Empty() {
		entry.GVR = gvr
	}
	return entry
}

// deleteReport builds an exact-name IsDelete [InventoryObjectReport]
// for one previously reported UID.
func (w *Writer) deleteReport(gvr schema.GroupVersionResource, uid string) (InventoryObjectReport, error) {
	name, err := ObjectResourceName(KubernetesObjectIdentity{
		TargetID: domain.TargetID(w.targetID),
		GVR:      gvr,
		UID:      uid,
	})
	if err != nil {
		return InventoryObjectReport{}, err
	}
	return InventoryObjectReport{
		Name:     name,
		IsDelete: true,
	}, nil
}

func (w *Writer) diffEdges() (adds, dels []Edge, newEdges map[edgeKey]Edge) {
	ns := buildNodeStore(w.currentNodes)
	newEdges = make(map[edgeKey]Edge)

	// Type-specific edges from BuildEdges closures.
	for _, edgeFn := range w.edgeFuncs {
		for _, e := range edgeFn(ns) {
			newEdges[edgeKey{e.SourceUID, e.DestUID, e.EdgeType}] = e
		}
	}

	// Common edges (ownedBy traversal) for ALL nodes.
	for uid := range w.currentNodes {
		for _, e := range commonEdges(uid, ns) {
			newEdges[edgeKey{e.SourceUID, e.DestUID, e.EdgeType}] = e
		}
	}

	for key, edge := range newEdges {
		if _, ok := w.previousEdges[key]; !ok {
			adds = append(adds, edge)
		}
	}
	for key, edge := range w.previousEdges {
		if _, ok := newEdges[key]; !ok {
			dels = append(dels, edge)
		}
	}
	return adds, dels, newEdges
}
