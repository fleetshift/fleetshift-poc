package kubernetes

import (
	"context"
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

// Writer batches informer events and reports them through an
// [InventoryReporter]. Topology edge deltas are computed in memory and
// delivered to an [EdgeSink] (typically [NoopEdgeSink]); they never
// flow through the inventory reporter.
type Writer struct {
	targetID      string
	reporter      InventoryReporter
	edgeSink      EdgeSink
	schema        map[schema.GroupVersionResource]SchemaEntry
	eventCh       chan ResourceEvent
	resyncCh      chan ResyncEvent
	removeCh      chan RemoveGVREvent
	batchInterval time.Duration
	currentNodes  map[string]inventoryNode
	edgeFuncs     map[string]func(NodeStore) []Edge
	previousEdges map[edgeKey]Edge
	logger        *slog.Logger
}

// NewWriter creates a Writer that batches events over batchInterval and
// reports them via reporter. If edgeSink is nil, [NoopEdgeSink] is used.
func NewWriter(
	targetID string,
	reporter InventoryReporter,
	edgeSink EdgeSink,
	schema map[schema.GroupVersionResource]SchemaEntry,
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
		schema:        schema,
		eventCh:       make(chan ResourceEvent, 256),
		resyncCh:      make(chan ResyncEvent, 16),
		removeCh:      make(chan RemoveGVREvent, 16),
		batchInterval: batchInterval,
		currentNodes:  make(map[string]inventoryNode),
		edgeFuncs:     make(map[string]func(NodeStore) []Edge),
		previousEdges: make(map[edgeKey]Edge),
		logger:        logger,
	}
}

// EventCh returns the channel callers use to submit resource events.
func (w *Writer) EventCh() chan<- ResourceEvent { return w.eventCh }

// ResyncCh returns the channel callers use to submit resync events.
func (w *Writer) ResyncCh() chan<- ResyncEvent { return w.resyncCh }

// RemoveCh returns the channel callers use to signal that a GVR is no
// longer being indexed and its inventory collection should be deleted.
func (w *Writer) RemoveCh() chan<- RemoveGVREvent { return w.removeCh }

// Run starts the event loop. It blocks until ctx is cancelled, flushing any
// remaining batch before returning. Informer shutdown flushes real pending
// object events only; it does not turn local cache eviction into persisted
// deletes, and it does not emit RemoveGVR / DeleteCollection commands.
func (w *Writer) Run(ctx context.Context) {
	batchTicker := time.NewTicker(w.batchInterval)
	defer batchTicker.Stop()

	// Pending batch state.
	pendingUpserts := make(map[string]*unstructured.Unstructured) // UID -> resource
	pendingUpsertGVR := make(map[string]schema.GroupVersionResource)
	pendingDeletes := make(map[string]schema.GroupVersionResource) // UID -> GVR

	// Dedup: tracks UID -> last-sent resourceVersion.
	sentVersions := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			w.flush(shutdownCtx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions)
			shutdownCancel()
			return

		case ev := <-w.eventCh:
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
				// Remove from edge state.
				delete(w.currentNodes, uid)
				delete(w.edgeFuncs, uid)
			}

		case rs := <-w.resyncCh:
			w.sendResync(ctx, rs)

		case rm := <-w.removeCh:
			// Drop any pending work for this GVR so a later flush cannot
			// resurrect objects whose collection is about to be deleted.
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
			w.removeGVR(ctx, rm.GVR)

		case <-batchTicker.C:
			if err := w.flush(ctx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions); err == nil {
				pendingUpserts = make(map[string]*unstructured.Unstructured)
				pendingUpsertGVR = make(map[string]schema.GroupVersionResource)
				pendingDeletes = make(map[string]schema.GroupVersionResource)
			}
		}
	}
}

// flush sends the accumulated batch as a single ApplyDelta call. It applies
// dedup by skipping upserts whose resourceVersion has not changed.
// Returns error if the write fails; state is only advanced on success.
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

		// Track inventory node for edge computation.
		w.currentNodes[uid] = node

		if entry.BuildEdges != nil {
			w.edgeFuncs[uid] = entry.BuildEdges(r, uid)
		}
	}

	var deletedRefs []domain.InventoryResourceRef
	for uid, gvr := range deletes {
		ref, err := w.resourceRef(gvr, uid)
		if err != nil {
			w.logger.Warn("skipping delete; resource name construction failed",
				"uid", uid,
				"gvr", gvr.String(),
				"error", err)
			continue
		}
		deletedRefs = append(deletedRefs, ref)
	}

	if len(reports) == 0 && len(deletedRefs) == 0 {
		return nil
	}

	edgeAdds, edgeDels, newEdges := w.diffEdges()

	if w.reporter == nil {
		return nil
	}

	delta := InventoryDeltaReport{
		Upserts: reports,
		Deletes: deletedRefs,
	}
	if err := w.applyDeltaWithRetry(ctx, delta); err != nil {
		return err
	}

	if len(edgeAdds) > 0 || len(edgeDels) > 0 {
		if err := w.edgeSink.ApplyEdgeDelta(ctx, domain.TargetID(w.targetID), EdgeDelta{
			Adds:    edgeAdds,
			Deletes: edgeDels,
		}); err != nil {
			w.logger.Warn("edge sink ApplyEdgeDelta failed", "error", err)
			return err
		}
	}

	maps.Copy(sentVersions, newSentVersions)
	w.previousEdges = newEdges
	return nil
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

// sendResync replaces the exact target+GVR inventory collection.
//
// Resync handles items only — edges are not written. Edge computation
// is deferred to the flush path, which runs ALL edge closures against
// the full w.currentNodes on every tick. This avoids a class of race
// where a GVR resync runs before cross-GVR dependencies are in
// w.currentNodes (e.g. Pod resync before ReplicaSets are known),
// which would delete correct edges and fail to re-create them.
func (w *Writer) sendResync(ctx context.Context, rs ResyncEvent) {
	entry := w.schemaEntry(rs.GVR)

	resyncUIDs := make(map[string]struct{})
	var reports []InventoryObjectReport

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
		reports = append(reports, report)
		uid := string(r.GetUID())
		resyncUIDs[uid] = struct{}{}

		w.currentNodes[uid] = node
		if entry.BuildEdges != nil {
			w.edgeFuncs[uid] = entry.BuildEdges(r, uid)
		}
	}

	for uid, node := range w.currentNodes {
		if node.GVR == rs.GVR {
			if _, exists := resyncUIDs[uid]; !exists {
				delete(w.currentNodes, uid)
				delete(w.edgeFuncs, uid)
			}
		}
	}

	collection, err := ObjectCollectionName(domain.TargetID(w.targetID), rs.GVR)
	if err != nil {
		w.logger.Error("resync aborted; collection name construction failed",
			"gvr", rs.GVR.String(),
			"error", err)
		return
	}

	if w.reporter == nil {
		return
	}
	w.replaceCollectionWithRetry(ctx, InventoryCollectionSnapshot{
		Collection: collection,
		Reports:    reports,
	})
}

func (w *Writer) replaceCollectionWithRetry(ctx context.Context, snapshot InventoryCollectionSnapshot) {
	var err error
	for attempt := range 3 {
		err = w.reporter.ReplaceCollection(ctx, snapshot)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}

		backoff := time.Duration(1<<attempt) * time.Second
		w.logger.Warn("ReplaceCollection failed, retrying",
			"attempt", attempt+1,
			"backoff", backoff,
			"error", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}

	w.logger.Error("ReplaceCollection failed after retries", "error", err)
}

// removeGVR deletes the exact target+GVR inventory collection and drops
// in-memory nodes/edge closures for that GVR. It does not persist edge
// deletes; the next flush recomputes topology against the remaining
// nodes. Informer shutdown must not call this path.
func (w *Writer) removeGVR(ctx context.Context, gvr schema.GroupVersionResource) {
	for uid, node := range w.currentNodes {
		if node.GVR == gvr {
			delete(w.currentNodes, uid)
			delete(w.edgeFuncs, uid)
		}
	}

	collection, err := ObjectCollectionName(domain.TargetID(w.targetID), gvr)
	if err != nil {
		w.logger.Error("GVR removal aborted; collection name construction failed",
			"gvr", gvr.String(),
			"error", err)
		return
	}

	if w.reporter == nil {
		return
	}

	ref := domain.InventoryCollectionRef{
		ResourceType: ObjectResourceType,
		Collection:   collection,
	}
	var deleteErr error
	for attempt := range 3 {
		deleteErr = w.reporter.DeleteCollection(ctx, ref)
		if deleteErr == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}

		backoff := time.Duration(1<<attempt) * time.Second
		w.logger.Warn("DeleteCollection failed, retrying",
			"attempt", attempt+1,
			"backoff", backoff,
			"gvr", gvr.String(),
			"error", deleteErr)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}

	w.logger.Error("DeleteCollection failed after retries",
		"gvr", gvr.String(),
		"error", deleteErr)
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

func (w *Writer) resourceRef(gvr schema.GroupVersionResource, uid string) (domain.InventoryResourceRef, error) {
	name, err := ObjectResourceName(KubernetesObjectIdentity{
		TargetID: domain.TargetID(w.targetID),
		GVR:      gvr,
		UID:      uid,
	})
	if err != nil {
		return domain.InventoryResourceRef{}, err
	}
	return domain.InventoryResourceRef{
		ResourceType: ObjectResourceType,
		Name:         name,
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
