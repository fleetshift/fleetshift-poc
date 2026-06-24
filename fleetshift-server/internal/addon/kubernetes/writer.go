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

// EventOp represents the type of informer event.
type EventOp int

const (
	EventAdd EventOp = iota
	EventUpdate
	EventDelete
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

// ResourceEvent is a single informer event for a Kubernetes resource.
type ResourceEvent struct {
	Op       EventOp
	Resource *unstructured.Unstructured
	GVR      schema.GroupVersionResource
}

// ResyncEvent carries the full resource set for a GVR after an informer
// completes its initial LIST.
type ResyncEvent struct {
	GVR       schema.GroupVersionResource
	Resources []*unstructured.Unstructured
}

// Writer batches informer events and writes them as domain inventory
// items via an InventoryWriter.
type Writer struct {
	targetID          string
	writer            domain.InventoryWriter
	schema            map[schema.GroupVersionResource]SchemaEntry
	eventCh           chan ResourceEvent
	resyncCh          chan ResyncEvent
	batchInterval     time.Duration
	heartbeatInterval time.Duration
	currentNodes      map[string]inventoryNode
	edgeFuncs         map[string]func(NodeStore) []Edge
	previousEdges     map[edgeKey]Edge
	logger            *slog.Logger
}

// NewWriter creates a Writer that batches events over batchInterval and
// writes them via the given InventoryWriter. A zero heartbeatInterval
// defaults to 60 seconds.
func NewWriter(
	targetID string,
	writer domain.InventoryWriter,
	schema map[schema.GroupVersionResource]SchemaEntry,
	batchInterval time.Duration,
	logger *slog.Logger,
) *Writer {
	heartbeatInterval := 60 * time.Second
	return &Writer{
		targetID:          targetID,
		writer:            writer,
		schema:            schema,
		eventCh:           make(chan ResourceEvent, 256),
		resyncCh:          make(chan ResyncEvent, 16),
		batchInterval:     batchInterval,
		heartbeatInterval: heartbeatInterval,
		currentNodes:      make(map[string]inventoryNode),
		edgeFuncs:         make(map[string]func(NodeStore) []Edge),
		previousEdges:     make(map[edgeKey]Edge),
		logger:            logger,
	}
}

// EventCh returns the channel callers use to submit resource events.
func (w *Writer) EventCh() chan<- ResourceEvent { return w.eventCh }

// ResyncCh returns the channel callers use to submit resync events.
func (w *Writer) ResyncCh() chan<- ResyncEvent { return w.resyncCh }

// Run starts the event loop. It blocks until ctx is cancelled, flushing any
// remaining batch before returning.
func (w *Writer) Run(ctx context.Context) {
	batchTicker := time.NewTicker(w.batchInterval)
	defer batchTicker.Stop()

	heartbeatTicker := time.NewTicker(w.heartbeatInterval)
	defer heartbeatTicker.Stop()

	lastActivity := time.Now()

	// Pending batch state.
	pendingUpserts := make(map[string]*unstructured.Unstructured) // UID -> resource
	pendingUpsertGVR := make(map[string]schema.GroupVersionResource)
	pendingDeletes := make(map[string]struct{}) // UIDs deleted in this batch

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
			lastActivity = time.Now()
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
				pendingDeletes[uid] = struct{}{}
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
			lastActivity = time.Now()
			w.sendResync(ctx, rs)

		case <-batchTicker.C:
			if len(pendingUpserts) > 0 || len(pendingDeletes) > 0 {
				lastActivity = time.Now()
			}
			if err := w.flush(ctx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions); err == nil {
				pendingUpserts = make(map[string]*unstructured.Unstructured)
				pendingUpsertGVR = make(map[string]schema.GroupVersionResource)
				pendingDeletes = make(map[string]struct{})
			}

		case <-heartbeatTicker.C:
			if time.Since(lastActivity) >= w.heartbeatInterval {
				w.applyWithRetry(ctx, nil, nil, nil, nil)
				lastActivity = time.Now()
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
	deletes map[string]struct{},
	sentVersions map[string]string,
) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	var items []domain.InventoryItem
	newSentVersions := make(map[string]string)

	for uid, r := range upserts {
		rv := r.GetResourceVersion()
		// Dedup: skip if we already sent this exact version.
		if lastRV, ok := sentVersions[uid]; ok && lastRV == rv {
			continue
		}

		gvr := upsertGVR[uid]
		entry := w.schema[gvr]
		item, node := ExtractObservedResource(r, entry, w.targetID)
		node.GVR = gvr
		items = append(items, item)
		newSentVersions[uid] = rv

		// Track inventory node for edge computation.
		w.currentNodes[uid] = node

		if entry.BuildEdges != nil {
			w.edgeFuncs[uid] = entry.BuildEdges(r, uid)
		}
	}

	var deletedIDs []domain.InventoryItemID
	for uid := range deletes {
		deletedIDs = append(deletedIDs, domain.InventoryItemID(w.targetID+"/"+uid))
	}

	if len(items) == 0 && len(deletedIDs) == 0 {
		return nil
	}

	// Compute edges for all current nodes.
	ns := buildNodeStore(w.currentNodes)
	newEdges := make(map[edgeKey]Edge)

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

	// Diff edges to produce adds and deletes.
	var edgeAdds, edgeDels []domain.InventoryEdge

	for key, edge := range newEdges {
		if _, ok := w.previousEdges[key]; !ok {
			edgeAdds = append(edgeAdds, domain.InventoryEdge{
				EdgeType:   string(edge.EdgeType),
				SourceUID:  edge.SourceUID,
				DestUID:    edge.DestUID,
				SourceKind: edge.SourceKind,
				DestKind:   edge.DestKind,
			})
		}
	}

	for key, edge := range w.previousEdges {
		if _, ok := newEdges[key]; !ok {
			edgeDels = append(edgeDels, domain.InventoryEdge{
				EdgeType:   string(edge.EdgeType),
				SourceUID:  edge.SourceUID,
				DestUID:    edge.DestUID,
				SourceKind: edge.SourceKind,
				DestKind:   edge.DestKind,
			})
		}
	}

	if w.writer == nil {
		return nil
	}

	if err := w.applyWithRetry(ctx, items, deletedIDs, edgeAdds, edgeDels); err != nil {
		return err
	}

	maps.Copy(sentVersions, newSentVersions)
	w.previousEdges = newEdges
	return nil
}

// applyWithRetry applies a delta with exponential backoff retry.
// It retries up to 3 times with 1s, 2s, 4s backoff (capped at 30s).
// Returns the error if all retries fail.
func (w *Writer) applyWithRetry(
	ctx context.Context,
	items []domain.InventoryItem,
	deletedIDs []domain.InventoryItemID,
	edgeAdds, edgeDels []domain.InventoryEdge,
) error {
	if w.writer == nil {
		return nil
	}

	var err error
	for attempt := range 3 {
		err = w.writer.ApplyDelta(ctx, domain.TargetID(w.targetID), items, deletedIDs, edgeAdds, edgeDels)
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

// sendResync sends a Resync call for the given GVR.
//
// Resync handles items only — edges are nil, which tells the
// InventoryWriter to leave existing edges untouched.  Edge computation
// is deferred to the flush path, which runs ALL edge closures against
// the full w.currentNodes on every tick.  This avoids a class of race
// where a GVR resync runs before cross-GVR dependencies are in
// w.currentNodes (e.g. Pod resync before ReplicaSets are known),
// which would delete correct edges and fail to re-create them.
func (w *Writer) sendResync(ctx context.Context, rs ResyncEvent) {
	entry := w.schema[rs.GVR]

	resyncUIDs := make(map[string]struct{})
	var items []domain.InventoryItem

	for _, r := range rs.Resources {
		item, node := ExtractObservedResource(r, entry, w.targetID)
		node.GVR = rs.GVR
		items = append(items, item)
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

	kind := entry.Kind
	if kind == "" && len(rs.Resources) > 0 {
		kind = rs.Resources[0].GetKind()
	}

	var invType domain.InventoryType
	if rs.GVR.Group != "" {
		invType = domain.InventoryType(rs.GVR.Group + "/" + rs.GVR.Version + "/" + kind)
	} else {
		invType = domain.InventoryType(rs.GVR.Version + "/" + kind)
	}

	if w.writer == nil {
		return
	}
	w.resyncWithRetry(ctx, invType, items)
}

func (w *Writer) resyncWithRetry(ctx context.Context, invType domain.InventoryType, items []domain.InventoryItem) {
	var err error
	for attempt := range 3 {
		err = w.writer.Resync(ctx, domain.TargetID(w.targetID), invType, items)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}

		backoff := time.Duration(1<<attempt) * time.Second
		w.logger.Warn("Resync failed, retrying",
			"attempt", attempt+1,
			"backoff", backoff,
			"error", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}

	w.logger.Error("Resync failed after retries", "error", err)
}
