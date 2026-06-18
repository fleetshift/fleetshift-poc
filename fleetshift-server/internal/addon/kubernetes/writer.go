package kubernetes

import (
	"context"
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
	targetID      string
	writer        domain.InventoryWriter
	schema        map[schema.GroupVersionResource]SchemaEntry
	eventCh       chan ResourceEvent
	resyncCh      chan ResyncEvent
	batchInterval time.Duration
}

// NewWriter creates a Writer that batches events over batchInterval and
// writes them via the given InventoryWriter.
func NewWriter(
	targetID string,
	writer domain.InventoryWriter,
	schema map[schema.GroupVersionResource]SchemaEntry,
	batchInterval time.Duration,
) *Writer {
	return &Writer{
		targetID:      targetID,
		writer:        writer,
		schema:        schema,
		eventCh:       make(chan ResourceEvent, 256),
		resyncCh:      make(chan ResyncEvent, 16),
		batchInterval: batchInterval,
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

	// Pending batch state.
	pendingUpserts := make(map[string]*unstructured.Unstructured) // UID -> resource
	pendingUpsertGVR := make(map[string]schema.GroupVersionResource)
	pendingDeletes := make(map[string]struct{}) // UIDs deleted in this batch

	// Dedup: tracks UID -> last-sent resourceVersion.
	sentVersions := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			w.flush(ctx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions)
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
				pendingDeletes[uid] = struct{}{}
				// Remove any pending upsert for this UID — the delete wins.
				delete(pendingUpserts, uid)
				delete(pendingUpsertGVR, uid)
				// Clear sent version so a future add for this UID is not
				// deduped against the deleted resource.
				delete(sentVersions, uid)
			}

		case rs := <-w.resyncCh:
			w.sendResync(ctx, rs)

		case <-batchTicker.C:
			w.flush(ctx, pendingUpserts, pendingUpsertGVR, pendingDeletes, sentVersions)
			// Reset batch state.
			pendingUpserts = make(map[string]*unstructured.Unstructured)
			pendingUpsertGVR = make(map[string]schema.GroupVersionResource)
			pendingDeletes = make(map[string]struct{})
		}
	}
}

// flush sends the accumulated batch as a single ApplyDelta call. It applies
// dedup by skipping upserts whose resourceVersion has not changed.
func (w *Writer) flush(
	ctx context.Context,
	upserts map[string]*unstructured.Unstructured,
	upsertGVR map[string]schema.GroupVersionResource,
	deletes map[string]struct{},
	sentVersions map[string]string,
) {
	if len(upserts) == 0 && len(deletes) == 0 {
		return
	}

	var items []domain.InventoryItem

	for uid, r := range upserts {
		rv := r.GetResourceVersion()
		// Dedup: skip if we already sent this exact version.
		if lastRV, ok := sentVersions[uid]; ok && lastRV == rv {
			continue
		}

		gvr := upsertGVR[uid]
		entry := w.schema[gvr]
		item, _ := ExtractObservedResource(r, entry, w.targetID)
		items = append(items, item)
		sentVersions[uid] = rv
	}

	var deletedIDs []domain.InventoryItemID
	for uid := range deletes {
		deletedIDs = append(deletedIDs, domain.InventoryItemID(w.targetID+"/"+uid))
	}

	if len(items) == 0 && len(deletedIDs) == 0 {
		return
	}

	if w.writer == nil {
		return
	}
	_ = w.writer.ApplyDelta(ctx, domain.TargetID(w.targetID), items, deletedIDs, nil, nil)
}

// sendResync sends a Resync call for the given GVR.
func (w *Writer) sendResync(ctx context.Context, rs ResyncEvent) {
	entry := w.schema[rs.GVR] // zero-value SchemaEntry if not in schema — base extraction only

	var items []domain.InventoryItem
	for _, r := range rs.Resources {
		item, _ := ExtractObservedResource(r, entry, w.targetID)
		items = append(items, item)
	}

	// Derive the Kind for the inventory type. Prefer the schema entry's Kind,
	// but fall back to the first resource's Kind for resources without a schema entry.
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
	_ = w.writer.Resync(ctx, domain.TargetID(w.targetID), invType, items, nil)
}
