package kubernetes

import (
	"context"
	"log/slog"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// IndexConfig holds configuration for the indexer delegate.
type IndexConfig struct {
	Schema        IndexSchema
	DenyList      []Resource
	AllowList     []Resource
	BatchInterval time.Duration
}

// indexerDelegate holds indexing-specific state for an Agent.
// It manages the informer-to-writer pipeline that watches Kubernetes
// resources and writes inventory items via an InventoryWriter.
type indexerDelegate struct {
	targetID      string
	dynClient     dynamic.Interface
	discClient    discovery.DiscoveryInterface
	writer        domain.InventoryWriter
	schema        IndexSchema
	batchInterval time.Duration
	logger        *slog.Logger
	done          chan struct{}
}

// newIndexerDelegate creates an indexerDelegate. A zero batchInterval
// in cfg defaults to 5 seconds.
func newIndexerDelegate(
	targetID string,
	dynClient dynamic.Interface,
	discClient discovery.DiscoveryInterface,
	writer domain.InventoryWriter,
	cfg IndexConfig,
	logger *slog.Logger,
) *indexerDelegate {
	batchInterval := cfg.BatchInterval
	if batchInterval == 0 {
		batchInterval = 5 * time.Second
	}
	return &indexerDelegate{
		targetID:      targetID,
		dynClient:     dynClient,
		discClient:    discClient,
		writer:        writer,
		schema:        cfg.Schema,
		batchInterval: batchInterval,
		logger:        logger,
		done:          make(chan struct{}),
	}
}

// start runs the informer manager and writer until ctx is cancelled.
func (ic *indexerDelegate) start(ctx context.Context) {
	defer close(ic.done)

	schemaMap := ic.schema.Entries
	desiredGVRs := ic.schema.GVRs()

	w := NewWriter(ic.targetID, ic.writer, schemaMap, ic.batchInterval)

	mgr := NewInformerManager(
		ic.dynClient,
		ic.discClient,
		w.EventCh(),
		w.ResyncCh(),
		ic.logger,
	)

	writerCtx, writerCancel := context.WithCancel(ctx)
	defer writerCancel()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w.Run(writerCtx)
	}()

	mgr.Reconcile(ctx, desiredGVRs)
	defer mgr.StopAll()

	// Block until context is cancelled.
	<-ctx.Done()
	writerCancel()
	<-writerDone
}
