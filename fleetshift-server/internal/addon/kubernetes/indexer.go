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
	Schema          IndexSchema
	DenyList        []Resource
	AllowList       []Resource
	NamespaceFilter *NamespaceFilterConfig
	BatchInterval   time.Duration
}

// indexerDelegate holds indexing-specific state for an Agent.
// It manages the informer-to-writer pipeline that watches Kubernetes
// resources and writes inventory items via an InventoryWriter.
type indexerDelegate struct {
	targetID   string
	dynClient  dynamic.Interface
	discClient discovery.DiscoveryInterface
	writer     domain.InventoryWriter
	cfg        IndexConfig
	logger     *slog.Logger
	done       chan struct{}
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
	if cfg.BatchInterval == 0 {
		cfg.BatchInterval = 5 * time.Second
	}
	return &indexerDelegate{
		targetID:   targetID,
		dynClient:  dynClient,
		discClient: discClient,
		writer:     writer,
		cfg:        cfg,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// start runs the informer manager and writer until ctx is cancelled.
// It discovers all supported GVRs, filters them through deny/allow lists,
// and uses RunContinuous to watch for CRD changes and re-reconcile.
func (ic *indexerDelegate) start(ctx context.Context) {
	defer close(ic.done)

	schemaMap := ic.cfg.Schema.Entries

	var nsFilter *NamespaceFilter
	if ic.cfg.NamespaceFilter != nil {
		nsFilter = NewNamespaceFilter(*ic.cfg.NamespaceFilter)
	}

	w := NewWriter(ic.targetID, ic.writer, schemaMap, ic.cfg.BatchInterval, ic.logger)

	mgr := NewInformerManager(
		ic.dynClient,
		ic.discClient,
		w.EventCh(),
		w.ResyncCh(),
		nsFilter,
		ic.logger,
	)

	writerCtx, writerCancel := context.WithCancel(ctx)
	defer writerCancel()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w.Run(writerCtx)
	}()

	// RunContinuous blocks until ctx is cancelled, performing initial
	// reconciliation and re-reconciling when CRDs change.
	mgr.RunContinuous(ctx, ic.cfg.DenyList, ic.cfg.AllowList)

	// Context is done; clean up informers and writer.
	mgr.StopAll()
	writerCancel()
	<-writerDone
}
