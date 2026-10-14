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

// indexerDelegate holds indexing-specific state for one managed cluster.
// It manages the informer-to-writer pipeline that watches Kubernetes
// resources and reports inventory through an [InventoryReporter].
type indexerDelegate struct {
	clusterResourceName domain.ResourceName
	dynClient           dynamic.Interface
	discClient          discovery.DiscoveryInterface
	reporter            InventoryReporter
	edgeSink            EdgeSink
	cfg                 IndexConfig
	logger              *slog.Logger
	done                chan struct{}
}

// newIndexerDelegate creates an indexerDelegate. A zero batchInterval
// in cfg defaults to 5 seconds. If edgeSink is nil, [NoopEdgeSink] is
// used. clusterResourceName is the managed cluster (clusters/{id}) used
// for object resource-name parents and edge-delta keys.
func newIndexerDelegate(
	clusterResourceName domain.ResourceName,
	dynClient dynamic.Interface,
	discClient discovery.DiscoveryInterface,
	reporter InventoryReporter,
	edgeSink EdgeSink,
	cfg IndexConfig,
	logger *slog.Logger,
) *indexerDelegate {
	if cfg.BatchInterval == 0 {
		cfg.BatchInterval = 5 * time.Second
	}
	if edgeSink == nil {
		edgeSink = NoopEdgeSink{}
	}
	return &indexerDelegate{
		clusterResourceName: clusterResourceName,
		dynClient:           dynClient,
		discClient:          discClient,
		reporter:            reporter,
		edgeSink:            edgeSink,
		cfg:                 cfg,
		logger:              logger,
		done:                make(chan struct{}),
	}
}

// start runs the informer manager and writer until ctx is cancelled.
// It discovers all supported GVRs, filters them through deny/allow lists,
// and uses RunContinuous to watch for CRD changes and re-reconcile.
//
// On shutdown, done closes only after ordinary and CRD informers have been
// awaited and the writer has finished its final flush under a shared
// shutdown deadline. That means no further inventory reporter / database
// calls are made for this target after done closes.
func (ic *indexerDelegate) start(ctx context.Context) {
	defer close(ic.done)

	schemaMap := ic.cfg.Schema.Entries

	var nsFilter *NamespaceFilter
	if ic.cfg.NamespaceFilter != nil {
		var err error
		nsFilter, err = NewNamespaceFilter(*ic.cfg.NamespaceFilter)
		if err != nil {
			ic.logger.Error("invalid namespace filter config; indexing will not start", "error", err)
			return
		}
	}

	w := NewWriter(ic.clusterResourceName, ic.reporter, ic.edgeSink, schemaMap, ic.cfg.BatchInterval, ic.logger)

	mgr := NewInformerManager(
		ic.dynClient,
		ic.discClient,
		w.EventCh(),
		w.ResyncCh(),
		w.RemoveCh(),
		nsFilter,
		ic.logger,
	)

	// Writer is not a child of ctx: ctx cancel must stop producers first,
	// then stop the writer under the shared shutdown budget.
	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w.Run(writerCtx)
	}()

	// RunContinuous blocks until ctx is cancelled, performing initial
	// reconciliation and re-reconciling when CRDs change.
	mgr.RunContinuous(ctx, ic.cfg.DenyList, ic.cfg.AllowList)

	// Context is done; clean up informers and writer under one budget.
	// StopAll does not emit RemoveGVR events, so shutdown does not treat
	// local cache eviction as a desired-set GVR removal.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultStopTimeout)
	defer shutdownCancel()

	if err := mgr.StopAll(shutdownCtx); err != nil {
		ic.logger.Warn("stop informers on shutdown incomplete", "error", err)
	}

	// Prefer Stop so the final flush inherits shutdownCtx's deadline.
	// Only cancel the run context if the shared budget expires first.
	w.Stop(shutdownCtx)
	select {
	case <-writerDone:
	case <-shutdownCtx.Done():
		writerCancel()
		<-writerDone
	}
}
