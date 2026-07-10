package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// KubernetesInProcessIndexHost hosts in-process per-target Kubernetes indexers.
// It implements [InProcessIndexRuntime] for the in-process controller.
type KubernetesInProcessIndexHost struct {
	ctx      context.Context
	vault    domain.Vault
	reporter InventoryReporter
	logger   *slog.Logger

	newDynamic   func(*rest.Config) (dynamic.Interface, error)
	newDiscovery func(*rest.Config) (discovery.DiscoveryInterface, error)
	buildConfig  func(context.Context, domain.TargetInfo) (*rest.Config, error)
	indexConfig  func(domain.TargetInfo) IndexConfig

	mu      sync.Mutex
	running map[domain.TargetID]*inProcessKubernetesIndexer
}

type inProcessKubernetesIndexer struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

// KubernetesInProcessIndexHostOption configures a [KubernetesInProcessIndexHost].
type KubernetesInProcessIndexHostOption func(*KubernetesInProcessIndexHost)

// WithInProcessIndexHostDynamicClientFactory overrides dynamic client construction.
// Intended for tests.
func WithInProcessIndexHostDynamicClientFactory(fn func(*rest.Config) (dynamic.Interface, error)) KubernetesInProcessIndexHostOption {
	return func(h *KubernetesInProcessIndexHost) {
		if fn != nil {
			h.newDynamic = fn
		}
	}
}

// WithInProcessIndexHostDiscoveryClientFactory overrides discovery client
// construction. Intended for tests.
func WithInProcessIndexHostDiscoveryClientFactory(fn func(*rest.Config) (discovery.DiscoveryInterface, error)) KubernetesInProcessIndexHostOption {
	return func(h *KubernetesInProcessIndexHost) {
		if fn != nil {
			h.newDiscovery = fn
		}
	}
}

// WithInProcessIndexHostRESTConfigFactory overrides REST config construction.
// Intended for tests.
func WithInProcessIndexHostRESTConfigFactory(fn func(context.Context, domain.TargetInfo) (*rest.Config, error)) KubernetesInProcessIndexHostOption {
	return func(h *KubernetesInProcessIndexHost) {
		if fn != nil {
			h.buildConfig = fn
		}
	}
}

// WithInProcessIndexHostIndexConfig overrides effective IndexConfig construction.
// Intended for tests.
func WithInProcessIndexHostIndexConfig(fn func(domain.TargetInfo) IndexConfig) KubernetesInProcessIndexHostOption {
	return func(h *KubernetesInProcessIndexHost) {
		if fn != nil {
			h.indexConfig = fn
		}
	}
}

// NewKubernetesInProcessIndexHost creates a host. ctx must outlive individual
// request or activity contexts; it governs every in-process indexer the
// host starts. logger may be nil.
func NewKubernetesInProcessIndexHost(
	ctx context.Context,
	vault domain.Vault,
	reporter InventoryReporter,
	logger *slog.Logger,
	opts ...KubernetesInProcessIndexHostOption,
) *KubernetesInProcessIndexHost {
	if logger == nil {
		logger = slog.Default()
	}
	h := &KubernetesInProcessIndexHost{
		ctx:      ctx,
		vault:    vault,
		reporter: reporter,
		logger:   logger.With("component", "kubernetes-index-host"),
		newDynamic: func(cfg *rest.Config) (dynamic.Interface, error) {
			return dynamic.NewForConfig(cfg)
		},
		newDiscovery: func(cfg *rest.Config) (discovery.DiscoveryInterface, error) {
			return discovery.NewDiscoveryClientForConfig(cfg)
		},
		indexConfig: func(domain.TargetInfo) IndexConfig {
			return IndexConfig{Schema: DefaultKubernetesSchema()}
		},
		running: make(map[domain.TargetID]*inProcessKubernetesIndexer),
	}
	h.buildConfig = func(ctx context.Context, target domain.TargetInfo) (*rest.Config, error) {
		return BuildTargetRESTConfig(ctx, h.vault, target)
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// StartIndexer builds Kubernetes clients from target properties and
// vault, constructs the per-target indexer, and starts it under the
// host's long-lived context. It is idempotent by target ID and does
// not delete inventory.
func (h *KubernetesInProcessIndexHost) StartIndexer(ctx context.Context, target domain.TargetInfo) error {
	if target.Type() != TargetType {
		return fmt.Errorf("%w: target %q has type %q, want %q",
			domain.ErrInvalidArgument, target.ID(), target.Type(), TargetType)
	}
	id := target.ID()

	h.mu.Lock()
	if _, ok := h.running[id]; ok {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	cfg, err := h.buildConfig(ctx, target)
	if err != nil {
		return fmt.Errorf("build rest config for %s: %w", id, err)
	}
	dynClient, err := h.newDynamic(cfg)
	if err != nil {
		return fmt.Errorf("create dynamic client for %s: %w", id, err)
	}
	discClient, err := h.newDiscovery(cfg)
	if err != nil {
		return fmt.Errorf("create discovery client for %s: %w", id, err)
	}

	logger := h.logger.With("target", string(id))
	ic := newIndexerDelegate(
		string(id),
		dynClient,
		discClient,
		h.reporter,
		NoopEdgeSink{},
		h.indexConfig(target),
		logger,
	)

	idxCtx, cancel := context.WithCancel(h.ctx)
	indexer := &inProcessKubernetesIndexer{cancel: cancel, done: ic.done}

	h.mu.Lock()
	if _, ok := h.running[id]; ok {
		h.mu.Unlock()
		cancel()
		return nil
	}
	h.running[id] = indexer
	h.mu.Unlock()

	go func() {
		ic.start(idxCtx)
		h.mu.Lock()
		if cur, ok := h.running[id]; ok && cur == indexer {
			delete(h.running, id)
		}
		h.mu.Unlock()
		if err := idxCtx.Err(); err != nil && err != context.Canceled {
			logger.Warn("in-process indexer exited unexpectedly", "error", err)
		} else if idxCtx.Err() == nil {
			logger.Warn("in-process indexer exited unexpectedly")
		}
	}()
	return nil
}

// StopIndexer cancels the in-process indexer for target, waits for shutdown
// bounded by ctx, and removes it from the running map. It does not
// delete inventory. Stop is idempotent.
func (h *KubernetesInProcessIndexHost) StopIndexer(ctx context.Context, target domain.TargetInfo) error {
	id := target.ID()

	h.mu.Lock()
	indexer, ok := h.running[id]
	if !ok {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	indexer.cancel()
	select {
	case <-indexer.done:
		h.mu.Lock()
		if cur, still := h.running[id]; still && cur == indexer {
			delete(h.running, id)
		}
		h.mu.Unlock()
		return nil
	case <-ctx.Done():
		// Leave the entry in the map until the indexer finishes so a
		// concurrent StartIndexer cannot start a duplicate. A background
		// waiter removes it once shutdown completes.
		go func() {
			<-indexer.done
			h.mu.Lock()
			if cur, still := h.running[id]; still && cur == indexer {
				delete(h.running, id)
			}
			h.mu.Unlock()
		}()
		return fmt.Errorf("stop in-process indexer for %s: %w", id, ctx.Err())
	}
}

// StopAllIndexers stops every running in-process indexer. It is safe during server
// shutdown and does not perform inventory cleanup.
func (h *KubernetesInProcessIndexHost) StopAllIndexers(ctx context.Context) error {
	h.mu.Lock()
	indexers := make(map[domain.TargetID]*inProcessKubernetesIndexer, len(h.running))
	maps.Copy(indexers, h.running)
	h.mu.Unlock()

	var firstErr error
	for id, indexer := range indexers {
		indexer.cancel()
		select {
		case <-indexer.done:
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = fmt.Errorf("stop all: waiting for %s: %w", id, ctx.Err())
			}
			go func(id domain.TargetID, indexer *inProcessKubernetesIndexer) {
				<-indexer.done
				h.mu.Lock()
				if cur, still := h.running[id]; still && cur == indexer {
					delete(h.running, id)
				}
				h.mu.Unlock()
			}(id, indexer)
			continue
		}
		h.mu.Lock()
		if cur, still := h.running[id]; still && cur == indexer {
			delete(h.running, id)
		}
		h.mu.Unlock()
	}
	return firstErr
}

// Running reports whether an in-process indexer is tracked for id. Intended
// for tests.
func (h *KubernetesInProcessIndexHost) Running(id domain.TargetID) bool {
	return h.HasIndexer(id)
}

// HasIndexer implements [InProcessIndexRuntime].
func (h *KubernetesInProcessIndexHost) HasIndexer(id domain.TargetID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.running[id]
	return ok
}
