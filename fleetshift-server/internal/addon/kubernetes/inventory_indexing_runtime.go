package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// defaultIndexConfigDigest is included in the runtime fingerprint so
// changing DefaultIndexConfig construction invalidates running indexers.
const defaultIndexConfigDigest = "default-kubernetes-schema"

// defaultStopTimeout bounds indexer shutdown waits used by the indexing
// runtime and per-target indexer final flush.
const defaultStopTimeout = 5 * time.Second

// IndexingRuntime ensures indexing is active for Kubernetes targets
// (EnsureIndexer / StopIndexer / StopAll) without deleting inventory.
// [KubernetesInProcessIndexHost] is the in-process implementation.
type IndexingRuntime interface {
	// EnsureIndexer starts or replaces an indexer and returns after the start
	// attempt finishes: already-ready success, joined in-flight result,
	// discovery readiness success, or failure. Caller ctx bounds only the
	// start attempt (including waiting on an in-flight start), not the
	// long-lived indexer once ready.
	EnsureIndexer(ctx context.Context, input IndexRuntimeInput) error
	// StopIndexer stops one target's indexer. It is idempotent and does not
	// delete inventory.
	StopIndexer(ctx context.Context, targetID domain.TargetID) error
	// StopAll cancels every hosted indexer (including in-flight starts) and
	// waits for exit, bounded by ctx. Unexpected-exit restart is suppressed
	// for the duration of the call; EnsureIndexer is allowed again afterward.
	StopAll(ctx context.Context) error
}

// IndexRuntimeInput carries connection and index configuration for one
// EnsureIndexer call. Construct only via [NewIndexRuntimeInput]; zero values
// and struct literals are not a supported API. Credential must not be logged
// or persisted by callers.
type IndexRuntimeInput struct {
	// TargetID identifies the Kubernetes target being indexed.
	TargetID domain.TargetID
	// ClusterResourceName is the managed cluster resource (e.g.
	// "clusters/c1"). Its ID is the object resource-name parent segment so
	// inventory nests under the cluster identity.
	ClusterResourceName domain.ResourceName
	// APIServer is the Kubernetes API server URL.
	APIServer string
	// CACert is the optional PEM-encoded cluster CA certificate.
	CACert string
	// Credential is the bearer token used to construct Kubernetes clients.
	Credential []byte
	// SecretRef is an optional vault reference used to reload the indexing
	// credential on local restart after unexpected exit.
	SecretRef domain.SecretRef
	// Generation is the producer revision used for fencing.
	Generation domain.Generation
	// IndexConfig is the effective watch/filter configuration for this start.
	IndexConfig IndexConfig
}

// NewIndexRuntimeInput constructs a valid [IndexRuntimeInput]. Permanently
// unusable combinations return [domain.ErrInvalidArgument]. Prefer this over
// building a struct literal and checking fields later.
// caCert and secretRef may be empty (no custom CA; no unexpected-exit restart).
func NewIndexRuntimeInput(
	targetID domain.TargetID,
	clusterResourceName domain.ResourceName,
	apiServer string,
	caCert string,
	credential []byte,
	secretRef domain.SecretRef,
	generation domain.Generation,
	indexConfig IndexConfig,
) (IndexRuntimeInput, error) {
	if targetID == "" {
		return IndexRuntimeInput{}, fmt.Errorf("%w: target id is required", domain.ErrInvalidArgument)
	}
	if err := requireClusterResourceName(clusterResourceName); err != nil {
		return IndexRuntimeInput{}, err
	}
	if apiServer == "" {
		return IndexRuntimeInput{}, fmt.Errorf("%w: api server is required", domain.ErrInvalidArgument)
	}
	if len(credential) == 0 {
		return IndexRuntimeInput{}, fmt.Errorf("%w: indexing credential is required", domain.ErrInvalidArgument)
	}
	return IndexRuntimeInput{
		TargetID:            targetID,
		ClusterResourceName: clusterResourceName,
		APIServer:           apiServer,
		CACert:              caCert,
		Credential:          credential,
		SecretRef:           secretRef,
		Generation:          generation,
		IndexConfig:         indexConfig,
	}, nil
}

// ErrStaleIndexerGeneration is returned when EnsureIndexer receives a
// producer generation lower than the one already accepted for the target.
var ErrStaleIndexerGeneration = errors.New("stale indexer generation")

// ErrIndexerAllowListEmpty is a permanent validation error: an explicit
// non-empty allow-list filtered to zero watchable GVRs.
var ErrIndexerAllowListEmpty = errors.New("index allow-list matched no watchable resources")

// maxUnexpectedRestartAttempts bounds local restarts after an indexer exits
// without an intentional stop.
const maxUnexpectedRestartAttempts = 3

// unexpectedRestartBackoff is the per-attempt delay before a local restart.
var unexpectedRestartBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}

// KubernetesInProcessIndexHost hosts in-process per-target Kubernetes indexers.
// It implements [IndexingRuntime].
type KubernetesInProcessIndexHost struct {
	ctx      context.Context
	vault    domain.Vault
	reporter InventoryReporter
	clients  IndexerClients
	logger   *slog.Logger

	mu           sync.Mutex
	entries      map[domain.TargetID]*managedIndexer
	targetOps    map[domain.TargetID]*sync.Mutex
	shuttingDown bool
}

// managedIndexer is the in-memory registry entry for one target's indexer
// lifecycle (start, ready, stop, and optional local restart).
type managedIndexer struct {
	cancel context.CancelFunc
	done   <-chan struct{}

	generation  domain.Generation
	fingerprint string

	apiServer           string
	caCert              string
	secretRef           domain.SecretRef
	clusterResourceName domain.ResourceName
	indexConfig         IndexConfig

	ready           bool
	starting        bool
	readyWait       chan struct{} // closed when starting attempt finishes
	startErr        error
	readinessCancel context.CancelFunc

	restartEligible bool
	intentionalStop bool
	restartAttempts int
}

// NewKubernetesInProcessIndexHost creates a host that implements
// [IndexingRuntime]. ctx must outlive individual request contexts; it
// governs every indexer the host starts. clients is required (use
// [DefaultIndexerClients] in production). logger may be nil.
func NewKubernetesInProcessIndexHost(
	ctx context.Context,
	vault domain.Vault,
	reporter InventoryReporter,
	clients IndexerClients,
	logger *slog.Logger,
) *KubernetesInProcessIndexHost {
	if logger == nil {
		logger = slog.Default()
	}
	if clients == nil {
		clients = DefaultIndexerClients{}
	}
	return &KubernetesInProcessIndexHost{
		ctx:       ctx,
		vault:     vault,
		reporter:  reporter,
		clients:   clients,
		logger:    logger.With("component", "kubernetes-indexing-runtime"),
		entries:   make(map[domain.TargetID]*managedIndexer),
		targetOps: make(map[domain.TargetID]*sync.Mutex),
	}
}

// Compile-time check.
var _ IndexingRuntime = (*KubernetesInProcessIndexHost)(nil)

// EnsureIndexer starts or replaces an indexer for input.TargetID.
//
// If a matching ready process already exists, it returns nil. If a matching
// start is in flight, it waits for that attempt. Otherwise it runs discovery
// readiness and, on success, starts the indexer under the host's long-lived
// context. Caller ctx bounds only this start attempt (not the ready indexer).
// Discovery RPCs are not forcibly aborted by ctx; cancellation is observed
// between steps and by concurrent StopIndexer.
func (h *KubernetesInProcessIndexHost) EnsureIndexer(ctx context.Context, input IndexRuntimeInput) error {
	fingerprint := indexRuntimeFingerprint(input)
	id := input.TargetID

	for {
		op := h.lockTarget(id)

		h.mu.Lock()
		if h.shuttingDown {
			h.mu.Unlock()
			op.Unlock()
			return fmt.Errorf("indexing runtime is shutting down")
		}
		entry := h.entries[id]

		if entry != nil {
			if input.Generation < entry.generation {
				h.mu.Unlock()
				op.Unlock()
				return fmt.Errorf("%w: got %d, have %d", ErrStaleIndexerGeneration, input.Generation, entry.generation)
			}
			if input.Generation == entry.generation && entry.fingerprint == fingerprint {
				if entry.ready {
					h.mu.Unlock()
					op.Unlock()
					return nil
				}
				if entry.starting {
					wait := entry.readyWait
					h.mu.Unlock()
					op.Unlock()
					select {
					case <-wait:
						return entry.startErr
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			// Changed fingerprint, higher generation, or a non-joinable leftover.
			h.mu.Unlock()
			err := h.stopLocked(ctx, id, true)
			op.Unlock()
			if err != nil {
				return err
			}
			continue
		}

		readinessCtx, readinessCancel := context.WithCancel(ctx)
		entry = &managedIndexer{
			generation:          input.Generation,
			fingerprint:         fingerprint,
			apiServer:           input.APIServer,
			caCert:              input.CACert,
			secretRef:           input.SecretRef,
			clusterResourceName: input.ClusterResourceName,
			indexConfig:         input.IndexConfig,
			starting:            true,
			readyWait:           make(chan struct{}),
			readinessCancel:     readinessCancel,
		}
		h.entries[id] = entry
		h.mu.Unlock()
		// Release the per-target lock during readiness so StopIndexer and
		// concurrent EnsureIndexer callers can join or cancel the attempt.
		op.Unlock()

		err := h.startReady(readinessCtx, id, entry, input)
		readinessCancel()

		// Finalize without the per-target op lock so a concurrent StopIndexer
		// waiting on readyWait cannot deadlock against this goroutine.
		h.mu.Lock()
		entry.startErr = err
		entry.starting = false
		entry.readinessCancel = nil
		if err != nil {
			if cur := h.entries[id]; cur == entry {
				delete(h.entries, id)
			}
		}
		close(entry.readyWait)
		h.mu.Unlock()
		return err
	}
}

// startReady performs discovery readiness, then launches the long-lived
// indexer under the host context. The per-target op lock must not be held.
func (h *KubernetesInProcessIndexHost) startReady(
	ctx context.Context,
	id domain.TargetID,
	entry *managedIndexer,
	input IndexRuntimeInput,
) error {
	h.mu.Lock()
	if h.shuttingDown || entry.intentionalStop {
		h.mu.Unlock()
		return fmt.Errorf("indexer start cancelled for %s", id)
	}
	h.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	cfg := restConfigFromIndexInput(input)
	dynClient, err := h.clients.Dynamic(cfg)
	if err != nil {
		return fmt.Errorf("create dynamic client for %s: %w", id, err)
	}
	discClient, err := h.clients.Discovery(cfg)
	if err != nil {
		return fmt.Errorf("create discovery client for %s: %w", id, err)
	}

	logger := h.logger.With("target", string(id))
	if err := checkDiscoveryReadiness(discClient, input.IndexConfig, logger); err != nil {
		return err
	}

	h.mu.Lock()
	if h.shuttingDown || entry.intentionalStop || h.entries[id] != entry {
		h.mu.Unlock()
		return fmt.Errorf("indexer start cancelled for %s", id)
	}
	h.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	ic := newIndexerDelegate(
		input.ClusterResourceName,
		dynClient,
		discClient,
		h.reporter,
		NoopEdgeSink{},
		input.IndexConfig,
		logger,
	)

	idxCtx, cancel := context.WithCancel(h.ctx)

	h.mu.Lock()
	if h.shuttingDown || entry.intentionalStop || h.entries[id] != entry {
		h.mu.Unlock()
		cancel()
		return fmt.Errorf("indexer start cancelled for %s", id)
	}
	// Publish cancel/done only when the indexer goroutine will launch, so
	// StopAll/stopLocked never wait on a done channel that will never close.
	entry.cancel = cancel
	entry.done = ic.done
	entry.ready = true
	entry.restartEligible = input.SecretRef != ""
	entry.restartAttempts = 0
	h.mu.Unlock()

	go func() {
		ic.start(idxCtx)
		h.onIndexerExit(id, entry, logger)
	}()
	return nil
}

// onIndexerExit runs after an indexer goroutine returns. Intentional stops and
// StopAll clear the entry. Otherwise, if a SecretRef is present, a bounded
// local restart may be scheduled.
func (h *KubernetesInProcessIndexHost) onIndexerExit(id domain.TargetID, entry *managedIndexer, logger *slog.Logger) {
	op := h.lockTarget(id)

	h.mu.Lock()
	cur, ok := h.entries[id]
	if !ok || cur != entry {
		h.mu.Unlock()
		op.Unlock()
		return
	}

	intentional := entry.intentionalStop || h.shuttingDown
	restartEligible := entry.restartEligible && entry.secretRef != "" && !intentional
	nextAttempt := entry.restartAttempts + 1
	secretRef := entry.secretRef
	apiServer := entry.apiServer
	caCert := entry.caCert
	clusterResourceName := entry.clusterResourceName
	indexConfig := entry.indexConfig
	generation := entry.generation

	if intentional || !restartEligible || nextAttempt > maxUnexpectedRestartAttempts {
		delete(h.entries, id)
		h.mu.Unlock()
		op.Unlock()
		if !intentional {
			logger.Warn("in-process indexer exited unexpectedly; not restarting",
				"restart_eligible", restartEligible,
				"attempts", entry.restartAttempts,
			)
		}
		return
	}

	entry.ready = false
	entry.restartAttempts = nextAttempt
	backoffIdx := nextAttempt - 1
	if backoffIdx >= len(unexpectedRestartBackoff) {
		backoffIdx = len(unexpectedRestartBackoff) - 1
	}
	backoff := unexpectedRestartBackoff[backoffIdx]
	// Drop the dead runner before releasing the per-target lock so EnsureIndexer
	// can recreate the entry.
	delete(h.entries, id)
	h.mu.Unlock()
	op.Unlock()

	logger.Warn("in-process indexer exited unexpectedly; scheduling restart",
		"attempt", nextAttempt,
		"backoff", backoff,
	)

	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-h.ctx.Done():
		return
	}

	h.mu.Lock()
	shuttingDown := h.shuttingDown
	h.mu.Unlock()
	if shuttingDown {
		return
	}

	token, err := h.resolveSecret(h.ctx, secretRef)
	if err != nil {
		logger.Warn("indexer restart skipped; credential unavailable",
			"error", err,
		)
		return
	}

	input, err := NewIndexRuntimeInput(
		id,
		clusterResourceName,
		apiServer,
		caCert,
		token,
		secretRef,
		generation,
		indexConfig,
	)
	if err != nil {
		logger.Warn("indexer restart skipped; invalid restart input", "error", err)
		return
	}
	if err := h.EnsureIndexer(h.ctx, input); err != nil {
		logger.Warn("indexer restart failed", "error", err)
		return
	}
	// Carry the attempt count onto the replacement so a crash-loop still
	// exhausts the budget.
	h.mu.Lock()
	if cur, ok := h.entries[id]; ok {
		cur.restartAttempts = nextAttempt
	}
	h.mu.Unlock()
}

// resolveSecret loads indexing credential bytes from the configured vault.
func (h *KubernetesInProcessIndexHost) resolveSecret(ctx context.Context, ref domain.SecretRef) ([]byte, error) {
	if ref == "" {
		return nil, fmt.Errorf("missing secret ref")
	}
	if h.vault == nil {
		return nil, fmt.Errorf("no vault configured")
	}
	val, err := h.vault.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, fmt.Errorf("empty vault secret %q", ref)
	}
	return val, nil
}

// StopIndexer stops the indexer for targetID. It is idempotent, does not
// delete inventory, and awaits shutdown bounded by ctx.
func (h *KubernetesInProcessIndexHost) StopIndexer(ctx context.Context, targetID domain.TargetID) error {
	op := h.lockTarget(targetID)
	defer op.Unlock()
	return h.stopLocked(ctx, targetID, true)
}

// StopAllIndexers stops every hosted indexer by delegating to StopAll.
func (h *KubernetesInProcessIndexHost) StopAllIndexers(ctx context.Context) error {
	return h.StopAll(ctx)
}

// StopAll cancels every indexer (including in-flight starts), suppresses
// unexpected-exit restart for the duration of the call, and waits for exit
// bounded by ctx. After it returns, new EnsureIndexer calls are allowed again.
func (h *KubernetesInProcessIndexHost) StopAll(ctx context.Context) error {
	type stopHandle struct {
		id     domain.TargetID
		entry  *managedIndexer
		cancel context.CancelFunc
		done   <-chan struct{}
	}

	h.mu.Lock()
	h.shuttingDown = true
	handles := make([]stopHandle, 0, len(h.entries))
	for id, entry := range h.entries {
		entry.intentionalStop = true
		if entry.readinessCancel != nil {
			entry.readinessCancel()
		}
		handles = append(handles, stopHandle{
			id:     id,
			entry:  entry,
			cancel: entry.cancel,
			done:   entry.done,
		})
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.shuttingDown = false
		h.mu.Unlock()
	}()

	if len(handles) == 0 {
		return nil
	}

	for _, handle := range handles {
		if handle.cancel != nil {
			handle.cancel()
		}
	}

	type waitResult struct {
		id       domain.TargetID
		entry    *managedIndexer
		done     <-chan struct{}
		timedOut bool
	}
	results := make(chan waitResult, len(handles))
	for _, handle := range handles {
		go func(handle stopHandle) {
			if handle.done == nil {
				results <- waitResult{id: handle.id, entry: handle.entry, done: handle.done}
				return
			}
			select {
			case <-handle.done:
				results <- waitResult{id: handle.id, entry: handle.entry, done: handle.done}
			case <-ctx.Done():
				results <- waitResult{id: handle.id, entry: handle.entry, done: handle.done, timedOut: true}
			}
		}(handle)
	}

	var firstErr error
	for range handles {
		r := <-results
		if !r.timedOut {
			h.mu.Lock()
			if cur, still := h.entries[r.id]; still && cur == r.entry {
				delete(h.entries, r.id)
			}
			h.mu.Unlock()
			continue
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("stop all: waiting for %s: %w", r.id, ctx.Err())
		}
		// Leave the entry tracked until the indexer finishes so a concurrent
		// EnsureIndexer cannot start a duplicate.
		if r.done != nil {
			go func(id domain.TargetID, entry *managedIndexer, done <-chan struct{}) {
				<-done
				h.mu.Lock()
				if cur, still := h.entries[id]; still && cur == entry {
					delete(h.entries, id)
				}
				h.mu.Unlock()
			}(r.id, r.entry, r.done)
		} else {
			h.mu.Lock()
			if cur, still := h.entries[r.id]; still && cur == r.entry {
				delete(h.entries, r.id)
			}
			h.mu.Unlock()
		}
	}
	return firstErr
}

// stopLocked stops one target. The caller must already hold that target's
// op lock. When intentional is true, local restart is fenced off for the entry.
func (h *KubernetesInProcessIndexHost) stopLocked(ctx context.Context, id domain.TargetID, intentional bool) error {
	h.mu.Lock()
	entry, ok := h.entries[id]
	if !ok {
		h.mu.Unlock()
		return nil
	}
	if intentional {
		entry.intentionalStop = true
		entry.restartEligible = false
	}
	if entry.readinessCancel != nil {
		entry.readinessCancel()
	}
	cancel := entry.cancel
	done := entry.done
	starting := entry.starting
	readyWait := entry.readyWait
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if starting && done == nil {
		// Readiness still in progress; wait for the ensure attempt to finish
		// removing or promoting the entry, then stop again if it became ready.
		if readyWait != nil {
			select {
			case <-readyWait:
			case <-ctx.Done():
				return fmt.Errorf("stop in-process indexer for %s: %w", id, ctx.Err())
			}
		}
		h.mu.Lock()
		entry, ok = h.entries[id]
		if !ok {
			h.mu.Unlock()
			return nil
		}
		if intentional {
			entry.intentionalStop = true
			entry.restartEligible = false
		}
		cancel = entry.cancel
		done = entry.done
		h.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done == nil {
			h.mu.Lock()
			if cur, still := h.entries[id]; still && cur == entry {
				delete(h.entries, id)
			}
			h.mu.Unlock()
			return nil
		}
	}

	if done == nil {
		h.mu.Lock()
		if cur, still := h.entries[id]; still && cur == entry {
			delete(h.entries, id)
		}
		h.mu.Unlock()
		return nil
	}

	select {
	case <-done:
		h.mu.Lock()
		if cur, still := h.entries[id]; still && cur == entry {
			delete(h.entries, id)
		}
		h.mu.Unlock()
		return nil
	case <-ctx.Done():
		go func() {
			<-done
			h.mu.Lock()
			if cur, still := h.entries[id]; still && cur == entry {
				delete(h.entries, id)
			}
			h.mu.Unlock()
		}()
		return fmt.Errorf("stop in-process indexer for %s: %w", id, ctx.Err())
	}
}

// HasIndexer reports whether the host currently tracks an entry for id
// (starting, ready, or otherwise still registered).
func (h *KubernetesInProcessIndexHost) HasIndexer(id domain.TargetID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.entries[id]
	return ok
}

// lockTarget returns the per-target ops mutex already locked. Hold it only for
// state transitions (ensure/stop/restart bookkeeping); do not hold it across
// discovery readiness or indexer run.
func (h *KubernetesInProcessIndexHost) lockTarget(id domain.TargetID) *sync.Mutex {
	h.mu.Lock()
	m, ok := h.targetOps[id]
	if !ok {
		m = &sync.Mutex{}
		h.targetOps[id] = m
	}
	h.mu.Unlock()
	m.Lock()
	return m
}

// restConfigFromIndexInput builds a client-go REST config from EnsureIndexer
// input. Timeout stays unset so long-lived watches are not killed.
func restConfigFromIndexInput(input IndexRuntimeInput) *rest.Config {
	cfg := &rest.Config{
		Host:        input.APIServer,
		BearerToken: string(input.Credential),
	}
	if input.CACert != "" {
		cfg.TLSClientConfig = rest.TLSClientConfig{CAData: []byte(input.CACert)}
	}
	return cfg
}

// indexRuntimeFingerprint identifies the effective runtime identity for
// idempotent EnsureIndexer and stop-and-replace decisions.
func indexRuntimeFingerprint(input IndexRuntimeInput) string {
	sum := sha256.Sum256(input.Credential)
	credID := "sha256:" + hex.EncodeToString(sum[:])
	if input.SecretRef != "" {
		credID = string(input.SecretRef) + "|" + credID
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		input.TargetID,
		input.ClusterResourceName,
		input.APIServer,
		input.CACert,
		credID,
		indexConfigDigest(input.IndexConfig),
	)
}

// indexConfigDigest summarizes watch/filter configuration for fingerprints.
func indexConfigDigest(cfg IndexConfig) string {
	if len(cfg.AllowList) == 0 && len(cfg.DenyList) == 0 && cfg.NamespaceFilter == nil {
		return defaultIndexConfigDigest
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%#v|%#v|%#v", cfg.AllowList, cfg.DenyList, cfg.NamespaceFilter)))
	return "cfg:" + hex.EncodeToString(sum[:8])
}

// checkDiscoveryReadiness reports whether discovery yields a usable filtered
// watchable GVR set. It does not wait for LIST or inventory writes. Partial
// discovery group errors are allowed when the resource list is non-nil.
func checkDiscoveryReadiness(disc discovery.DiscoveryInterface, cfg IndexConfig, logger *slog.Logger) error {
	supported, err := SupportedResources(disc, logger)
	if err != nil && supported == nil {
		return fmt.Errorf("discovery: %w", err)
	}
	filtered := FilterSupportedResources(supported, cfg.DenyList, cfg.AllowList, logger)
	if len(filtered) > 0 {
		return nil
	}
	if len(cfg.AllowList) > 0 {
		return fmt.Errorf("%w: %w", domain.ErrInvalidArgument, ErrIndexerAllowListEmpty)
	}
	return fmt.Errorf("discovery readiness: no watchable resources after filter")
}
