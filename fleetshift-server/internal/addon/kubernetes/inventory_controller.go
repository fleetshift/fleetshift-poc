package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// PropInventoryMode is the target property that selects whether this
// server hosts an in-process indexer for the target.
const PropInventoryMode = "fleetshift.inventory.mode"

// InventoryMode is the effective fleetshift.inventory.mode value.
type InventoryMode string

const (
	// InventoryModeInProcess keeps the public target-property value "local",
	// meaning this server process hosts the indexer.
	InventoryModeInProcess InventoryMode = "local"
	// InventoryModeExternal means an external agent reports inventory;
	// this server must not host an in-process indexer for the target.
	InventoryModeExternal InventoryMode = "external"
	// InventoryModeDisabled means no Kubernetes indexing should run for
	// the target.
	InventoryModeDisabled InventoryMode = "disabled"
)

// defaultIndexConfigDigest is included in the runtime fingerprint so a
// future change to effective IndexConfig construction invalidates
// running indexers. Per-target IndexConfig overrides are not sourced
// yet; every in-process indexer uses DefaultKubernetesSchema().
const defaultIndexConfigDigest = "default-kubernetes-schema"

const (
	defaultReconcileInterval = 30 * time.Second
	defaultStopTimeout       = 5 * time.Second
)

// TargetLister lists every known target. The in-process controller treats
// the list as authoritative desired-state input.
type TargetLister interface {
	// ListTargets returns every target known to the platform.
	ListTargets(ctx context.Context) ([]domain.TargetInfo, error)
}

// InProcessIndexRuntime hosts per-target in-process indexers for one target
// type. Start and stop must be idempotent by target ID.
type InProcessIndexRuntime interface {
	// StartIndexer starts an in-process indexer for target. It is
	// idempotent by target ID and must not delete inventory.
	StartIndexer(ctx context.Context, target domain.TargetInfo) error
	// StopIndexer stops the in-process indexer for target. It is
	// idempotent and must not delete inventory.
	StopIndexer(ctx context.Context, target domain.TargetInfo) error
	// StopAllIndexers stops every running in-process indexer. It is safe
	// during server shutdown and must not perform inventory cleanup.
	StopAllIndexers(ctx context.Context) error
	// HasIndexer reports whether an in-process indexer is currently tracked
	// for id. Controllers use this to detect unexpected runner exit
	// without treating every reconcile as a start.
	HasIndexer(id domain.TargetID) bool
}

// TargetIndexDecision is the policy result for one target that should
// have an in-process indexer.
type TargetIndexDecision struct {
	// Fingerprint captures every input that must force a restart when
	// it changes.
	Fingerprint string
}

// InProcessIndexPolicy decides whether a target should have an in-process
// indexer and, if so, what runtime fingerprint it should run under.
type InProcessIndexPolicy interface {
	// Desired reports whether target should have an in-process indexer.
	// When ok is true, decision.Fingerprint is the runtime fingerprint
	// the indexer must run under.
	Desired(target domain.TargetInfo) (decision TargetIndexDecision, ok bool)
}

// DefaultInProcessIndexPolicy is the initial rule for Kubernetes targets.
// Desired when:
//
//	target.Type == kubernetes
//	target.State in ["", "ready"]
//	effective mode == "local" (the in-process/default mode)
type DefaultInProcessIndexPolicy struct{}

// Desired implements [InProcessIndexPolicy].
func (DefaultInProcessIndexPolicy) Desired(target domain.TargetInfo) (TargetIndexDecision, bool) {
	if target.Type() != TargetType {
		return TargetIndexDecision{}, false
	}
	state := target.State()
	if state != "" && state != domain.TargetStateReady {
		return TargetIndexDecision{}, false
	}
	mode := effectiveInventoryMode(target)
	if mode != InventoryModeInProcess {
		return TargetIndexDecision{}, false
	}
	return TargetIndexDecision{Fingerprint: runtimeFingerprint(target)}, true
}

func effectiveInventoryMode(target domain.TargetInfo) InventoryMode {
	mode := InventoryMode(target.Properties()[PropInventoryMode])
	if mode == "" {
		return InventoryModeInProcess
	}
	return mode
}

// runtimeFingerprint includes every target field that affects in-process
// indexer construction for the first implementation.
//
// TODO: rotating a vault secret in place (same PropServiceAccountTokenRef,
// new secret bytes) does not change this fingerprint, so the indexer keeps
// the token loaded at start until a property/ref change, unexpected exit, or
// process restart. Prefer bumping the ref (or another fingerprinted prop) on
// rotation; later, include a vault secret version/generation if the vault
// API exposes one — do not fingerprint raw secret bytes.
func runtimeFingerprint(target domain.TargetInfo) string {
	props := target.Properties()
	return fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s|%s|%s|%s",
		target.ID(),
		target.Type(),
		target.State(),
		effectiveInventoryMode(target),
		props[PropAPIServer],
		props[PropCACert],
		props[PropServiceAccountToken],
		props[PropServiceAccountTokenRef],
		defaultIndexConfigDigest,
	)
}

type runningTarget struct {
	Target      domain.TargetInfo
	Fingerprint string
}

// InProcessIndexController reconciles in-process Kubernetes indexers against
// the target store. Target lifecycle notifications are wake-up and
// bounded-stop hints only; startup and periodic reconcile remain the
// correctness path.
type InProcessIndexController struct {
	targets        TargetLister
	runtime        InProcessIndexRuntime
	policy         InProcessIndexPolicy
	logger         *slog.Logger
	reconcileEvery time.Duration
	stopTimeout    time.Duration

	wakeCh chan struct{}

	mu      sync.Mutex
	running map[domain.TargetID]runningTarget
	// terminating suppresses in-process restarts while target cleanup is in
	// progress and the target row may still be visible to reconcile.
	// TODO: target terminating state should eventually be saved on
	// target itself in platform db. Change this when target status gets
	// implemented at platform level.
	terminating map[domain.TargetID]struct{}
}

// InProcessIndexControllerOption configures an [InProcessIndexController].
type InProcessIndexControllerOption func(*InProcessIndexController)

// WithReconcileInterval overrides the periodic reconcile interval.
// Non-positive values are ignored.
func WithReconcileInterval(d time.Duration) InProcessIndexControllerOption {
	return func(c *InProcessIndexController) {
		if d > 0 {
			c.reconcileEvery = d
		}
	}
}

// WithStopTimeout overrides the bounded stop timeout used for
// terminating notifications. Non-positive values are ignored.
func WithStopTimeout(d time.Duration) InProcessIndexControllerOption {
	return func(c *InProcessIndexController) {
		if d > 0 {
			c.stopTimeout = d
		}
	}
}

// NewInProcessIndexController creates a controller. logger may be nil.
func NewInProcessIndexController(
	targets TargetLister,
	runtime InProcessIndexRuntime,
	policy InProcessIndexPolicy,
	logger *slog.Logger,
	opts ...InProcessIndexControllerOption,
) *InProcessIndexController {
	if logger == nil {
		logger = slog.Default()
	}
	c := &InProcessIndexController{
		targets:        targets,
		runtime:        runtime,
		policy:         policy,
		logger:         logger.With("component", "kubernetes-index-controller"),
		reconcileEvery: defaultReconcileInterval,
		stopTimeout:    defaultStopTimeout,
		wakeCh:         make(chan struct{}, 1),
		running:        make(map[domain.TargetID]runningTarget),
		terminating:    make(map[domain.TargetID]struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// NotifyTargetReady clears any previous terminating suppression for
// target and wakes reconcile soon. It never fails the caller.
func (c *InProcessIndexController) NotifyTargetReady(_ context.Context, target domain.TargetInfo) {
	c.mu.Lock()
	delete(c.terminating, target.ID())
	c.mu.Unlock()
	c.wake()
}

// NotifyTargetTerminating requests a bounded best-effort stop for the
// target, then wakes reconcile. Stop failures are logged only.
func (c *InProcessIndexController) NotifyTargetTerminating(ctx context.Context, target domain.TargetInfo) {
	c.mu.Lock()
	c.terminating[target.ID()] = struct{}{}
	c.mu.Unlock()

	stopCtx, cancel := context.WithTimeout(ctx, c.stopTimeout)
	defer cancel()
	if err := c.runtime.StopIndexer(stopCtx, target); err != nil {
		c.logger.Warn("bounded stop on terminating target failed",
			"target", string(target.ID()),
			"error", err,
		)
		c.wake()
		return
	}
	c.mu.Lock()
	delete(c.running, target.ID())
	c.mu.Unlock()
	c.wake()
}

// Run performs an initial reconcile, then reconciles on wake-ups and
// on the periodic interval until ctx is cancelled. On shutdown it
// calls [InProcessIndexRuntime.StopAllIndexers].
func (c *InProcessIndexController) Run(ctx context.Context) {
	c.wake() // ensure startup reconcile even if no notification arrives

	ticker := time.NewTicker(c.reconcileEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			stopCtx, cancel := context.WithTimeout(context.Background(), c.stopTimeout)
			defer cancel()
			if err := c.runtime.StopAllIndexers(stopCtx); err != nil {
				c.logger.Warn("stop all in-process indexers on shutdown failed", "error", err)
			}
			c.mu.Lock()
			c.running = make(map[domain.TargetID]runningTarget)
			c.terminating = make(map[domain.TargetID]struct{})
			c.mu.Unlock()
			return
		case <-c.wakeCh:
			c.reconcile(ctx)
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *InProcessIndexController) wake() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

func (c *InProcessIndexController) reconcile(ctx context.Context) {
	targets, err := c.targets.ListTargets(ctx)
	if err != nil {
		c.logger.Error("list targets for reconcile failed", "error", err)
		return
	}

	present := make(map[domain.TargetID]struct{}, len(targets))
	for _, target := range targets {
		present[target.ID()] = struct{}{}
	}

	c.mu.Lock()
	for id := range c.terminating {
		if _, ok := present[id]; !ok {
			delete(c.terminating, id)
		}
	}
	terminatingSnapshot := make(map[domain.TargetID]struct{}, len(c.terminating))
	for id := range c.terminating {
		terminatingSnapshot[id] = struct{}{}
	}
	runningSnapshot := make(map[domain.TargetID]runningTarget, len(c.running))
	for id, rt := range c.running {
		runningSnapshot[id] = rt
	}
	c.mu.Unlock()

	desired := make(map[domain.TargetID]TargetIndexDecision, len(targets))
	desiredTargets := make(map[domain.TargetID]domain.TargetInfo, len(targets))
	for _, target := range targets {
		if _, terminating := terminatingSnapshot[target.ID()]; terminating {
			continue
		}
		decision, ok := c.policy.Desired(target)
		if !ok {
			continue
		}
		desired[target.ID()] = decision
		desiredTargets[target.ID()] = target
	}

	for id, rt := range runningSnapshot {
		if _, ok := desired[id]; ok {
			continue
		}
		if err := c.stopIndexerBounded(ctx, rt.Target); err != nil {
			c.logger.Warn("stop undesired in-process indexer failed",
				"target", string(id),
				"error", err,
			)
			continue
		}
		c.mu.Lock()
		delete(c.running, id)
		c.mu.Unlock()
	}

	for id, decision := range desired {
		target := desiredTargets[id]
		c.mu.Lock()
		if _, terminating := c.terminating[id]; terminating {
			c.mu.Unlock()
			continue
		}
		rt, isRunning := c.running[id]
		c.mu.Unlock()

		if isRunning && rt.Fingerprint == decision.Fingerprint {
			if c.runtime.HasIndexer(id) {
				continue
			}
			// Runner exited unexpectedly; drop controller state and
			// fall through to start a replacement.
			c.mu.Lock()
			delete(c.running, id)
			c.mu.Unlock()
		} else if isRunning {
			if err := c.stopIndexerBounded(ctx, rt.Target); err != nil {
				c.logger.Warn("stop in-process indexer before fingerprint restart failed",
					"target", string(id),
					"error", err,
				)
				continue
			}
			c.mu.Lock()
			delete(c.running, id)
			c.mu.Unlock()
		}
		if err := c.runtime.StartIndexer(ctx, target); err != nil {
			c.logger.Warn("start in-process indexer failed; will retry on next reconcile",
				"target", string(id),
				"error", err,
			)
			continue
		}
		c.mu.Lock()
		c.running[id] = runningTarget{Target: target, Fingerprint: decision.Fingerprint}
		c.mu.Unlock()
	}
}

// stopIndexerBounded stops one indexer with the controller's stop timeout,
// matching terminating-notification bounds so reconcile retries cannot hang
// indefinitely on a stuck runner.
func (c *InProcessIndexController) stopIndexerBounded(ctx context.Context, target domain.TargetInfo) error {
	stopCtx, cancel := context.WithTimeout(ctx, c.stopTimeout)
	defer cancel()
	return c.runtime.StopIndexer(stopCtx, target)
}
