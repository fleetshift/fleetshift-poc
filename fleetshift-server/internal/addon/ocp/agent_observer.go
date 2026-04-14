package ocp

import (
	"context"
	"log/slog"
)

// AgentObserver is called at key points during OCP cluster delivery.
// Implementations should embed [NoOpAgentObserver] for forward
// compatibility with new methods added to this interface.
type AgentObserver interface {
	// ClusterDeliverStarted is called when the agent begins delivering
	// a single cluster spec. Returns a probe to track the operation.
	ClusterDeliverStarted(ctx context.Context, clusterName string) (context.Context, ClusterDeliverProbe)
}

// ClusterDeliverProbe tracks a single OCP cluster delivery.
// Implementations should embed [NoOpClusterDeliverProbe] for forward
// compatibility.
type ClusterDeliverProbe interface {
	// CredentialsResolved is called after credentials are obtained from
	// the credential provider. Provider is the credential type (e.g.,
	// "sso", "static").
	CredentialsResolved(provider string)

	// PhaseCompleted is called after each major phase of the OCP
	// provisioning workflow completes. Phase is a human-readable name
	// (e.g., "install-config", "manifests", "provision"); elapsed is
	// the duration of that phase in milliseconds.
	PhaseCompleted(phase string, elapsed int)

	// BootstrapCompleted is called after post-provision bootstrapping
	// (ServiceAccount, kubeconfig extraction, etc.) succeeds.
	BootstrapCompleted()

	// Error is called when the delivery fails.
	Error(err error)

	// End signals the delivery is complete. Called via defer.
	End()
}

// NoOpAgentObserver is an [AgentObserver] that returns no-op probes.
type NoOpAgentObserver struct{}

func (NoOpAgentObserver) ClusterDeliverStarted(ctx context.Context, _ string) (context.Context, ClusterDeliverProbe) {
	return ctx, NoOpClusterDeliverProbe{}
}

// NoOpClusterDeliverProbe is a [ClusterDeliverProbe] that discards all calls.
type NoOpClusterDeliverProbe struct{}

func (NoOpClusterDeliverProbe) CredentialsResolved(string)   {}
func (NoOpClusterDeliverProbe) PhaseCompleted(string, int)   {}
func (NoOpClusterDeliverProbe) BootstrapCompleted()          {}
func (NoOpClusterDeliverProbe) Error(error)                  {}
func (NoOpClusterDeliverProbe) End()                         {}

// SlogAgentObserver is an [AgentObserver] that logs via [slog].
type SlogAgentObserver struct {
	NoOpAgentObserver
	logger *slog.Logger
}

// NewSlogAgentObserver returns an observer that logs to logger.
func NewSlogAgentObserver(logger *slog.Logger) *SlogAgentObserver {
	return &SlogAgentObserver{logger: logger.With("component", "ocp")}
}

func (o *SlogAgentObserver) ClusterDeliverStarted(ctx context.Context, clusterName string) (context.Context, ClusterDeliverProbe) {
	return ctx, &slogClusterDeliverProbe{
		logger: o.logger.With(slog.String("cluster", clusterName)),
		ctx:    ctx,
	}
}

type slogClusterDeliverProbe struct {
	NoOpClusterDeliverProbe
	logger *slog.Logger
	ctx    context.Context
	err    error
}

func (p *slogClusterDeliverProbe) CredentialsResolved(provider string) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "credentials resolved",
		slog.String("provider", provider),
	)
}

func (p *slogClusterDeliverProbe) PhaseCompleted(phase string, elapsed int) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "phase completed",
		slog.String("phase", phase),
		slog.Int("elapsed_ms", elapsed),
	)
}

func (p *slogClusterDeliverProbe) BootstrapCompleted() {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "bootstrap completed")
}

func (p *slogClusterDeliverProbe) Error(err error) { p.err = err }

func (p *slogClusterDeliverProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "cluster delivery failed",
			slog.String("error", p.err.Error()),
		)
	}
}
