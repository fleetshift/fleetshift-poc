package kind

import (
	"context"
	"log/slog"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ConfigSource identifies which path [Agent.resolveConfig] took.
type ConfigSource string

const (
	ConfigSourceOIDC    ConfigSource = "oidc"
	ConfigSourceCustom  ConfigSource = "custom"
	ConfigSourceDefault ConfigSource = "default"
)

// AgentObserver is called at key points during kind cluster delivery.
// Implementations should embed [NoOpAgentObserver] for forward
// compatibility with new methods added to this interface.
type AgentObserver interface {
	// ClusterDeliverStarted is called when the agent begins delivering
	// a single cluster spec. Returns a probe to track the operation.
	ClusterDeliverStarted(ctx context.Context, clusterName string) (context.Context, ClusterDeliverProbe)
}

// ClusterDeliverProbe tracks a single kind cluster delivery.
// Implementations should embed [NoOpClusterDeliverProbe] for forward
// compatibility.
type ClusterDeliverProbe interface {
	// ConfigResolved is called after [Agent.resolveConfig] determines
	// the cluster configuration. For OIDC, issuerURL and audience are
	// the values derived from the caller's identity; for other sources
	// they are zero.
	ConfigResolved(source ConfigSource, issuerURL domain.IssuerURL, audience domain.Audience)

	// RBACBootstrapped is called after the caller is granted a
	// ClusterRoleBinding on the new cluster.
	RBACBootstrapped(subjectID domain.SubjectID, username string)

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

func (NoOpClusterDeliverProbe) ConfigResolved(ConfigSource, domain.IssuerURL, domain.Audience) {}
func (NoOpClusterDeliverProbe) RBACBootstrapped(domain.SubjectID, string)                      {}
func (NoOpClusterDeliverProbe) Error(error)                                                    {}
func (NoOpClusterDeliverProbe) End()                                                           {}

// SlogAgentObserver is an [AgentObserver] that logs via [slog].
type SlogAgentObserver struct {
	NoOpAgentObserver
	logger *slog.Logger
}

// NewSlogAgentObserver returns an observer that logs to logger.
func NewSlogAgentObserver(logger *slog.Logger) *SlogAgentObserver {
	return &SlogAgentObserver{logger: logger.With("component", "kind")}
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

func (p *slogClusterDeliverProbe) ConfigResolved(source ConfigSource, issuerURL domain.IssuerURL, audience domain.Audience) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	attrs := []slog.Attr{
		slog.String("config_source", string(source)),
	}
	if source == ConfigSourceOIDC {
		attrs = append(attrs,
			slog.String("oidc_issuer", string(issuerURL)),
			slog.String("oidc_audience", string(audience)),
		)
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "cluster config resolved", attrs...)
}

func (p *slogClusterDeliverProbe) RBACBootstrapped(subjectID domain.SubjectID, username string) {
	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "RBAC bootstrapped",
		slog.String("subject_id", string(subjectID)),
		slog.String("k8s_username", username),
		slog.String("role", "cluster-admin"),
	)
}

func (p *slogClusterDeliverProbe) Error(err error) { p.err = err }

func (p *slogClusterDeliverProbe) End() {
	if p.err != nil {
		p.logger.LogAttrs(p.ctx, slog.LevelError, "cluster delivery failed",
			slog.String("error", p.err.Error()),
		)
	}
}
