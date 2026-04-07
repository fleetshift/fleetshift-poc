package hcp

import (
	"context"
	"log/slog"
)

// AgentObserver is called at key points during HCP cluster delivery.
// Implementations should embed [NoOpAgentObserver] for forward
// compatibility with new methods added to this interface.
type AgentObserver interface {
	// ClusterDeliverStarted is called when the agent begins delivering
	// a single cluster spec. Returns a probe to track the operation.
	ClusterDeliverStarted(ctx context.Context, clusterName string) (context.Context, ClusterDeliverProbe)
}

// ClusterDeliverProbe tracks a single HCP cluster delivery.
// Implementations should embed [NoOpClusterDeliverProbe] for forward
// compatibility.
type ClusterDeliverProbe interface {
	// InfraCreated is called after the VPC infrastructure is provisioned.
	InfraCreated(vpcID string)

	// IAMCreated is called after the OIDC provider is created for IRSA.
	IAMCreated(oidcProviderArn string)

	// CRDsApplied is called after HyperShift CRDs are applied to the
	// management cluster.
	CRDsApplied()

	// HostedClusterAvailable is called when the HostedCluster reports
	// its API server as available.
	HostedClusterAvailable(apiServer string)

	// TargetRegistered is called after the cluster is registered as a
	// target in the fleet.
	TargetRegistered(targetID string)

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

func (NoOpClusterDeliverProbe) InfraCreated(string)            {}
func (NoOpClusterDeliverProbe) IAMCreated(string)              {}
func (NoOpClusterDeliverProbe) CRDsApplied()                   {}
func (NoOpClusterDeliverProbe) HostedClusterAvailable(string)  {}
func (NoOpClusterDeliverProbe) TargetRegistered(string)        {}
func (NoOpClusterDeliverProbe) Error(error)                    {}
func (NoOpClusterDeliverProbe) End()                           {}

// SlogAgentObserver is an [AgentObserver] that logs via [slog].
type SlogAgentObserver struct {
	NoOpAgentObserver
	logger *slog.Logger
}

// NewSlogAgentObserver returns an observer that logs to logger.
func NewSlogAgentObserver(logger *slog.Logger) *SlogAgentObserver {
	return &SlogAgentObserver{logger: logger.With("component", "hcp")}
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

func (p *slogClusterDeliverProbe) InfraCreated(vpcID string) {
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "infrastructure created",
		slog.String("vpc_id", vpcID),
	)
}

func (p *slogClusterDeliverProbe) IAMCreated(oidcProviderArn string) {
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "IAM OIDC provider created",
		slog.String("oidc_provider_arn", oidcProviderArn),
	)
}

func (p *slogClusterDeliverProbe) CRDsApplied() {
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "HyperShift CRDs applied")
}

func (p *slogClusterDeliverProbe) HostedClusterAvailable(apiServer string) {
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "HostedCluster available",
		slog.String("api_server", apiServer),
	)
}

func (p *slogClusterDeliverProbe) TargetRegistered(targetID string) {
	p.logger.LogAttrs(p.ctx, slog.LevelInfo, "target registered",
		slog.String("target_id", targetID),
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
