package ocp

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
)

type provisionState struct {
	done      chan struct{}
	closeOnce sync.Once

	mu         sync.Mutex
	completion *ocpv1.CompletionRequest
	failure    *ocpv1.FailureRequest
	workDir    string // retained on failure for cleanup by Remove()
}

type callbackServer struct {
	ocpv1.UnimplementedCallbackServiceServer
	provisions    *sync.Map
	tokenVerifier *CallbackTokenSigner
}

func (s *callbackServer) authenticateCallback(ctx context.Context, requestClusterID string) (*provisionState, error) {
	token := extractCallbackToken(ctx)
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "missing callback token")
	}

	tokenClusterID, err := s.tokenVerifier.Verify(token)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid callback token: %v", err)
	}

	if tokenClusterID != requestClusterID {
		return nil, status.Error(codes.PermissionDenied, "token cluster ID does not match request")
	}

	return s.getProvision(requestClusterID)
}

func (s *callbackServer) getProvision(clusterID string) (*provisionState, error) {
	val, ok := s.provisions.Load(clusterID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown cluster ID: %s", clusterID)
	}
	state, ok := val.(*provisionState)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid provision state for cluster ID: %s", clusterID)
	}
	return state, nil
}

func (s *callbackServer) ReportPhaseResult(ctx context.Context, req *ocpv1.PhaseResultRequest) (*ocpv1.Ack, error) {
	if _, err := s.authenticateCallback(ctx, req.ClusterId); err != nil {
		slog.Warn("callback: ReportPhaseResult auth failed", "cluster", req.ClusterId, "error", err)
		return nil, err
	}
	slog.Info("callback: phase result", "cluster", req.ClusterId, "phase", req.Phase, "status", req.Status)
	return &ocpv1.Ack{}, nil
}

func (s *callbackServer) ReportMilestone(ctx context.Context, req *ocpv1.MilestoneRequest) (*ocpv1.Ack, error) {
	if _, err := s.authenticateCallback(ctx, req.ClusterId); err != nil {
		slog.Warn("callback: ReportMilestone auth failed", "cluster", req.ClusterId, "error", err)
		return nil, err
	}
	slog.Info("callback: milestone", "cluster", req.ClusterId, "event", req.Event)
	return &ocpv1.Ack{}, nil
}

func (s *callbackServer) ReportCompletion(ctx context.Context, req *ocpv1.CompletionRequest) (*ocpv1.Ack, error) {
	state, err := s.authenticateCallback(ctx, req.ClusterId)
	if err != nil {
		slog.Warn("callback: ReportCompletion auth failed", "cluster", req.ClusterId, "error", err)
		return nil, err
	}
	slog.Info("callback: completion", "cluster", req.ClusterId, "infra_id", req.InfraId)
	state.mu.Lock()
	state.completion = req
	state.mu.Unlock()
	state.closeOnce.Do(func() { close(state.done) })
	return &ocpv1.Ack{}, nil
}

func (s *callbackServer) ReportFailure(ctx context.Context, req *ocpv1.FailureRequest) (*ocpv1.Ack, error) {
	state, err := s.authenticateCallback(ctx, req.ClusterId)
	if err != nil {
		slog.Warn("callback: ReportFailure auth failed", "cluster", req.ClusterId, "error", err)
		return nil, err
	}
	slog.Info("callback: failure", "cluster", req.ClusterId, "phase", req.Phase, "reason", req.FailureReason)
	state.mu.Lock()
	state.failure = req
	state.mu.Unlock()
	state.closeOnce.Do(func() { close(state.done) })
	return &ocpv1.Ack{}, nil
}

func extractCallbackToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(vals[0], prefix) {
		return ""
	}
	return vals[0][len(prefix):]
}
