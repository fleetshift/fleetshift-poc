package ocp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	fleetshiftv1 "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

type provisionState struct {
	done       chan struct{}
	closeOnce  sync.Once
	completion *fleetshiftv1.OCPEngineCompletionRequest
	failure    *fleetshiftv1.OCPEngineFailureRequest
}

type callbackServer struct {
	fleetshiftv1.UnimplementedOCPEngineCallbackServiceServer
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
		return nil, fmt.Errorf("unknown cluster ID: %s", clusterID)
	}
	state, ok := val.(*provisionState)
	if !ok {
		return nil, fmt.Errorf("invalid provision state for cluster ID: %s", clusterID)
	}
	return state, nil
}

func (s *callbackServer) ReportPhaseResult(ctx context.Context, req *fleetshiftv1.OCPEnginePhaseResultRequest) (*fleetshiftv1.OCPEngineAck, error) {
	if _, err := s.authenticateCallback(ctx, req.ClusterId); err != nil {
		return nil, err
	}
	return &fleetshiftv1.OCPEngineAck{}, nil
}

func (s *callbackServer) ReportMilestone(ctx context.Context, req *fleetshiftv1.OCPEngineMilestoneRequest) (*fleetshiftv1.OCPEngineAck, error) {
	if _, err := s.authenticateCallback(ctx, req.ClusterId); err != nil {
		return nil, err
	}
	return &fleetshiftv1.OCPEngineAck{}, nil
}

func (s *callbackServer) ReportCompletion(ctx context.Context, req *fleetshiftv1.OCPEngineCompletionRequest) (*fleetshiftv1.OCPEngineAck, error) {
	state, err := s.authenticateCallback(ctx, req.ClusterId)
	if err != nil {
		return nil, err
	}
	state.completion = req
	state.closeOnce.Do(func() { close(state.done) })
	return &fleetshiftv1.OCPEngineAck{}, nil
}

func (s *callbackServer) ReportFailure(ctx context.Context, req *fleetshiftv1.OCPEngineFailureRequest) (*fleetshiftv1.OCPEngineAck, error) {
	state, err := s.authenticateCallback(ctx, req.ClusterId)
	if err != nil {
		return nil, err
	}
	state.failure = req
	state.closeOnce.Do(func() { close(state.done) })
	return &fleetshiftv1.OCPEngineAck{}, nil
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
