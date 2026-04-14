package ocp

import (
	"context"
	"fmt"
	"sync"

	fleetshiftv1 "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

// provisionState holds the in-memory state for a single in-flight provision.
// The agent creates it in Deliver() and stores it in a sync.Map. The callback
// server looks it up by cluster ID and signals completion/failure.
type provisionState struct {
	done       chan struct{}
	completion *fleetshiftv1.OCPCompletionRequest
	failure    *fleetshiftv1.OCPFailureRequest
}

// callbackServer implements fleetshiftv1.OCPCallbackServiceServer.
// It receives callbacks from ocp-engine subprocesses and signals the agent's
// provision state channel on completion or failure.
type callbackServer struct {
	fleetshiftv1.UnimplementedOCPCallbackServiceServer
	provisions *sync.Map
}

// getProvision loads a provision state from the sync.Map by cluster ID.
// Returns an error if the provision is not found.
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

// ReportPhaseResult receives phase result reports from ocp-engine.
// This is for observability only; the server acknowledges receipt but does
// not modify provision state.
func (s *callbackServer) ReportPhaseResult(ctx context.Context, req *fleetshiftv1.OCPPhaseResultRequest) (*fleetshiftv1.OCPAck, error) {
	_, err := s.getProvision(req.ClusterId)
	if err != nil {
		return nil, err
	}
	return &fleetshiftv1.OCPAck{}, nil
}

// ReportMilestone receives milestone reports from ocp-engine.
// This is for observability only; the server acknowledges receipt but does
// not modify provision state.
func (s *callbackServer) ReportMilestone(ctx context.Context, req *fleetshiftv1.OCPMilestoneRequest) (*fleetshiftv1.OCPAck, error) {
	_, err := s.getProvision(req.ClusterId)
	if err != nil {
		return nil, err
	}
	return &fleetshiftv1.OCPAck{}, nil
}

// ReportCompletion receives completion reports from ocp-engine.
// It stores the completion request in provision state and closes the done
// channel to signal the waiting agent.
func (s *callbackServer) ReportCompletion(ctx context.Context, req *fleetshiftv1.OCPCompletionRequest) (*fleetshiftv1.OCPAck, error) {
	state, err := s.getProvision(req.ClusterId)
	if err != nil {
		return nil, err
	}
	state.completion = req
	close(state.done)
	return &fleetshiftv1.OCPAck{}, nil
}

// ReportFailure receives failure reports from ocp-engine.
// It stores the failure request in provision state and closes the done
// channel to signal the waiting agent.
func (s *callbackServer) ReportFailure(ctx context.Context, req *fleetshiftv1.OCPFailureRequest) (*fleetshiftv1.OCPAck, error) {
	state, err := s.getProvision(req.ClusterId)
	if err != nil {
		return nil, err
	}
	state.failure = req
	close(state.done)
	return &fleetshiftv1.OCPAck{}, nil
}
