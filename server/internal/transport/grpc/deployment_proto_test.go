package grpc

import (
	"testing"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestDeploymentToProto_ReconcilingRespectsPause(t *testing.T) {
	tests := []struct {
		name            string
		state           domain.FulfillmentState
		pauseReason     string
		wantReconciling bool
	}{
		{
			name:            "creating without pause is reconciling",
			state:           domain.FulfillmentStateCreating,
			wantReconciling: true,
		},
		{
			name:            "deleting without pause is reconciling",
			state:           domain.FulfillmentStateDeleting,
			wantReconciling: true,
		},
		{
			name:            "creating with pause is not reconciling",
			state:           domain.FulfillmentStateCreating,
			pauseReason:     "credential rotation required",
			wantReconciling: false,
		},
		{
			name:            "deleting with pause is not reconciling",
			state:           domain.FulfillmentStateDeleting,
			pauseReason:     "credential rotation required",
			wantReconciling: false,
		},
		{
			name:            "active without pause is not reconciling",
			state:           domain.FulfillmentStateActive,
			wantReconciling: false,
		},
		{
			name:            "active with pause is not reconciling",
			state:           domain.FulfillmentStateActive,
			pauseReason:     "paused",
			wantReconciling: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := domain.DeploymentView{
				Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
					Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
				}),
				Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
					ID:          "f1",
					State:       tt.state,
					PauseReason: tt.pauseReason,
				}),
			}

			got := deploymentToProto(view)

			if got.GetReconciling() != tt.wantReconciling {
				t.Errorf("Reconciling = %v, want %v", got.GetReconciling(), tt.wantReconciling)
			}
			if got.GetPauseReason() != tt.pauseReason {
				t.Errorf("PauseReason = %q, want %q", got.GetPauseReason(), tt.pauseReason)
			}

			wantState := fulfillmentStateToProto(tt.state)
			if got.GetState() != wantState {
				t.Errorf("State = %v, want %v", got.GetState(), wantState)
			}
		})
	}
}

func TestDeploymentToProto_PauseReasonPopulated(t *testing.T) {
	view := domain.DeploymentView{
		Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
		}),
		Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID:          "f1",
			State:       domain.FulfillmentStateCreating,
			PauseReason: "credential rotation required",
		}),
	}

	got := deploymentToProto(view)

	if got.GetState() != pb.Deployment_STATE_CREATING {
		t.Errorf("State = %v, want STATE_CREATING", got.GetState())
	}
	if got.GetPauseReason() != "credential rotation required" {
		t.Errorf("PauseReason = %q, want %q", got.GetPauseReason(), "credential rotation required")
	}
}
