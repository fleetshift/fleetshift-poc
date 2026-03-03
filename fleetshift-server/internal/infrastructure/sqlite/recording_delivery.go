package sqlite

import (
	"context"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// RecordingDeliveryService implements [domain.DeliveryAgent] (and
// [domain.DeliveryService]) by writing delivery records to SQLite
// without performing real delivery. Useful as a stub agent for
// development, testing, or target types that have no real delivery
// agent registered yet.
type RecordingDeliveryService struct {
	Deliveries *DeliveryRepo
	Now        func() time.Time
}

func (s *RecordingDeliveryService) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	now := s.now()
	d := domain.Delivery{
		ID:           deliveryID,
		DeploymentID: deploymentIDFromDeliveryID(deliveryID),
		TargetID:     target.ID,
		Manifests:    manifests,
		State:        domain.DeliveryStateDelivered,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.Deliveries.Put(ctx, d); err != nil {
		result := domain.DeliveryResult{State: domain.DeliveryStateFailed}
		signaler.Done(ctx, result)
		return result, err
	}
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (s *RecordingDeliveryService) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ *domain.DeliverySignaler) error {
	_, err := s.Deliveries.GetByDeploymentTarget(ctx, deploymentIDFromDeliveryID(deliveryID), target.ID)
	if err != nil {
		return nil
	}
	return s.Deliveries.Put(ctx, domain.Delivery{
		ID:           deliveryID,
		DeploymentID: deploymentIDFromDeliveryID(deliveryID),
		TargetID:     target.ID,
		State:        domain.DeliveryStatePending,
		CreatedAt:    s.now(),
		UpdatedAt:    s.now(),
	})
}

func (s *RecordingDeliveryService) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// deploymentIDFromDeliveryID extracts the deployment ID from a
// composite delivery ID of the form "deploymentID:targetID".
func deploymentIDFromDeliveryID(id domain.DeliveryID) domain.DeploymentID {
	for i := 0; i < len(id); i++ {
		if id[i] == ':' {
			return domain.DeploymentID(id[:i])
		}
	}
	return domain.DeploymentID(id)
}
