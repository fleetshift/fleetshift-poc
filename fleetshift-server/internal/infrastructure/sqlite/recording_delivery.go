package sqlite

import (
	"context"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// RecordingDeliveryService implements [domain.DeliveryService] by writing
// delivery records to SQLite. This is the naive implementation used until
// the real fleetlet-based delivery is available.
type RecordingDeliveryService struct {
	Records *DeliveryRecordRepo
	Now     func() time.Time
}

func (s *RecordingDeliveryService) Deliver(ctx context.Context, target domain.TargetInfo, deploymentID domain.DeploymentID, manifests []domain.Manifest) (domain.DeliveryResult, error) {
	now := s.now()
	rec := domain.DeliveryRecord{
		DeploymentID: deploymentID,
		TargetID:     target.ID,
		Manifests:    manifests,
		State:        domain.DeliveryStateDelivered,
		UpdatedAt:    now,
	}
	if err := s.Records.Put(ctx, rec); err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed}, err
	}
	return domain.DeliveryResult{State: domain.DeliveryStateDelivered}, nil
}

func (s *RecordingDeliveryService) Remove(ctx context.Context, target domain.TargetInfo, deploymentID domain.DeploymentID) error {
	// For now, removing means deleting the record. A real implementation
	// would send a removal command through the fleetlet.
	_, err := s.Records.Get(ctx, deploymentID, target.ID)
	if err != nil {
		return nil
	}
	return s.Records.Put(ctx, domain.DeliveryRecord{
		DeploymentID: deploymentID,
		TargetID:     target.ID,
		State:        domain.DeliveryStatePending,
		UpdatedAt:    s.now(),
	})
}

func (s *RecordingDeliveryService) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
