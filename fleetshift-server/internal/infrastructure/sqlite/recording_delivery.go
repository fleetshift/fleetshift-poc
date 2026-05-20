package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// RecordingDeliveryService implements [domain.DeliveryAgent] (and
// [domain.DeliveryService]) by writing delivery records to SQLite
// without performing real delivery. Useful as a stub agent for
// development, testing, or target types that have no real delivery
// agent registered yet.
//
// Deliver records the delivery and reports completion asynchronously
// via [domain.DeliveryReporter.ReportResult].
type RecordingDeliveryService struct {
	Store    domain.Store
	Reporter domain.DeliveryReporter
	Now      func() time.Time
}

func (s *RecordingDeliveryService) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	now := s.now()
	d := domain.Delivery{
		ID:            deliveryID,
		FulfillmentID: fulfillmentIDFromDeliveryID(deliveryID),
		TargetID:      target.ID,
		Manifests:     manifests,
		Generation:    generation,
		State:         domain.DeliveryStatePending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.Deliveries().Put(ctx, d); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if s.Reporter != nil {
		go func() {
			_ = s.Reporter.ReportResult(context.Background(), deliveryID, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		}()
	}

	return nil
}

func (s *RecordingDeliveryService) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Deliveries().GetByFulfillmentTarget(ctx, fulfillmentIDFromDeliveryID(deliveryID), target.ID)
	if err != nil {
		return nil
	}
	if err := tx.Deliveries().Put(ctx, domain.Delivery{
		ID:            deliveryID,
		FulfillmentID: fulfillmentIDFromDeliveryID(deliveryID),
		TargetID:      target.ID,
		State:         domain.DeliveryStatePending,
		CreatedAt:     s.now(),
		UpdatedAt:     s.now(),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RecordingDeliveryService) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// fulfillmentIDFromDeliveryID extracts the fulfillment ID from a
// composite delivery ID of the form "fulfillmentID:targetID".
func fulfillmentIDFromDeliveryID(id domain.DeliveryID) domain.FulfillmentID {
	for i := 0; i < len(id); i++ {
		if id[i] == ':' {
			return domain.FulfillmentID(id[:i])
		}
	}
	return domain.FulfillmentID(id)
}
