// Package deliveryrecordrepotest provides contract tests for
// [domain.DeliveryRecordRepository] implementations.
package deliveryrecordrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.DeliveryRecordRepository] for each test.
type Factory func(t *testing.T) domain.DeliveryRecordRepository

// Run exercises the [domain.DeliveryRecordRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC)

	t.Run("PutAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		rec := domain.DeliveryRecord{
			DeploymentID: "d1",
			TargetID:     "t1",
			Manifests:    []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			State:        domain.DeliveryStateDelivered,
			UpdatedAt:    now,
		}

		if err := repo.Put(ctx, rec); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := repo.Get(ctx, "d1", "t1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.State != domain.DeliveryStateDelivered {
			t.Errorf("State = %q, want %q", got.State, domain.DeliveryStateDelivered)
		}
		if len(got.Manifests) != 1 {
			t.Errorf("Manifests len = %d, want 1", len(got.Manifests))
		}
	})

	t.Run("PutUpserts", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		rec := domain.DeliveryRecord{
			DeploymentID: "d1", TargetID: "t1",
			State: domain.DeliveryStatePending, UpdatedAt: now,
		}
		_ = repo.Put(ctx, rec)

		rec.State = domain.DeliveryStateDelivered
		rec.UpdatedAt = now.Add(time.Minute)
		if err := repo.Put(ctx, rec); err != nil {
			t.Fatalf("second Put: %v", err)
		}

		got, _ := repo.Get(ctx, "d1", "t1")
		if got.State != domain.DeliveryStateDelivered {
			t.Errorf("State after upsert = %q, want %q", got.State, domain.DeliveryStateDelivered)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "d1", "t1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("ListByDeployment", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		for _, tid := range []domain.TargetID{"t1", "t2"} {
			_ = repo.Put(ctx, domain.DeliveryRecord{
				DeploymentID: "d1", TargetID: tid,
				State: domain.DeliveryStateDelivered, UpdatedAt: now,
			})
		}
		_ = repo.Put(ctx, domain.DeliveryRecord{
			DeploymentID: "d2", TargetID: "t3",
			State: domain.DeliveryStateDelivered, UpdatedAt: now,
		})

		got, err := repo.ListByDeployment(ctx, "d1")
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListByDeployment: got %d, want 2", len(got))
		}
	})

	t.Run("DeleteByDeployment", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Put(ctx, domain.DeliveryRecord{
			DeploymentID: "d1", TargetID: "t1",
			State: domain.DeliveryStateDelivered, UpdatedAt: now,
		})
		_ = repo.Put(ctx, domain.DeliveryRecord{
			DeploymentID: "d1", TargetID: "t2",
			State: domain.DeliveryStateDelivered, UpdatedAt: now,
		})

		if err := repo.DeleteByDeployment(ctx, "d1"); err != nil {
			t.Fatalf("DeleteByDeployment: %v", err)
		}

		got, _ := repo.ListByDeployment(ctx, "d1")
		if len(got) != 0 {
			t.Fatalf("after delete: got %d records, want 0", len(got))
		}
	})
}
