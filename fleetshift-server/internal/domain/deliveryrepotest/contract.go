// Package deliveryrepotest provides contract tests for
// [domain.DeliveryRepository] implementations.
package deliveryrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.DeliveryRepository] for each test.
type Factory func(t *testing.T) domain.DeliveryRepository

// Run exercises the [domain.DeliveryRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC)

	t.Run("PutAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		d := domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f1:t1",
			FulfillmentID: "f1",
			TargetID:      "t1",
			Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			State:         domain.DeliveryStateDelivered,
			CreatedAt:     now,
			UpdatedAt:     now,
		})

		if err := repo.Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := repo.Get(ctx, "f1:t1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.State() != domain.DeliveryStateDelivered {
			t.Errorf("State = %q, want %q", got.State(), domain.DeliveryStateDelivered)
		}
		if len(got.Manifests()) != 1 {
			t.Errorf("Manifests len = %d, want 1", len(got.Manifests()))
		}
		if got.ID() != "f1:t1" {
			t.Errorf("ID = %q, want %q", got.ID(), "f1:t1")
		}
	})

	t.Run("GetByFulfillmentTarget", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		d := domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f1:t1",
			FulfillmentID: "f1",
			TargetID:      "t1",
			Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			State:         domain.DeliveryStateDelivered,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
		if err := repo.Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := repo.GetByFulfillmentTarget(ctx, "f1", "t1")
		if err != nil {
			t.Fatalf("GetByFulfillmentTarget: %v", err)
		}
		if got.ID() != "f1:t1" {
			t.Errorf("ID = %q, want %q", got.ID(), "f1:t1")
		}
	})

	t.Run("PutUpserts", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		d := domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f1:t1",
			FulfillmentID: "f1",
			TargetID:      "t1",
			State:         domain.DeliveryStatePending,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
		_ = repo.Put(ctx, d)

		if err := d.TransitionTo(domain.DeliveryStateDelivered, now.Add(time.Minute)); err != nil {
			t.Fatalf("TransitionTo: %v", err)
		}
		if err := repo.Put(ctx, d); err != nil {
			t.Fatalf("second Put: %v", err)
		}

		got, _ := repo.Get(ctx, "f1:t1")
		if got.State() != domain.DeliveryStateDelivered {
			t.Errorf("State after upsert = %q, want %q", got.State(), domain.DeliveryStateDelivered)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("ListByFulfillment", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		for _, tid := range []domain.TargetID{"t1", "t2"} {
			_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID:            domain.DeliveryID("f1:" + string(tid)),
				FulfillmentID: "f1",
				TargetID:      tid,
				State:         domain.DeliveryStateDelivered,
				CreatedAt:     now,
				UpdatedAt:     now,
			}))
		}
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f2:t3",
			FulfillmentID: "f2",
			TargetID:      "t3",
			State:         domain.DeliveryStateDelivered,
			CreatedAt:     now,
			UpdatedAt:     now,
		}))

		got, err := repo.ListByFulfillment(ctx, "f1")
		if err != nil {
			t.Fatalf("ListByFulfillment: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListByFulfillment: got %d, want 2", len(got))
		}
	})

	t.Run("ListActive_All", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f1:t1", FulfillmentID: "f1", TargetID: "t1",
			State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f2:t2", FulfillmentID: "f2", TargetID: "t2",
			State: domain.DeliveryStateAccepted, CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f3:t3", FulfillmentID: "f3", TargetID: "t3",
			State: domain.DeliveryStateDelivered, CreatedAt: now, UpdatedAt: now,
		}))

		got, err := repo.ListActive(ctx, nil)
		if err != nil {
			t.Fatalf("ListActive(nil): %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListActive(nil): got %d, want 2", len(got))
		}
		byTarget := make(map[domain.TargetID]domain.DeliveryState, len(got))
		for _, d := range got {
			byTarget[d.TargetID()] = d.State()
		}
		if s, ok := byTarget["t1"]; !ok || s != domain.DeliveryStateProgressing {
			t.Errorf("expected t1 progressing, got ok=%v state=%q", ok, s)
		}
		if s, ok := byTarget["t2"]; !ok || s != domain.DeliveryStateAccepted {
			t.Errorf("expected t2 accepted, got ok=%v state=%q", ok, s)
		}
		if _, ok := byTarget["t3"]; ok {
			t.Error("t3 (delivered/terminal) should not be in active list")
		}
	})

	t.Run("ListActive_FilterByTargets", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f1:t1", FulfillmentID: "f1", TargetID: "t1",
			State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f2:t2", FulfillmentID: "f2", TargetID: "t2",
			State: domain.DeliveryStateAccepted, CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID: "f3:t3", FulfillmentID: "f3", TargetID: "t3",
			State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
		}))

		got, err := repo.ListActive(ctx, []domain.TargetID{"t1", "t3"})
		if err != nil {
			t.Fatalf("ListActive([t1,t3]): %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListActive([t1,t3]): got %d, want 2", len(got))
		}
		byTarget := make(map[domain.TargetID]domain.DeliveryState, len(got))
		for _, d := range got {
			byTarget[d.TargetID()] = d.State()
		}
		if s, ok := byTarget["t1"]; !ok || s != domain.DeliveryStateProgressing {
			t.Errorf("expected t1 progressing, got ok=%v state=%q", ok, s)
		}
		if s, ok := byTarget["t3"]; !ok || s != domain.DeliveryStatePending {
			t.Errorf("expected t3 pending, got ok=%v state=%q", ok, s)
		}

		got, err = repo.ListActive(ctx, []domain.TargetID{"t2"})
		if err != nil {
			t.Fatalf("ListActive([t2]): %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ListActive([t2]): got %d, want 1", len(got))
		}
		if got[0].TargetID() != "t2" {
			t.Errorf("TargetID = %q, want %q", got[0].TargetID(), "t2")
		}

		got, err = repo.ListActive(ctx, []domain.TargetID{"t-unknown"})
		if err != nil {
			t.Fatalf("ListActive([t-unknown]): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListActive([t-unknown]): got %d, want 0", len(got))
		}
	})

	t.Run("DeleteByFulfillment", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f1:t1",
			FulfillmentID: "f1",
			TargetID:      "t1",
			State:         domain.DeliveryStateDelivered,
			CreatedAt:     now,
			UpdatedAt:     now,
		}))
		_ = repo.Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            "f1:t2",
			FulfillmentID: "f1",
			TargetID:      "t2",
			State:         domain.DeliveryStateDelivered,
			CreatedAt:     now,
			UpdatedAt:     now,
		}))

		if err := repo.DeleteByFulfillment(ctx, "f1"); err != nil {
			t.Fatalf("DeleteByFulfillment: %v", err)
		}

		got, _ := repo.ListByFulfillment(ctx, "f1")
		if len(got) != 0 {
			t.Fatalf("after delete: got %d records, want 0", len(got))
		}
	})
}
