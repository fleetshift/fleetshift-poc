// Package storetest provides contract tests for [domain.Store]
// implementations.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Store] for each test invocation.
type Factory func(t *testing.T) domain.Store

func sampleFulfillment(id domain.FulfillmentID, now time.Time) domain.Fulfillment {
	f := domain.Fulfillment{
		ID:        id,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type: domain.PlacementStrategyAll,
	}, now)
	return f
}

// Run exercises the [domain.Store] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("CommitPersists", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx.Rollback()

		if err := tx.Targets().Create(ctx, domain.TargetInfo{ID: "t1", Name: "cluster-a"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		tx2, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx2.Rollback()

		got, err := tx2.Targets().Get(ctx, "t1")
		if err != nil {
			t.Fatalf("Get after commit: %v", err)
		}
		if got.Name != "cluster-a" {
			t.Errorf("Name = %q, want %q", got.Name, "cluster-a")
		}
	})

	t.Run("RollbackReverts", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		if err := tx.Targets().Create(ctx, domain.TargetInfo{ID: "t1", Name: "cluster-a"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		tx2, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx2.Rollback()

		_, err = tx2.Targets().Get(ctx, "t1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after rollback: got %v, want ErrNotFound", err)
		}
	})

	t.Run("RollbackAfterCommitIsNoop", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		if err := tx.Targets().Create(ctx, domain.TargetInfo{ID: "t1", Name: "cluster-a"}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback after Commit should be no-op, got: %v", err)
		}

		tx2, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx2.Rollback()

		_, err = tx2.Targets().Get(ctx, "t1")
		if err != nil {
			t.Fatalf("data should still be present after rollback-after-commit: %v", err)
		}
	})

	t.Run("CrossRepoAtomicity", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		fixed := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx.Rollback()

		if err := tx.Targets().Create(ctx, domain.TargetInfo{ID: "t1", Name: "cluster-a"}); err != nil {
			t.Fatalf("Create target: %v", err)
		}
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment("f-cross", fixed)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		if err := tx.Deployments().Create(ctx, domain.Deployment{
			ID:            "d1",
			UID:           "uid-cross",
			FulfillmentID: "f-cross",
			CreatedAt:     fixed,
			UpdatedAt:     fixed,
			Etag:          "etag-1",
		}); err != nil {
			t.Fatalf("Create deployment: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		tx2, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx2.Rollback()

		if _, err := tx2.Targets().Get(ctx, "t1"); err != nil {
			t.Fatalf("target not found after cross-repo commit: %v", err)
		}
		if _, err := tx2.Fulfillments().Get(ctx, "f-cross"); err != nil {
			t.Fatalf("fulfillment not found after cross-repo commit: %v", err)
		}
		d, err := tx2.Deployments().Get(ctx, "d1")
		if err != nil {
			t.Fatalf("deployment not found after cross-repo commit: %v", err)
		}
		if d.FulfillmentID != "f-cross" {
			t.Fatalf("deployment.FulfillmentID = %q, want f-cross", d.FulfillmentID)
		}
	})

	t.Run("FulfillmentsAccessorPersists", func(t *testing.T) {
		store := factory(t)
		ctx := context.Background()
		fixed := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx.Rollback()
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment("f-acc", fixed)); err != nil {
			t.Fatalf("Fulfillments().Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		tx2, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx2.Rollback()

		got, err := tx2.Fulfillments().Get(ctx, "f-acc")
		if err != nil {
			t.Fatalf("Get fulfillment after commit: %v", err)
		}
		if got.ID != "f-acc" || got.State != domain.FulfillmentStateCreating {
			t.Fatalf("loaded fulfillment = %+v, want ID f-acc and creating state", got)
		}
	})
}
