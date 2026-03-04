// Package storetest provides contract tests for [domain.Store]
// implementations.
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Store] for each test invocation.
type Factory func(t *testing.T) domain.Store

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

		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		defer tx.Rollback()

		if err := tx.Targets().Create(ctx, domain.TargetInfo{ID: "t1", Name: "cluster-a"}); err != nil {
			t.Fatalf("Create target: %v", err)
		}
		if err := tx.Deployments().Create(ctx, domain.Deployment{ID: "d1", State: domain.DeploymentStateCreating}); err != nil {
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
		if _, err := tx2.Deployments().Get(ctx, "d1"); err != nil {
			t.Fatalf("deployment not found after cross-repo commit: %v", err)
		}
	})
}
