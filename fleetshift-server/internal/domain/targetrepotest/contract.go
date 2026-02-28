// Package targetrepotest provides contract tests for [domain.TargetRepository]
// implementations.
package targetrepotest

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.TargetRepository] for each test invocation.
type Factory func(t *testing.T) domain.TargetRepository

// Run exercises the [domain.TargetRepository] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("CreateAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		target := domain.TargetInfo{
			ID:         "t1",
			Name:       "cluster-a",
			Labels:     map[string]string{"env": "prod"},
			Properties: map[string]string{"region": "us-east"},
		}

		if err := repo.Create(ctx, target); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "t1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "cluster-a" {
			t.Errorf("Name = %q, want %q", got.Name, "cluster-a")
		}
		if got.Labels["env"] != "prod" {
			t.Errorf("Labels[env] = %q, want %q", got.Labels["env"], "prod")
		}
		if got.Properties["region"] != "us-east" {
			t.Errorf("Properties[region] = %q, want %q", got.Properties["region"], "us-east")
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		target := domain.TargetInfo{ID: "t1", Name: "cluster-a"}

		if err := repo.Create(ctx, target); err != nil {
			t.Fatalf("first Create: %v", err)
		}
		err := repo.Create(ctx, target)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second Create: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		targets := []domain.TargetInfo{
			{ID: "t1", Name: "a"},
			{ID: "t2", Name: "b"},
		}
		for _, tgt := range targets {
			if err := repo.Create(ctx, tgt); err != nil {
				t.Fatalf("Create %s: %v", tgt.ID, err)
			}
		}

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("List: got %d, want 2", len(got))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.Create(ctx, domain.TargetInfo{ID: "t1", Name: "a"}); err != nil {
			t.Fatal(err)
		}
		if err := repo.Delete(ctx, "t1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "t1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Delete(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Delete: got %v, want ErrNotFound", err)
		}
	})
}
