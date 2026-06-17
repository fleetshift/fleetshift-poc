// Package resourceidentityrepotest provides contract tests for
// [domain.ResourceIdentityRepository] implementations.
package resourceidentityrepotest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.ResourceIdentityRepository] for each
// test.
type Factory func(t *testing.T) domain.ResourceIdentityRepository

// Run exercises the [domain.ResourceIdentityRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("CreateAndGetByUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-1", "clusters", "clusters/prod", map[string]string{"env": "prod"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "uid-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.UID() != "uid-1" {
			t.Errorf("UID = %q, want uid-1", got.UID())
		}
		if got.CollectionID() != "clusters" {
			t.Errorf("CollectionID = %q, want clusters", got.CollectionID())
		}
		if got.RelativeName() != "clusters/prod" {
			t.Errorf("RelativeName = %q, want clusters/prod", got.RelativeName())
		}
		if got.Labels()["env"] != "prod" {
			t.Errorf("Labels[env] = %q, want prod", got.Labels()["env"])
		}
		if !got.CreatedAt().Equal(now) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt(), now)
		}
	})

	t.Run("GetByRelativeName", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-2", "clusters", "clusters/staging", nil, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, "clusters/staging")
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if got.UID() != "uid-2" {
			t.Errorf("UID = %q, want uid-2", got.UID())
		}
	})

	t.Run("DuplicateRelativeName", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-a", "clusters", "clusters/dup", nil, now)
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create first: %v", err)
		}

		r2 := domain.NewPlatformResource("uid-b", "clusters", "clusters/dup", nil, now)
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("ListByCollection", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.Create(ctx, domain.NewPlatformResource("uid-l1", "clusters", "clusters/a", nil, now)); err != nil {
			t.Fatalf("Create a: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource("uid-l2", "clusters", "clusters/b", nil, now)); err != nil {
			t.Fatalf("Create b: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource("uid-l3", "nodes", "nodes/n1", nil, now)); err != nil {
			t.Fatalf("Create n1: %v", err)
		}

		got, err := repo.ListByCollection(ctx, "clusters")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0].RelativeName() != "clusters/a" {
			t.Errorf("got[0].RelativeName = %q, want clusters/a", got[0].RelativeName())
		}
		if got[1].RelativeName() != "clusters/b" {
			t.Errorf("got[1].RelativeName = %q, want clusters/b", got[1].RelativeName())
		}
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-u1", "clusters", "clusters/labelled", map[string]string{"a": "1"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		later := now.Add(time.Hour)
		r.SetLabels(map[string]string{"b": "2"}, later)
		if err := repo.Update(ctx, r); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "uid-u1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Labels()["b"] != "2" {
			t.Errorf("Labels[b] = %q, want 2", got.Labels()["b"])
		}
		if _, ok := got.Labels()["a"]; ok {
			t.Error("Labels[a] should be gone after update")
		}
		if !got.CreatedAt().Equal(now) {
			t.Errorf("CreatedAt changed: got %v, want %v", got.CreatedAt(), now)
		}
		if !got.UpdatedAt().Equal(later) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt(), later)
		}
	})

	t.Run("CreateWithRepresentations", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-r1", "clusters", "clusters/multi", nil, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1alpha1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
			Labels:      map[string]string{"runtime": "containerd"},
		}, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "gcp.fleetshift.io",
			Version:     "v1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleInventory},
			Labels:      map[string]string{"project": "my-proj"},
		}, now)

		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "uid-r1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Representations()) != 2 {
			t.Fatalf("representations len = %d, want 2", len(got.Representations()))
		}
	})

	t.Run("UpdateRepresentation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-pu1", "clusters", "clusters/update-rep", nil, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1alpha1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
			Labels:      map[string]string{"v": "1"},
		}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, "uid-pu1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		later := now.Add(time.Hour)
		_ = loaded.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1beta1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged, domain.RepresentationRoleTarget},
			Labels:      map[string]string{"v": "2"},
		}, later)
		if err := repo.Update(ctx, loaded); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "uid-pu1")
		if err != nil {
			t.Fatalf("Get after update: %v", err)
		}
		reps := got.Representations()
		if len(reps) != 1 {
			t.Fatalf("representations len = %d, want 1", len(reps))
		}
		if reps[0].Version != "v1beta1" {
			t.Errorf("Version = %q, want v1beta1", reps[0].Version)
		}
		if reps[0].Labels["v"] != "2" {
			t.Errorf("Labels[v] = %q, want 2", reps[0].Labels["v"])
		}
	})

	t.Run("TombstoneRepresentation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-ts1", "clusters", "clusters/tomb", nil, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
		}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, "uid-ts1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		later := now.Add(time.Hour)
		if err := loaded.TombstoneRepresentation("kind.fleetshift.io", later); err != nil {
			t.Fatalf("Tombstone: %v", err)
		}
		if err := repo.Update(ctx, loaded); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "uid-ts1")
		if err != nil {
			t.Fatalf("Get after tombstone: %v", err)
		}
		if len(got.Representations()) != 0 {
			t.Errorf("active representations len = %d, want 0", len(got.Representations()))
		}

		// Direct GetRepresentation should still return it (with DeletedAt set).
		rep, err := repo.GetRepresentation(ctx, "//kind.fleetshift.io/clusters/tomb")
		if err != nil {
			t.Fatalf("GetRepresentation: %v", err)
		}
		if rep.DeletedAt == nil {
			t.Fatal("DeletedAt is nil, want non-nil after tombstone")
		}
	})

	t.Run("CreateWithAliases", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-a1", "clusters", "clusters/aliased", nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "my-proj-123")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}

		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		uid, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("ResolveAlias: %v", err)
		}
		if uid != "uid-a1" {
			t.Errorf("resolved UID = %q, want uid-a1", uid)
		}

		got, err := repo.Get(ctx, "uid-a1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Aliases()) != 1 {
			t.Fatalf("aliases len = %d, want 1", len(got.Aliases()))
		}
	})

	t.Run("AliasIdempotentForSameUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-ai1", "clusters", "clusters/alias-idem", nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "proj-1")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, "uid-ai1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if err := loaded.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias (idempotent): %v", err)
		}
		if err := repo.Update(ctx, loaded); err != nil {
			t.Fatalf("Update (idempotent alias): %v", err)
		}
	})

	t.Run("AliasConflictsForDifferentUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-ac1", "clusters", "clusters/ac1", nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "contested")
		if err := r1.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias r1: %v", err)
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		r2 := domain.NewPlatformResource("uid-ac2", "clusters", "clusters/ac2", nil, now)
		if err := r2.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias r2: %v", err)
		}
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("CreateWithRelationships", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-rel1", "clusters", "clusters/rel1", nil, now)
		r2 := domain.NewPlatformResource("uid-rel2", "nodes", "nodes/rel2", nil, now)
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		_ = r1.AddRelationship(domain.ResourceRelationship{
			SourceUID:     "uid-rel1",
			Type:          "runs-on",
			TargetUID:     "uid-rel2",
			SourceService: "kind.fleetshift.io",
			CreatedAt:     now,
		})
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		got, err := repo.Get(ctx, "uid-rel1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		rels := got.Relationships()
		if len(rels) != 1 {
			t.Fatalf("relationships len = %d, want 1", len(rels))
		}
		if rels[0].Type != "runs-on" {
			t.Errorf("Type = %q, want runs-on", rels[0].Type)
		}
		if rels[0].TargetUID != "uid-rel2" {
			t.Errorf("TargetUID = %q, want uid-rel2", rels[0].TargetUID)
		}
	})

	t.Run("GetNotFoundCases", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_, err := repo.Get(ctx, "missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("Get: got %v, want ErrNotFound", err)
		}

		_, err = repo.GetByName(ctx, "clusters/missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetByName: got %v, want ErrNotFound", err)
		}

		_, err = repo.GetRepresentation(ctx, "//missing.svc/clusters/missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetRepresentation: got %v, want ErrNotFound", err)
		}

		_, err = repo.ResolveAlias(ctx, domain.Alias{Namespace: "x", Key: "k", Value: "v"})
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("ResolveAlias: got %v, want ErrNotFound", err)
		}
	})
}
