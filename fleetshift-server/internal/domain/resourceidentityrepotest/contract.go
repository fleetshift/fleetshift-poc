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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/prod"), map[string]string{"env": "prod"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, uid)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.UID() != uid {
			t.Errorf("UID = %q, want %q", got.UID(), uid)
		}
		if got.Collection() != domain.CollectionName("clusters") {
			t.Errorf("Collection = %q, want clusters", got.Collection())
		}
		if got.Name() != domain.ResourceName("clusters/prod") {
			t.Errorf("Name = %q, want clusters/prod", got.Name())
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/staging"), nil, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, domain.ResourceName("clusters/staging"))
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if got.UID() != uid {
			t.Errorf("UID = %q, want %q", got.UID(), uid)
		}
	})

	t.Run("DuplicateRelativeName", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		uid1 := domain.NewPlatformResourceUID()
		r1 := domain.NewPlatformResource(uid1, domain.ResourceName("clusters/dup"), nil, now)
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create first: %v", err)
		}

		uid2 := domain.NewPlatformResourceUID()
		r2 := domain.NewPlatformResource(uid2, domain.ResourceName("clusters/dup"), nil, now)
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("ListByCollection", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		uidL1 := domain.NewPlatformResourceUID()
		uidL2 := domain.NewPlatformResourceUID()
		uidL3 := domain.NewPlatformResourceUID()

		if err := repo.Create(ctx, domain.NewPlatformResource(uidL1, domain.ResourceName("clusters/a"), nil, now)); err != nil {
			t.Fatalf("Create a: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource(uidL2, domain.ResourceName("clusters/b"), nil, now)); err != nil {
			t.Fatalf("Create b: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource(uidL3, domain.ResourceName("nodes/n1"), nil, now)); err != nil {
			t.Fatalf("Create n1: %v", err)
		}

		got, err := repo.ListByCollection(ctx, domain.CollectionName("clusters"))
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0].Name() != domain.ResourceName("clusters/a") {
			t.Errorf("got[0].Name = %q, want clusters/a", got[0].Name())
		}
		if got[1].Name() != domain.ResourceName("clusters/b") {
			t.Errorf("got[1].Name = %q, want clusters/b", got[1].Name())
		}
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/labelled"), map[string]string{"a": "1"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		later := now.Add(time.Hour)
		r.SetLabels(map[string]string{"b": "2"}, later)
		if err := repo.Update(ctx, r); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, uid)
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/multi"), nil, now)
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

		got, err := repo.Get(ctx, uid)
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/update-rep"), nil, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1alpha1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
			Labels:      map[string]string{"v": "1"},
		}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, uid)
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

		got, err := repo.Get(ctx, uid)
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/tomb"), nil, now)
		_ = r.AttachRepresentation(domain.AttachRepresentationInput{
			ServiceName: "kind.fleetshift.io",
			Version:     "v1",
			Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
		}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, uid)
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

		got, err := repo.Get(ctx, uid)
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/aliased"), nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "my-proj-123")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}

		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		resolvedUID, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("ResolveAlias: %v", err)
		}
		if resolvedUID != uid {
			t.Errorf("resolved UID = %q, want %q", resolvedUID, uid)
		}

		got, err := repo.Get(ctx, uid)
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

		uid := domain.NewPlatformResourceUID()
		r := domain.NewPlatformResource(uid, domain.ResourceName("clusters/alias-idem"), nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "proj-1")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.Get(ctx, uid)
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

		uid1 := domain.NewPlatformResourceUID()
		r1 := domain.NewPlatformResource(uid1, domain.ResourceName("clusters/ac1"), nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "contested")
		if err := r1.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias r1: %v", err)
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		uid2 := domain.NewPlatformResourceUID()
		r2 := domain.NewPlatformResource(uid2, domain.ResourceName("clusters/ac2"), nil, now)
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

		uid1 := domain.NewPlatformResourceUID()
		uid2 := domain.NewPlatformResourceUID()

		r2 := domain.NewPlatformResource(uid2, domain.ResourceName("nodes/rel2"), nil, now)
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		r1 := domain.NewPlatformResource(uid1, domain.ResourceName("clusters/rel1"), nil, now)
		_ = r1.AddRelationship(domain.ResourceRelationship{
			SourceUID:     uid1,
			Type:          "runs-on",
			TargetUID:     uid2,
			SourceService: "kind.fleetshift.io",
			CreatedAt:     now,
		})
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		got, err := repo.Get(ctx, uid1)
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
		if rels[0].TargetUID != uid2 {
			t.Errorf("TargetUID = %q, want %q", rels[0].TargetUID, uid2)
		}
	})

	t.Run("GetNotFoundCases", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		missingUID := domain.NewPlatformResourceUID()
		_, err := repo.Get(ctx, missingUID)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("Get: got %v, want ErrNotFound", err)
		}

		_, err = repo.GetByName(ctx, domain.ResourceName("clusters/missing"))
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
