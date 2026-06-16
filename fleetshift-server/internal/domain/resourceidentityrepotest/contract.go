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
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("CreatePlatformResource: %v", err)
		}

		got, err := repo.GetPlatformResourceByUID(ctx, "uid-1")
		if err != nil {
			t.Fatalf("GetByUID: %v", err)
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
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetPlatformResourceByName(ctx, "clusters/staging")
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
		if err := repo.CreatePlatformResource(ctx, r1); err != nil {
			t.Fatalf("Create first: %v", err)
		}

		r2 := domain.NewPlatformResource("uid-b", "clusters", "clusters/dup", nil, now)
		err := repo.CreatePlatformResource(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("ListByCollection", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreatePlatformResource(ctx, domain.NewPlatformResource("uid-l1", "clusters", "clusters/a", nil, now)); err != nil {
			t.Fatalf("Create a: %v", err)
		}
		if err := repo.CreatePlatformResource(ctx, domain.NewPlatformResource("uid-l2", "clusters", "clusters/b", nil, now)); err != nil {
			t.Fatalf("Create b: %v", err)
		}
		if err := repo.CreatePlatformResource(ctx, domain.NewPlatformResource("uid-l3", "nodes", "nodes/n1", nil, now)); err != nil {
			t.Fatalf("Create n1: %v", err)
		}

		got, err := repo.ListPlatformResourcesByCollection(ctx, "clusters")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		// Stable ordering by relative_name.
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
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		later := now.Add(time.Hour)
		r.SetLabels(map[string]string{"b": "2"}, later)
		if err := repo.UpdatePlatformResourceLabels(ctx, r); err != nil {
			t.Fatalf("UpdateLabels: %v", err)
		}

		got, err := repo.GetPlatformResourceByUID(ctx, "uid-u1")
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

	t.Run("PutRepresentation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-r1", "clusters", "clusters/multi", nil, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		kindRep := domain.ResourceRepresentation{
			PlatformUID:  "uid-r1",
			ServiceName:  "kind.fleetshift.io",
			Version:      "v1alpha1",
			CollectionID: "clusters",
			RelativeName: "clusters/multi",
			Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
			Labels:       map[string]string{"runtime": "containerd"},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := repo.PutRepresentation(ctx, kindRep); err != nil {
			t.Fatalf("Put kind: %v", err)
		}

		gcpRep := domain.ResourceRepresentation{
			PlatformUID:  "uid-r1",
			ServiceName:  "gcp.fleetshift.io",
			Version:      "v1",
			CollectionID: "clusters",
			RelativeName: "clusters/multi",
			Roles:        []domain.RepresentationRole{domain.RepresentationRoleInventory},
			Labels:       map[string]string{"project": "my-proj"},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := repo.PutRepresentation(ctx, gcpRep); err != nil {
			t.Fatalf("Put gcp: %v", err)
		}

		reps, err := repo.ListRepresentationsByPlatformUID(ctx, "uid-r1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(reps) != 2 {
			t.Fatalf("len = %d, want 2", len(reps))
		}
	})

	t.Run("PutRepresentationUpdatesSameIdentity", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-pu1", "clusters", "clusters/update-rep", nil, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		rep := domain.ResourceRepresentation{
			PlatformUID:  "uid-pu1",
			ServiceName:  "kind.fleetshift.io",
			Version:      "v1alpha1",
			CollectionID: "clusters",
			RelativeName: "clusters/update-rep",
			Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
			Labels:       map[string]string{"v": "1"},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := repo.PutRepresentation(ctx, rep); err != nil {
			t.Fatalf("Put first: %v", err)
		}

		later := now.Add(time.Hour)
		rep.Version = "v1beta1"
		rep.Labels = map[string]string{"v": "2"}
		rep.Roles = []domain.RepresentationRole{domain.RepresentationRoleManaged, domain.RepresentationRoleTarget}
		rep.UpdatedAt = later
		if err := repo.PutRepresentation(ctx, rep); err != nil {
			t.Fatalf("Put second: %v", err)
		}

		got, err := repo.GetRepresentation(ctx, "kind.fleetshift.io", "clusters", "clusters/update-rep")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Version != "v1beta1" {
			t.Errorf("Version = %q, want v1beta1", got.Version)
		}
		if got.Labels["v"] != "2" {
			t.Errorf("Labels[v] = %q, want 2", got.Labels["v"])
		}
		if len(got.Roles) != 2 {
			t.Errorf("Roles len = %d, want 2", len(got.Roles))
		}

		// Should still be one representation, not two.
		reps, err := repo.ListRepresentationsByPlatformUID(ctx, "uid-pu1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(reps) != 1 {
			t.Fatalf("len = %d, want 1 (update-in-place)", len(reps))
		}
	})

	t.Run("PutRepresentationConflictingPlatformUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-c1", "clusters", "clusters/c1", nil, now)
		r2 := domain.NewPlatformResource("uid-c2", "clusters", "clusters/c2", nil, now)
		if err := repo.CreatePlatformResource(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}
		if err := repo.CreatePlatformResource(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		rep := domain.ResourceRepresentation{
			PlatformUID:  "uid-c1",
			ServiceName:  "kind.fleetshift.io",
			Version:      "v1",
			CollectionID: "clusters",
			RelativeName: "clusters/shared-name",
			Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := repo.PutRepresentation(ctx, rep); err != nil {
			t.Fatalf("Put first: %v", err)
		}

		// Same service+collection+relative_name but different platform UID.
		rep.PlatformUID = "uid-c2"
		err := repo.PutRepresentation(ctx, rep)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("TombstoneRepresentation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-ts1", "clusters", "clusters/tomb", nil, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		rep := domain.ResourceRepresentation{
			PlatformUID:  "uid-ts1",
			ServiceName:  "kind.fleetshift.io",
			Version:      "v1",
			CollectionID: "clusters",
			RelativeName: "clusters/tomb",
			Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := repo.PutRepresentation(ctx, rep); err != nil {
			t.Fatalf("Put: %v", err)
		}

		later := now.Add(time.Hour)
		if err := repo.TombstoneRepresentation(ctx, "kind.fleetshift.io", "clusters", "clusters/tomb", later); err != nil {
			t.Fatalf("Tombstone: %v", err)
		}

		// Active list should exclude tombstoned representations.
		reps, err := repo.ListRepresentationsByPlatformUID(ctx, "uid-ts1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(reps) != 0 {
			t.Errorf("active reps len = %d, want 0 after tombstone", len(reps))
		}

		// Direct get should still return it (with deleted_at set).
		got, err := repo.GetRepresentation(ctx, "kind.fleetshift.io", "clusters", "clusters/tomb")
		if err != nil {
			t.Fatalf("GetRepresentation: %v", err)
		}
		if got.DeletedAt == nil {
			t.Fatal("DeletedAt is nil, want non-nil after tombstone")
		}
	})

	t.Run("PutAndResolveAlias", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-a1", "clusters", "clusters/aliased", nil, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		alias := domain.Alias{Namespace: "gcp", Key: "project_id", Value: "my-proj-123"}
		if err := repo.PutAlias(ctx, "uid-a1", alias, now); err != nil {
			t.Fatalf("PutAlias: %v", err)
		}

		uid, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if uid != "uid-a1" {
			t.Errorf("resolved UID = %q, want uid-a1", uid)
		}

		aliases, err := repo.ListAliasesByPlatformUID(ctx, "uid-a1")
		if err != nil {
			t.Fatalf("ListAliases: %v", err)
		}
		if len(aliases) != 1 {
			t.Fatalf("aliases len = %d, want 1", len(aliases))
		}
	})

	t.Run("PutAliasIdempotentForSameUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r := domain.NewPlatformResource("uid-ai1", "clusters", "clusters/alias-idem", nil, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		alias := domain.Alias{Namespace: "gcp", Key: "project_id", Value: "proj-1"}
		if err := repo.PutAlias(ctx, "uid-ai1", alias, now); err != nil {
			t.Fatalf("PutAlias first: %v", err)
		}
		if err := repo.PutAlias(ctx, "uid-ai1", alias, now); err != nil {
			t.Fatalf("PutAlias second (idempotent): %v", err)
		}
	})

	t.Run("PutAliasConflictsForDifferentUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-ac1", "clusters", "clusters/ac1", nil, now)
		r2 := domain.NewPlatformResource("uid-ac2", "clusters", "clusters/ac2", nil, now)
		if err := repo.CreatePlatformResource(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}
		if err := repo.CreatePlatformResource(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		alias := domain.Alias{Namespace: "gcp", Key: "project_id", Value: "contested"}
		if err := repo.PutAlias(ctx, "uid-ac1", alias, now); err != nil {
			t.Fatalf("PutAlias first: %v", err)
		}

		err := repo.PutAlias(ctx, "uid-ac2", alias, now)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("PutRelationship", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		r1 := domain.NewPlatformResource("uid-rel1", "clusters", "clusters/rel1", nil, now)
		r2 := domain.NewPlatformResource("uid-rel2", "nodes", "nodes/rel2", nil, now)
		if err := repo.CreatePlatformResource(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}
		if err := repo.CreatePlatformResource(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		rel := domain.ResourceRelationship{
			SourceUID:     "uid-rel1",
			Type:          "runs-on",
			TargetUID:     "uid-rel2",
			SourceService: "kind.fleetshift.io",
			CreatedAt:     now,
		}
		if err := repo.PutRelationship(ctx, rel); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}

		rels, err := repo.ListRelationshipsBySourceUID(ctx, "uid-rel1")
		if err != nil {
			t.Fatalf("ListRelationships: %v", err)
		}
		if len(rels) != 1 {
			t.Fatalf("rels len = %d, want 1", len(rels))
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

		_, err := repo.GetPlatformResourceByUID(ctx, "missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetByUID: got %v, want ErrNotFound", err)
		}

		_, err = repo.GetPlatformResourceByName(ctx, "clusters/missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetByName: got %v, want ErrNotFound", err)
		}

		_, err = repo.GetRepresentation(ctx, "missing.svc", "clusters", "clusters/missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetRepresentation: got %v, want ErrNotFound", err)
		}

		_, err = repo.ResolveAlias(ctx, domain.Alias{Namespace: "x", Key: "k", Value: "v"})
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("ResolveAlias: got %v, want ErrNotFound", err)
		}
	})
}
