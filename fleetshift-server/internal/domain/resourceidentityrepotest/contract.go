// Package resourceidentityrepotest provides contract tests for
// [domain.ResourceIdentityRepository] implementations.
package resourceidentityrepotest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Tx] for each test. The Tx is needed
// because representations are derived by joining extension resources
// (via [domain.Tx.ExtensionResources]) to platform resources on name.
type Factory func(t *testing.T) domain.Tx

// seedExtensionResourceType registers a fresh extension resource type
// under the given service, so that instances created against it (see
// seedExtensionResourceInstance) produce representations owned by that
// service. Each call registers a distinct type name to avoid colliding
// with other seeded types in the same test.
func seedExtensionResourceType(t *testing.T, tx domain.Tx, service domain.ServiceName, version domain.APIVersion, now time.Time) domain.ResourceType {
	t.Helper()
	suffix := domain.NewExtensionResourceUID().String()[:8]
	rt := domain.ResourceType(fmt.Sprintf("%s/Seed%s", service, suffix))
	typeDef := domain.NewExtensionResourceType(rt, version, "seeds", now)
	if err := tx.ExtensionResources().CreateType(context.Background(), typeDef); err != nil {
		t.Fatalf("seed extension resource type: %v", err)
	}
	return rt
}

// seedExtensionResourceInstance creates an extension resource of type
// rt at name. Representations are derived on read by joining on
// (collection_name, resource_id), so an instance seeded with the same
// name as a platform resource becomes that resource's representation
// for rt's service.
func seedExtensionResourceInstance(t *testing.T, tx domain.Tx, rt domain.ResourceType, name domain.ResourceName, now time.Time) domain.ExtensionResourceUID {
	t.Helper()
	uid := domain.NewExtensionResourceUID()
	r := domain.NewExtensionResource(uid, rt, name, now)
	if err := tx.ExtensionResources().Create(context.Background(), r); err != nil {
		t.Fatalf("seed extension resource instance: %v", err)
	}
	return uid
}

// seedExtensionContributedAlias registers a fresh inventory-capable
// extension resource type and reports alias for name via
// ReplaceInventory, giving name an extension-contributed
// resource_alias_claims row (platform_owned = false, one contributor)
// before any [domain.ResourceIdentityRepository] call touches it --
// reconcileAliases's corroborate/un-corroborate cases need one of
// these to already exist to be meaningfully distinct from the
// plain-insert case CreateWithAliases already covers.
func seedExtensionContributedAlias(t *testing.T, tx domain.Tx, name domain.ResourceName, alias domain.Alias, now time.Time) {
	t.Helper()
	suffix := domain.NewExtensionResourceUID().String()[:8]
	rt := domain.ResourceType(fmt.Sprintf("inv.fleetshift.io/Seed%s", suffix))
	typeDef := domain.NewExtensionResourceType(rt, "v1", "seeds", now, domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(context.Background(), typeDef); err != nil {
		t.Fatalf("seed inventory-capable type: %v", err)
	}
	if _, err := tx.ExtensionResources().ReplaceInventory(context.Background(), []domain.InventoryReplacement{{
		ResourceType: rt,
		Name:         name,
		CandidateUID: domain.NewExtensionResourceUID(),
		Aliases:      []domain.Alias{alias},
		ObservedAt:   now,
		ReceivedAt:   now,
	}}); err != nil {
		t.Fatalf("seed extension-contributed alias: %v", err)
	}
}

// Run exercises the [domain.ResourceIdentityRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("CreateAndGetByName", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/prod")
		r := domain.NewPlatformResource(name, map[string]string{"env": "prod"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if got.Collection() != domain.CollectionName("clusters") {
			t.Errorf("Collection = %q, want clusters", got.Collection())
		}
		if got.Name() != name {
			t.Errorf("Name = %q, want %q", got.Name(), name)
		}
		if got.Labels()["env"] != "prod" {
			t.Errorf("Labels[env] = %q, want prod", got.Labels()["env"])
		}
		if !got.CreatedAt().Equal(now) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt(), now)
		}
	})

	t.Run("DuplicateRelativeName", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/dup")
		if err := repo.Create(ctx, domain.NewPlatformResource(name, nil, now)); err != nil {
			t.Fatalf("Create first: %v", err)
		}

		err := repo.Create(ctx, domain.NewPlatformResource(name, nil, now))
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("ListByCollection", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("clusters/a"), nil, now)); err != nil {
			t.Fatalf("Create a: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("clusters/b"), nil, now)); err != nil {
			t.Fatalf("Create b: %v", err)
		}
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("nodes/n1"), nil, now)); err != nil {
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
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/labelled")
		r := domain.NewPlatformResource(name, map[string]string{"a": "1"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		later := now.Add(time.Hour)
		r.SetLabels(map[string]string{"b": "2"}, later)
		if err := repo.Update(ctx, r); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
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
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/multi")
		rt1 := seedExtensionResourceType(t, tx, "kind.fleetshift.io", "v1alpha1", now)
		rt2 := seedExtensionResourceType(t, tx, "gcp.fleetshift.io", "v1", now)
		seedExtensionResourceInstance(t, tx, rt1, name, now)
		seedExtensionResourceInstance(t, tx, rt2, name, now)

		r := domain.NewPlatformResource(name, nil, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if len(got.Representations()) != 2 {
			t.Fatalf("representations len = %d, want 2", len(got.Representations()))
		}
	})

	t.Run("RepresentationDisappearsOnExtensionResourceDelete", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/tomb")
		rt := seedExtensionResourceType(t, tx, "kind.fleetshift.io", "v1", now)
		seedExtensionResourceInstance(t, tx, rt, name, now)

		r := domain.NewPlatformResource(name, nil, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if len(got.Representations()) != 1 {
			t.Fatalf("representations len = %d, want 1", len(got.Representations()))
		}

		// The representation disappears once the backing extension
		// resource is deleted -- there's no separate detach step.
		if err := tx.ExtensionResources().Delete(ctx, rt.FullName(name)); err != nil {
			t.Fatalf("Delete extension resource: %v", err)
		}

		got, err = repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName after delete: %v", err)
		}
		if len(got.Representations()) != 0 {
			t.Errorf("representations len = %d, want 0", len(got.Representations()))
		}

		_, err = repo.GetRepresentation(ctx, rt.FullName(name))
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetRepresentation after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("RepresentationExtensionResourceUID_RoundTrips", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/er-link")
		rt := seedExtensionResourceType(t, tx, "kind.fleetshift.io", "v1", now)
		erUID := seedExtensionResourceInstance(t, tx, rt, name, now)

		r := domain.NewPlatformResource(name, nil, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		reps := got.Representations()
		if len(reps) != 1 {
			t.Fatalf("representations len = %d, want 1", len(reps))
		}
		if reps[0].ExtensionResourceUID() != erUID {
			t.Errorf("ExtensionResourceUID = %s, want %s", reps[0].ExtensionResourceUID(), erUID)
		}

		// GetRepresentation should also return the UID.
		rep, err := repo.GetRepresentation(ctx, rt.FullName(name))
		if err != nil {
			t.Fatalf("GetRepresentation: %v", err)
		}
		if rep.ExtensionResourceUID() != erUID {
			t.Errorf("GetRepresentation ExtensionResourceUID = %s, want %s", rep.ExtensionResourceUID(), erUID)
		}
	})

	t.Run("VirtualPlatformResource", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		// No repo.Create call at all -- the name only ever gets an
		// extension resource, never its own platform_resources row.
		name := domain.ResourceName("clusters/virtual")
		rt := seedExtensionResourceType(t, tx, "kind.fleetshift.io", "v1", now)
		erUID := seedExtensionResourceInstance(t, tx, rt, name, now)

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName (virtual): %v", err)
		}
		if got.Name() != name {
			t.Errorf("Name = %q, want %q", got.Name(), name)
		}
		if len(got.Labels()) != 0 {
			t.Errorf("virtual resource Labels = %+v, want empty", got.Labels())
		}
		if len(got.Aliases()) != 0 {
			t.Errorf("virtual resource Aliases = %+v, want empty", got.Aliases())
		}
		reps := got.Representations()
		if len(reps) != 1 {
			t.Fatalf("representations len = %d, want 1", len(reps))
		}
		if reps[0].ExtensionResourceUID() != erUID {
			t.Errorf("ExtensionResourceUID = %s, want %s", reps[0].ExtensionResourceUID(), erUID)
		}

		// A virtual resource under a collection surfaces from
		// ListByCollection exactly like a physical one would.
		list, err := repo.ListByCollection(ctx, domain.CollectionName("clusters"))
		if err != nil {
			t.Fatalf("ListByCollection (virtual): %v", err)
		}
		found := false
		for _, pr := range list {
			if pr.Name() == name {
				found = true
			}
		}
		if !found {
			t.Errorf("ListByCollection did not include virtual resource %q", name)
		}

		// A name with no representations, aliases, relationships, or
		// physical row at all truly doesn't exist.
		_, err = repo.GetByName(ctx, domain.ResourceName("clusters/never-existed"))
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetByName (nonexistent): got %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateWithAliases", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/aliased")
		r := domain.NewPlatformResource(name, nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "my-proj-123")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}

		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		resolvedName, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("ResolveAlias: %v", err)
		}
		if resolvedName != name {
			t.Errorf("resolved name = %q, want %q", resolvedName, name)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if len(got.Aliases()) != 1 {
			t.Fatalf("aliases len = %d, want 1", len(got.Aliases()))
		}
	})

	t.Run("AliasIdempotentForSameResource", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/alias-idem")
		r := domain.NewPlatformResource(name, nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "proj-1")
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		loaded, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if err := loaded.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias (idempotent): %v", err)
		}
		if err := repo.Update(ctx, loaded); err != nil {
			t.Fatalf("Update (idempotent alias): %v", err)
		}
	})

	t.Run("AliasConflictsForDifferentResource", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name1 := domain.ResourceName("clusters/ac1")
		r1 := domain.NewPlatformResource(name1, nil, now)
		alias, _ := domain.NewAlias("gcp", "project_id", "contested")
		if err := r1.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias r1: %v", err)
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		name2 := domain.ResourceName("clusters/ac2")
		r2 := domain.NewPlatformResource(name2, nil, now)
		if err := r2.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias r2: %v", err)
		}
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	// The next three tests pin down reconcileAliases's three-case
	// lifecycle for platform_owned (see its own doc comment in
	// resource_identity_repo.go): corroborating an
	// extension-contributed claim without duplicating it,
	// un-corroborating one whose platform declaration is withdrawn
	// but whose contributor(s) remain (the claim survives, merely
	// un-owned), and deleting one that's both un-corroborated and
	// left with zero contributors. Case 1 is also covered end-to-end
	// through an extension resource Delete by
	// AliasSurvivesExtensionResourceDeleteWhenPlatformOwned in
	// extensionresourcerepotest/contract.go; these three instead
	// isolate ResourceIdentityRepository's own Create/Update
	// contract, independent of ExtensionResourceRepo.Delete.

	t.Run("ReconcileAliasesCorroboratesExistingExtensionContributedClaim", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/corroborate")
		alias, _ := domain.NewAlias("gcp", "project_id", "corroborate-proj")
		seedExtensionContributedAlias(t, tx, name, alias, now)

		r := domain.NewPlatformResource(name, nil, now)
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		// Must not be rejected as ErrAlreadyExists: the claim already
		// targets this exact resource (just from the extension
		// resource's own contribution), and corroborating it in
		// place is exactly what reconcileAliases's first branch is
		// for -- ErrAlreadyExists is reserved for a claim that
		// targets a genuinely different resource.
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create (corroborate): %v", err)
		}

		resolved, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("ResolveAlias: %v", err)
		}
		if resolved != name {
			t.Errorf("resolved name = %q, want %q", resolved, name)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if len(got.Aliases()) != 1 || got.Aliases()[0] != alias {
			t.Fatalf("Aliases() = %+v, want exactly [%+v] (corroborated in place, not duplicated)", got.Aliases(), alias)
		}

		// Re-asserting the same alias on a later Update must stay a
		// no-op (platform_owned already true), not attempt a second
		// corroborate.
		if err := got.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias (idempotent re-assert): %v", err)
		}
		if err := repo.Update(ctx, got); err != nil {
			t.Fatalf("Update (idempotent corroborate): %v", err)
		}
	})

	t.Run("ReconcileAliasesUnCorroboratesButKeepsClaimAliveWhileContributorRemains", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/un-corroborate-survives")
		alias, _ := domain.NewAlias("gcp", "project_id", "un-corroborate-survives-proj")
		seedExtensionContributedAlias(t, tx, name, alias, now)

		r := domain.NewPlatformResource(name, nil, now)
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		// Withdraw the platform's own declaration of the alias.
		// PlatformResource.AddAlias is monotonic/additive (see
		// resource_identity.go), so there's no domain method to
		// shrink the aggregate's alias set directly -- build the
		// withdrawn snapshot by hand instead, the same way a
		// repository read would reconstitute one, per
		// reconcileAliases's own doc comment on why this matters
		// even without a public API exercising it yet.
		withdrawn := domain.PlatformResourceFromSnapshot(domain.PlatformResourceSnapshot{
			Name:      name,
			Labels:    map[string]string{},
			CreatedAt: now,
			UpdatedAt: now.Add(time.Minute),
		})
		if err := repo.Update(ctx, withdrawn); err != nil {
			t.Fatalf("Update (withdraw): %v", err)
		}

		// The extension resource's own contribution is untouched by
		// the platform's withdrawal -- the claim is un-owned, not
		// deleted, and must remain fully resolvable.
		resolved, err := repo.ResolveAlias(ctx, alias)
		if err != nil {
			t.Fatalf("ResolveAlias after un-corroborate: %v", err)
		}
		if resolved != name {
			t.Errorf("resolved name = %q, want %q", resolved, name)
		}
	})

	t.Run("ReconcileAliasesDeletesClaimOnceUnCorroboratedAndOrphaned", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("clusters/un-corroborate-orphaned")
		alias, _ := domain.NewAlias("gcp", "project_id", "un-corroborate-orphaned-proj")

		// No extension resource ever contributes this one -- it's
		// platform_owned from the start, with zero contributions, so
		// withdrawing it must delete the claim outright rather than
		// merely un-owning it.
		r := domain.NewPlatformResource(name, nil, now)
		if err := r.AddAlias(alias); err != nil {
			t.Fatalf("AddAlias: %v", err)
		}
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		withdrawn := domain.PlatformResourceFromSnapshot(domain.PlatformResourceSnapshot{
			Name:      name,
			Labels:    map[string]string{},
			CreatedAt: now,
			UpdatedAt: now.Add(time.Minute),
		})
		if err := repo.Update(ctx, withdrawn); err != nil {
			t.Fatalf("Update (withdraw): %v", err)
		}

		if _, err := repo.ResolveAlias(ctx, alias); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("ResolveAlias after orphaning: got %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateWithRelationships", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name1 := domain.ResourceName("clusters/rel1")
		name2 := domain.ResourceName("nodes/rel2")

		r2 := domain.NewPlatformResource(name2, nil, now)
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("Create r2: %v", err)
		}

		r1 := domain.NewPlatformResource(name1, nil, now)
		_ = r1.AddRelationship(domain.NewResourceRelationship(
			name1, "runs-on", name2, "kind.fleetshift.io", now,
		))
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create r1: %v", err)
		}

		got, err := repo.GetByName(ctx, name1)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		rels := got.Relationships()
		if len(rels) != 1 {
			t.Fatalf("relationships len = %d, want 1", len(rels))
		}
		if rels[0].Type() != "runs-on" {
			t.Errorf("Type = %q, want runs-on", rels[0].Type())
		}
		if rels[0].TargetName() != name2 {
			t.Errorf("TargetName = %q, want %q", rels[0].TargetName(), name2)
		}
	})

	t.Run("ListByCollection_NestedExcludesDescendants", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		// Create a resource in a nested collection: publishers/123/books
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("publishers/123/books/les-mis"), nil, now)); err != nil {
			t.Fatalf("Create book: %v", err)
		}

		// Create a deeper descendant: publishers/123/books/les-mis/chapters
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("publishers/123/books/les-mis/chapters/1"), nil, now)); err != nil {
			t.Fatalf("Create chapter: %v", err)
		}

		// Create a resource in a sibling collection: publishers/123/magazines
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("publishers/123/magazines/vogue"), nil, now)); err != nil {
			t.Fatalf("Create magazine: %v", err)
		}

		// Listing publishers/123/books should return only the direct child (les-mis),
		// not the grandchild chapter or sibling magazine.
		got, err := repo.ListByCollection(ctx, domain.CollectionName("publishers/123/books"))
		if err != nil {
			t.Fatalf("ListByCollection: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1 (only direct children)", len(got))
		}
		if got[0].Name() != domain.ResourceName("publishers/123/books/les-mis") {
			t.Errorf("got[0].Name = %q, want publishers/123/books/les-mis", got[0].Name())
		}
	})

	t.Run("ListByCollection_NestedParentDoesNotIncludeChildren", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		// Parent resource: publishers/123
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("publishers/123"), nil, now)); err != nil {
			t.Fatalf("Create publisher: %v", err)
		}

		// Child resource in a sub-collection: publishers/123/books/les-mis
		if err := repo.Create(ctx, domain.NewPlatformResource(domain.ResourceName("publishers/123/books/les-mis"), nil, now)); err != nil {
			t.Fatalf("Create book: %v", err)
		}

		// Listing the flat "publishers" collection should return only 123,
		// not the nested book.
		got, err := repo.ListByCollection(ctx, domain.CollectionName("publishers"))
		if err != nil {
			t.Fatalf("ListByCollection: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		if got[0].Name() != domain.ResourceName("publishers/123") {
			t.Errorf("got[0].Name = %q, want publishers/123", got[0].Name())
		}
	})

	t.Run("CreateAndGetByName_NestedCollection", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		name := domain.ResourceName("publishers/123/books/les-mis")
		r := domain.NewPlatformResource(name, map[string]string{"genre": "fiction"}, now)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}
		if got.Collection() != domain.CollectionName("publishers/123/books") {
			t.Errorf("Collection = %q, want publishers/123/books", got.Collection())
		}
		if got.Name() != name {
			t.Errorf("Name = %q, want %q", got.Name(), name)
		}
	})

	t.Run("ListByCollection_ReturnsAllResources", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		r1 := domain.NewPlatformResource(domain.ResourceName("clusters/alpha"), nil, now)
		r2 := domain.NewPlatformResource(domain.ResourceName("clusters/beta"), nil, now)

		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("Create alpha: %v", err)
		}
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("Create beta: %v", err)
		}

		got, err := repo.ListByCollection(ctx, domain.CollectionName("clusters"))
		if err != nil {
			t.Fatalf("ListByCollection: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})

	t.Run("GetNotFoundCases", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ResourceIdentities()
		ctx := context.Background()

		_, err := repo.GetByName(ctx, domain.ResourceName("clusters/missing"))
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

	runResolveAliasesBatchTests(t, factory, now)
}

// runResolveAliasesBatchTests exercises
// [domain.ResourceIdentityRepository.ResolveAliasesBatch].
func runResolveAliasesBatchTests(t *testing.T, factory Factory, now time.Time) {
	t.Run("ResolveAliasesBatch", func(t *testing.T) {
		t.Run("ResolvesMultipleAcrossResourcesAndOmitsUnresolved", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ResourceIdentities()
			ctx := context.Background()

			name1 := domain.ResourceName("clusters/rab1")
			r1 := domain.NewPlatformResource(name1, nil, now)
			alias1, _ := domain.NewAlias("gcp", "project_id", "proj-rab-1")
			if err := r1.AddAlias(alias1); err != nil {
				t.Fatalf("AddAlias r1: %v", err)
			}
			if err := repo.Create(ctx, r1); err != nil {
				t.Fatalf("Create r1: %v", err)
			}

			name2 := domain.ResourceName("clusters/rab2")
			r2 := domain.NewPlatformResource(name2, nil, now)
			alias2, _ := domain.NewAlias("aws", "account_id", "acct-rab-2")
			if err := r2.AddAlias(alias2); err != nil {
				t.Fatalf("AddAlias r2: %v", err)
			}
			if err := repo.Create(ctx, r2); err != nil {
				t.Fatalf("Create r2: %v", err)
			}

			unresolved, _ := domain.NewAlias("gcp", "project_id", "no-such-project")

			resolved, err := repo.ResolveAliasesBatch(ctx, []domain.Alias{alias1, alias2, unresolved})
			if err != nil {
				t.Fatalf("ResolveAliasesBatch: %v", err)
			}

			got1, ok := resolved[alias1]
			if !ok {
				t.Fatal("alias1 missing from result")
			}
			if got1 != name1 {
				t.Errorf("alias1 resolved name = %q, want %q", got1, name1)
			}

			got2, ok := resolved[alias2]
			if !ok {
				t.Fatal("alias2 missing from result")
			}
			if got2 != name2 {
				t.Errorf("alias2 resolved name = %q, want %q", got2, name2)
			}

			if _, ok := resolved[unresolved]; ok {
				t.Error("unresolved alias should be absent from result map, not present with a zero value")
			}
		})

		t.Run("EmptyInputReturnsEmptyMapNoError", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ResourceIdentities()
			ctx := context.Background()

			resolved, err := repo.ResolveAliasesBatch(ctx, nil)
			if err != nil {
				t.Fatalf("ResolveAliasesBatch(nil): %v", err)
			}
			if len(resolved) != 0 {
				t.Errorf("resolved len = %d, want 0", len(resolved))
			}
		})
	})
}
