package application_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func newIdentityService(t *testing.T) *application.ResourceIdentityService {
	t.Helper()
	var seq atomic.Int64
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	return &application.ResourceIdentityService{
		Store: newStore(t),
		Now:   func() time.Time { return now },
		NewID: func() domain.PlatformResourceUID {
			n := seq.Add(1)
			return domain.PlatformResourceUID("uid-" + string(rune('0'+n)))
		},
	}
}

func TestClaimOrGet_CreatesAndReturnsStableUID(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view1, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Labels:       map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet first: %v", err)
	}
	if view1.Resource.UID() == "" {
		t.Fatal("UID is empty")
	}

	view2, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet second: %v", err)
	}
	if view2.Resource.UID() != view1.Resource.UID() {
		t.Errorf("second UID = %q, want same as first %q", view2.Resource.UID(), view1.Resource.UID())
	}
}

func TestClaimOrGet_WithRepresentations(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	_, err = svc.AttachRepresentation(ctx, application.AttachRepresentationInput{
		PlatformUID:  view.Resource.UID(),
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1alpha1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
		Labels:       map[string]string{"runtime": "containerd"},
	})
	if err != nil {
		t.Fatalf("AttachRepresentation kind: %v", err)
	}

	got, err := svc.AttachRepresentation(ctx, application.AttachRepresentationInput{
		PlatformUID:  view.Resource.UID(),
		ServiceName:  "gcp.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []domain.RepresentationRole{domain.RepresentationRoleInventory},
		Labels:       map[string]string{"project": "my-proj"},
	})
	if err != nil {
		t.Fatalf("AttachRepresentation gcp: %v", err)
	}

	if len(got.Representations) != 2 {
		t.Errorf("representations len = %d, want 2", len(got.Representations))
	}
}

func TestClaimOrGet_ContradictoryAliasFails(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	_, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/c1",
		Aliases:      []domain.Alias{{Namespace: "gcp", Key: "id", Value: "contested"}},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet first: %v", err)
	}

	_, err = svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/c2",
		Aliases:      []domain.Alias{{Namespace: "gcp", Key: "id", Value: "contested"}},
	})
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("contradictory alias: got %v, want ErrAlreadyExists", err)
	}

	// Verify the second platform resource was NOT partially created.
	_, err = svc.GetByRelativeName(ctx, "clusters/c2")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("partial resource: got %v, want ErrNotFound", err)
	}
}

func TestSetLabels_UpdatesEffectiveLabels(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/labelled",
		Labels:       map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	got, err := svc.SetLabels(ctx, application.SetPlatformResourceLabelsInput{
		PlatformUID: view.Resource.UID(),
		Labels:      map[string]string{"env": "staging", "team": "infra"},
	})
	if err != nil {
		t.Fatalf("SetLabels: %v", err)
	}

	if got.EffectiveLabels["env"] != "staging" {
		t.Errorf("effective_labels[env] = %q, want staging", got.EffectiveLabels["env"])
	}
	if got.EffectiveLabels["team"] != "infra" {
		t.Errorf("effective_labels[team] = %q, want infra", got.EffectiveLabels["team"])
	}
}

func TestAttachRepresentation_NamespacesLabelsInEffectiveLabels(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/eff-labels",
		Labels:       map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	got, err := svc.AttachRepresentation(ctx, application.AttachRepresentationInput{
		PlatformUID:  view.Resource.UID(),
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/eff-labels",
		Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
		Labels:       map[string]string{"version": "1.29"},
	})
	if err != nil {
		t.Fatalf("AttachRepresentation: %v", err)
	}

	if got.EffectiveLabels["env"] != "prod" {
		t.Errorf("effective_labels[env] = %q, want prod", got.EffectiveLabels["env"])
	}
	if got.EffectiveLabels["kind.fleetshift.io/version"] != "1.29" {
		t.Errorf("effective_labels[kind.fleetshift.io/version] = %q, want 1.29", got.EffectiveLabels["kind.fleetshift.io/version"])
	}
}

func TestClaimOrGet_ExistingResource_AttachesNewAliases(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	// First claim creates the resource with one alias.
	view1, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/alias-merge",
		Aliases:      []domain.Alias{{Namespace: "gcp", Key: "id", Value: "proj-1"}},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet first: %v", err)
	}

	// Second claim for the same relative name adds a new alias.
	view2, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/alias-merge",
		Aliases:      []domain.Alias{{Namespace: "aws", Key: "arn", Value: "arn:aws:eks:us-east-1:123:cluster/alias-merge"}},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet second: %v", err)
	}
	if view2.Resource.UID() != view1.Resource.UID() {
		t.Fatalf("UID changed: %q vs %q", view2.Resource.UID(), view1.Resource.UID())
	}
	if len(view2.Aliases) != 2 {
		t.Errorf("aliases len = %d, want 2 (both old and new)", len(view2.Aliases))
	}
}

func TestClaimOrGet_ExistingResource_RejectsContradictoryAlias(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	// Create two resources, the first owning an alias.
	_, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/owner",
		Aliases:      []domain.Alias{{Namespace: "gcp", Key: "id", Value: "contested-val"}},
	})
	if err != nil {
		t.Fatalf("ClaimOrGet owner: %v", err)
	}

	// Create a second resource (no aliases yet).
	_, err = svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/other",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet other: %v", err)
	}

	// Re-claim the second resource with the alias already owned by the first.
	_, err = svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/other",
		Aliases:      []domain.Alias{{Namespace: "gcp", Key: "id", Value: "contested-val"}},
	})
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("contradictory alias on existing resource: got %v, want ErrAlreadyExists", err)
	}
}

func TestTombstoneRepresentation_LeavesPlatformResourceReadable(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/tomb",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	_, err = svc.AttachRepresentation(ctx, application.AttachRepresentationInput{
		PlatformUID:  view.Resource.UID(),
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/tomb",
		Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
	})
	if err != nil {
		t.Fatalf("AttachRepresentation: %v", err)
	}

	err = svc.TombstoneRepresentation(ctx, "kind.fleetshift.io", "clusters", "clusters/tomb")
	if err != nil {
		t.Fatalf("TombstoneRepresentation: %v", err)
	}

	got, err := svc.GetByUID(ctx, view.Resource.UID())
	if err != nil {
		t.Fatalf("GetByUID after tombstone: %v", err)
	}
	if got.Resource.UID() != view.Resource.UID() {
		t.Errorf("UID = %q, want %q", got.Resource.UID(), view.Resource.UID())
	}
	if len(got.Representations) != 0 {
		t.Errorf("active representations len = %d, want 0", len(got.Representations))
	}
}

func TestClaimOrGet_RejectsMismatchedCollectionID(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	_, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "nodes",
		RelativeName: "clusters/prod",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("mismatched collection: got %v, want ErrInvalidArgument", err)
	}
}

func TestClaimOrGet_ValidatesCollectionID(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	_, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "Clusters",
		RelativeName: "Clusters/prod",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("uppercase collection: got %v, want ErrInvalidArgument", err)
	}
}

func TestAttachRepresentation_RejectsMismatchedCollectionID(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/attach-mismatch",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	_, err = svc.AttachRepresentation(ctx, application.AttachRepresentationInput{
		PlatformUID:  view.Resource.UID(),
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "nodes",
		RelativeName: "clusters/attach-mismatch",
		Roles:        []domain.RepresentationRole{domain.RepresentationRoleManaged},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("mismatched collection in AttachRepresentation: got %v, want ErrInvalidArgument", err)
	}
}

func TestAddRelationship_MissingTargetUID(t *testing.T) {
	ctx := context.Background()
	svc := newIdentityService(t)

	view, err := svc.ClaimOrGet(ctx, application.ClaimPlatformResourceInput{
		CollectionID: "clusters",
		RelativeName: "clusters/rel-source",
	})
	if err != nil {
		t.Fatalf("ClaimOrGet: %v", err)
	}

	_, err = svc.AddRelationship(ctx, application.AddRelationshipInput{
		SourceUID:     view.Resource.UID(),
		Type:          "runs-on",
		TargetUID:     "nonexistent-uid",
		SourceService: "kind.fleetshift.io",
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing target: got %v, want ErrNotFound", err)
	}
}
