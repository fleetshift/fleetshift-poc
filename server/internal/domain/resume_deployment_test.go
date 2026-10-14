package domain_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func seedPausedDeployment(t *testing.T, store domain.Store, depName domain.ResourceName, gen domain.Generation) {
	t.Helper()
	seedFulfillmentAndDeployment(t, store, depName, domain.FulfillmentSnapshot{
		Generation: gen,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State:       domain.FulfillmentStateActive,
		PauseReason: "delivery auth failed",
	})
}

func newResumeSpec(store domain.Store) *domain.ResumeDeploymentWorkflowSpec {
	return &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: &fakeOrchestrationWorkflow{},
	}
}

func TestResumeDeployment_StaleEtag_Aborted(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)
	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	_, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name: "deployments/d1",
		Auth: domain.DeliveryAuth{Token: "tok"},
		Etag: domain.Etag("clearly-wrong"),
	})
	if err == nil {
		t.Fatal("expected error for stale etag")
	}
	if !errors.Is(err, domain.ErrStaleGeneration) {
		t.Errorf("expected ErrStaleGeneration, got: %v", err)
	}
}

func TestResumeDeployment_CorrectEtag_Succeeds(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)

	// Compute the current etag by reading the view.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	view, err := tx.Deployments().GetView(context.Background(), "deployments/d1")
	if err != nil {
		t.Fatalf("get view: %v", err)
	}
	_ = tx.Commit()
	currentEtag := view.Etag()

	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}
	result, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name: "deployments/d1",
		Auth: domain.DeliveryAuth{Token: "tok"},
		Etag: currentEtag,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fulfillment.Generation() != 4 {
		t.Errorf("Generation = %d, want 4", result.Fulfillment.Generation())
	}
}

func TestResumeDeployment_StaleExpectedGeneration_Aborted(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)
	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	_, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name:               "deployments/d1",
		Auth:               domain.DeliveryAuth{Token: "tok"},
		ExpectedGeneration: 99,
	})
	if err == nil {
		t.Fatal("expected error for stale expected_generation")
	}
	if !errors.Is(err, domain.ErrStaleGeneration) {
		t.Errorf("expected ErrStaleGeneration, got: %v", err)
	}
}

func TestResumeDeployment_CorrectExpectedGeneration_Succeeds(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)
	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	result, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name:               "deployments/d1",
		Auth:               domain.DeliveryAuth{Token: "tok"},
		ExpectedGeneration: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fulfillment.Generation() != 4 {
		t.Errorf("Generation = %d, want 4", result.Fulfillment.Generation())
	}
}

func TestResumeDeployment_ExpectedGenerationOnly_SucceedsWhenNonGenStateChanged(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation: 3,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State:       domain.FulfillmentStateActive,
		PauseReason: "delivery auth failed",
		UpdatedAt:   time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	// Using only expected_generation (no etag) — should succeed even
	// though non-generation state (UpdatedAt) differs from the original.
	result, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name:               "deployments/d1",
		Auth:               domain.DeliveryAuth{Token: "tok"},
		ExpectedGeneration: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fulfillment.Generation() != 4 {
		t.Errorf("Generation = %d, want 4", result.Fulfillment.Generation())
	}
}

func TestResumeDeployment_SignatureRequiresExpectedGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)
	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	_, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name:          "deployments/d1",
		Auth:          domain.DeliveryAuth{Token: "tok"},
		UserSignature: []byte("some-sig"),
		ValidUntil:    time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected error when user_signature is present but expected_generation is missing")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestResumeDeployment_UnsignedLegacy_NoEtagNoGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedPausedDeployment(t, store, "deployments/d1", 3)
	spec := newResumeSpec(store)
	rec := &stubRecord{ctx: context.Background()}

	result, err := spec.Run(rec, domain.ResumeDeploymentInput{
		Name: "deployments/d1",
		Auth: domain.DeliveryAuth{Token: "tok"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fulfillment.Generation() != 4 {
		t.Errorf("Generation = %d, want 4", result.Fulfillment.Generation())
	}
}
