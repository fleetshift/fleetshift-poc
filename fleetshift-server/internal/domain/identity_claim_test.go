package domain

import (
	"context"
	"testing"
	"time"
)

func TestClaimOrGetIdentity_CreatesNewResource(t *testing.T) {
	repo := newFakeIdentityRepo()
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	pr, err := ClaimOrGetIdentity(ctx, repo, "clusters/prod", nil, now)
	if err != nil {
		t.Fatalf("ClaimOrGetIdentity: %v", err)
	}

	if pr.UID().IsZero() {
		t.Error("UID is zero, want non-zero")
	}
	if pr.Collection() != "clusters" {
		t.Errorf("Collection = %q, want clusters", pr.Collection())
	}
	if pr.Name() != "clusters/prod" {
		t.Errorf("Name = %q, want clusters/prod", pr.Name())
	}
}

func TestClaimOrGetIdentity_IdempotentForSameName(t *testing.T) {
	repo := newFakeIdentityRepo()
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	first, err := ClaimOrGetIdentity(ctx, repo, "clusters/prod", nil, now)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	second, err := ClaimOrGetIdentity(ctx, repo, "clusters/prod", nil, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if second.UID() != first.UID() {
		t.Errorf("second UID = %s, want %s (same resource)", second.UID(), first.UID())
	}
}

func TestClaimOrGetIdentity_RaceRetry(t *testing.T) {
	// Simulate: GetByName returns NotFound, Create returns
	// AlreadyExists (concurrent insert won), retry GetByName succeeds.
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uidWinner := NewPlatformResourceUID()
	winner := NewPlatformResource(uidWinner, "clusters/prod", nil, now)

	repo := &raceIdentityRepo{
		fakeIdentityRepo: newFakeIdentityRepo(),
		winner:           winner,
	}

	ctx := context.Background()
	pr, err := ClaimOrGetIdentity(ctx, repo, "clusters/prod", nil, now)
	if err != nil {
		t.Fatalf("ClaimOrGetIdentity with race: %v", err)
	}

	if pr.UID() != uidWinner {
		t.Errorf("UID = %s, want %s (from race retry)", pr.UID(), uidWinner)
	}
}

// ---------------------------------------------------------------------------
// Fake identity repo for unit testing ClaimOrGetIdentity
// ---------------------------------------------------------------------------

type fakeIdentityRepo struct {
	byUID  map[PlatformResourceUID]*PlatformResource
	byName map[ResourceName]*PlatformResource
}

func newFakeIdentityRepo() *fakeIdentityRepo {
	return &fakeIdentityRepo{
		byUID:  make(map[PlatformResourceUID]*PlatformResource),
		byName: make(map[ResourceName]*PlatformResource),
	}
}

func (r *fakeIdentityRepo) Create(_ context.Context, pr *PlatformResource) error {
	if _, exists := r.byName[pr.Name()]; exists {
		return ErrAlreadyExists
	}
	r.byUID[pr.UID()] = pr
	r.byName[pr.Name()] = pr
	return nil
}

func (r *fakeIdentityRepo) Get(_ context.Context, uid PlatformResourceUID) (*PlatformResource, error) {
	pr, ok := r.byUID[uid]
	if !ok {
		return nil, ErrNotFound
	}
	return pr, nil
}

func (r *fakeIdentityRepo) GetByName(_ context.Context, name ResourceName) (*PlatformResource, error) {
	pr, ok := r.byName[name]
	if !ok {
		return nil, ErrNotFound
	}
	return pr, nil
}

func (r *fakeIdentityRepo) Update(_ context.Context, pr *PlatformResource) error {
	r.byUID[pr.UID()] = pr
	r.byName[pr.Name()] = pr
	return nil
}

func (r *fakeIdentityRepo) ListByCollection(_ context.Context, collection CollectionName) ([]*PlatformResource, error) {
	var result []*PlatformResource
	for _, pr := range r.byName {
		if pr.Collection() == collection {
			result = append(result, pr)
		}
	}
	return result, nil
}

func (r *fakeIdentityRepo) ResolveAlias(_ context.Context, _ Alias) (PlatformResourceUID, error) {
	return PlatformResourceUID{}, ErrNotFound
}

func (r *fakeIdentityRepo) GetRepresentation(_ context.Context, _ FullResourceName) (ResourceRepresentation, error) {
	return ResourceRepresentation{}, ErrNotFound
}

// raceIdentityRepo simulates a Create race: Create always returns
// ErrAlreadyExists, and the winner resource becomes visible on the
// next GetByName call.
type raceIdentityRepo struct {
	*fakeIdentityRepo
	winner  *PlatformResource
	raceHit bool
}

func (r *raceIdentityRepo) Create(_ context.Context, _ *PlatformResource) error {
	r.raceHit = true
	r.fakeIdentityRepo.byUID[r.winner.UID()] = r.winner
	r.fakeIdentityRepo.byName[r.winner.Name()] = r.winner
	return ErrAlreadyExists
}
