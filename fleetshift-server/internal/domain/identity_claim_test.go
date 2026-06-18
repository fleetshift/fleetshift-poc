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

	pr, err := ClaimOrGetIdentity(ctx, repo, "uid-1", "clusters", "prod", nil, now)
	if err != nil {
		t.Fatalf("ClaimOrGetIdentity: %v", err)
	}

	if pr.UID() != "uid-1" {
		t.Errorf("UID = %q, want uid-1", pr.UID())
	}
	if pr.CollectionID() != "clusters" {
		t.Errorf("CollectionID = %q, want clusters", pr.CollectionID())
	}
	if pr.RelativeName() != "clusters/prod" {
		t.Errorf("RelativeName = %q, want clusters/prod", pr.RelativeName())
	}
}

func TestClaimOrGetIdentity_IdempotentForSameName(t *testing.T) {
	repo := newFakeIdentityRepo()
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	first, err := ClaimOrGetIdentity(ctx, repo, "uid-1", "clusters", "prod", nil, now)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	second, err := ClaimOrGetIdentity(ctx, repo, "uid-2", "clusters", "prod", nil, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if second.UID() != first.UID() {
		t.Errorf("second UID = %q, want %q (same resource)", second.UID(), first.UID())
	}
}

func TestClaimOrGetIdentity_RaceRetry(t *testing.T) {
	// Simulate: GetByName returns NotFound, Create returns
	// AlreadyExists (concurrent insert won), retry GetByName succeeds.
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	name, _ := NewRelativeResourceName("clusters", "prod")
	winner := NewPlatformResource("uid-winner", "clusters", name, nil, now)

	repo := &raceIdentityRepo{
		fakeIdentityRepo: newFakeIdentityRepo(),
		winner:           winner,
	}

	ctx := context.Background()
	pr, err := ClaimOrGetIdentity(ctx, repo, "uid-loser", "clusters", "prod", nil, now)
	if err != nil {
		t.Fatalf("ClaimOrGetIdentity with race: %v", err)
	}

	if pr.UID() != "uid-winner" {
		t.Errorf("UID = %q, want uid-winner (from race retry)", pr.UID())
	}
}

// ---------------------------------------------------------------------------
// Fake identity repo for unit testing ClaimOrGetIdentity
// ---------------------------------------------------------------------------

type fakeIdentityRepo struct {
	byUID  map[PlatformResourceUID]*PlatformResource
	byName map[RelativeResourceName]*PlatformResource
}

func newFakeIdentityRepo() *fakeIdentityRepo {
	return &fakeIdentityRepo{
		byUID:  make(map[PlatformResourceUID]*PlatformResource),
		byName: make(map[RelativeResourceName]*PlatformResource),
	}
}

func (r *fakeIdentityRepo) Create(_ context.Context, pr *PlatformResource) error {
	if _, exists := r.byName[pr.RelativeName()]; exists {
		return ErrAlreadyExists
	}
	r.byUID[pr.UID()] = pr
	r.byName[pr.RelativeName()] = pr
	return nil
}

func (r *fakeIdentityRepo) Get(_ context.Context, uid PlatformResourceUID) (*PlatformResource, error) {
	pr, ok := r.byUID[uid]
	if !ok {
		return nil, ErrNotFound
	}
	return pr, nil
}

func (r *fakeIdentityRepo) GetByName(_ context.Context, name RelativeResourceName) (*PlatformResource, error) {
	pr, ok := r.byName[name]
	if !ok {
		return nil, ErrNotFound
	}
	return pr, nil
}

func (r *fakeIdentityRepo) Update(_ context.Context, pr *PlatformResource) error {
	r.byUID[pr.UID()] = pr
	r.byName[pr.RelativeName()] = pr
	return nil
}

func (r *fakeIdentityRepo) ListByCollection(_ context.Context, collection CollectionID) ([]*PlatformResource, error) {
	var result []*PlatformResource
	for _, pr := range r.byName {
		if pr.CollectionID() == collection && pr.DeletedAt() == nil {
			result = append(result, pr)
		}
	}
	return result, nil
}

func (r *fakeIdentityRepo) ResolveAlias(_ context.Context, _ Alias) (PlatformResourceUID, error) {
	return "", ErrNotFound
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
	r.fakeIdentityRepo.byName[r.winner.RelativeName()] = r.winner
	return ErrAlreadyExists
}
