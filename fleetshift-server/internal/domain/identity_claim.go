package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ClaimOrGetIdentity claims a platform resource name, creating the
// resource if it does not yet exist. If the name is already claimed,
// the existing resource is returned (idempotent).
//
// This is a domain service function — it accepts a
// [ResourceIdentityRepository] from an existing transaction so callers
// control the transaction boundary. A new [PlatformResourceUID] is
// generated only when a resource must be created.
//
// If [ResourceIdentityRepository.Create] returns [ErrAlreadyExists]
// (race with a concurrent claim), the function retries via GetByName.
func ClaimOrGetIdentity(
	ctx context.Context,
	repo ResourceIdentityRepository,
	name ResourceName,
	labels map[string]string,
	now time.Time,
) (*PlatformResource, error) {
	existing, err := repo.GetByName(ctx, name)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("claim identity: get by name: %w", err)
	}

	pr := NewPlatformResource(NewPlatformResourceUID(), name, labels, now)
	if err := repo.Create(ctx, pr); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			existing, retryErr := repo.GetByName(ctx, name)
			if retryErr != nil {
				return nil, fmt.Errorf("claim identity: retry get after race: %w", retryErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("claim identity: create: %w", err)
	}
	return pr, nil
}
