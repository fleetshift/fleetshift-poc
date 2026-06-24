package domain

import "context"

// IndexAgent manages indexing for a specific [TargetType]. Addons
// provide IndexAgent implementations for their target types; the
// platform routes target lifecycle events to the correct agent based
// on [IndexCapability] declarations.
//
// StartIndexing is called when a target becomes ready. The addon
// creates per-target agents and begins watching resources. Idempotent
// — returns nil if already indexing the target.
//
// StopIndexing is called when a target is being terminated. The addon
// stops watching resources for the target. Idempotent — returns nil
// if no indexer is running for the target. StopIndexing does NOT
// delete inventory data — cleanup is the caller's responsibility.
type IndexAgent interface {
	StartIndexing(ctx context.Context, target TargetInfo) error
	StopIndexing(ctx context.Context, target TargetInfo) error
}
