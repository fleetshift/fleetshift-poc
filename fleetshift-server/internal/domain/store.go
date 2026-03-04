package domain

import "context"

// Store provides transactional access to all repositories. Use
// [Store.Begin] to start a transaction, then access repositories
// through the returned [Tx].
type Store interface {
	Begin(ctx context.Context) (Tx, error)
}

// Tx is a transaction that provides access to all repositories.
// All repository operations performed through a single Tx share the
// same underlying transaction. Call [Tx.Commit] to persist changes
// or [Tx.Rollback] to discard them. Rollback is safe to call after
// Commit (it becomes a no-op), so it can be unconditionally deferred:
//
//	tx, err := store.Begin(ctx)
//	if err != nil { return err }
//	defer tx.Rollback()
//	// ... use tx ...
//	return tx.Commit()
type Tx interface {
	Targets() TargetRepository
	Deployments() DeploymentRepository
	Deliveries() DeliveryRepository
	Inventory() InventoryRepository
	Commit() error
	Rollback() error
}
