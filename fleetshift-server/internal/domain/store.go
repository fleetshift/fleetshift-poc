package domain

import "context"

// Store provides transactional access to all repositories. Use
// [Store.Begin] to start a read-write transaction or
// [Store.BeginReadOnly] for a read-only transaction, then access
// repositories through the returned [Tx].
type Store interface {
	// Begin starts a read-write transaction. On SQLite this issues
	// BEGIN IMMEDIATE so writers are serialized and read-lock-upgrade
	// deadlocks cannot occur.
	Begin(ctx context.Context) (Tx, error)

	// BeginReadOnly starts a read-only transaction. It uses a
	// deferred lock so it never contends with other readers or
	// writers. Use this for queries that do not mutate state.
	BeginReadOnly(ctx context.Context) (Tx, error)
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
	Fulfillments() FulfillmentRepository
	Deployments() DeploymentRepository
	Deliveries() DeliveryRepository
	Inventory() InventoryRepository
	ManagedResources() ManagedResourceRepository
	SignerEnrollments() SignerEnrollmentRepository
	Commit() error
	Rollback() error
}
