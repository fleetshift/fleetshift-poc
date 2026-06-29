package domain

import "context"

// Store provides transactional access to all repositories. Use
// [Store.Begin] to start a read-write transaction or
// [Store.BeginReadOnly] for a read-only transaction, then access
// repositories through the returned [Tx].
//
// Both backends use serializable isolation:
//   - PostgreSQL: SERIALIZABLE (SSI). Concurrent transactions that
//     would produce a non-serializable history are aborted with
//     SQLSTATE 40001 (serialization_failure). Callers inside durable
//     workflow activities get automatic retries from the workflow
//     engine; application-service callers should anticipate adding
//     retry logic in the future.
//   - SQLite: BEGIN IMMEDIATE + WAL, which serializes writers and
//     prevents read-lock-upgrade deadlocks.
type Store interface {
	// Begin starts a read-write transaction with serializable
	// isolation. On PostgreSQL this is SERIALIZABLE (SSI); on SQLite
	// this issues BEGIN IMMEDIATE.
	Begin(ctx context.Context) (Tx, error)

	// BeginReadOnly starts a read-only transaction with serializable
	// isolation. Use this for queries that do not mutate state.
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
	ExtensionResources() ExtensionResourceRepository
	SignerEnrollments() SignerEnrollmentRepository
	ResourceIdentities() ResourceIdentityRepository
	Commit() error
	Rollback() error
}
