package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// TargetRepository persists and retrieves target metadata.
type TargetRepository interface {
	Create(ctx context.Context, target TargetInfo) error
	CreateOrUpdate(ctx context.Context, target TargetInfo) error
	Get(ctx context.Context, id TargetID) (TargetInfo, error)
	List(ctx context.Context) ([]TargetInfo, error)
	Delete(ctx context.Context, id TargetID) error
}

// FulfillmentRepository persists and retrieves fulfillments.
// Create and Update read pending strategy records from [Fulfillment.Snapshot]
// and flush them to storage, then call [Fulfillment.DrainPendingStrategyRecords]
// to clear the buffers. Get materializes current strategy specs by joining
// the version tables.
type FulfillmentRepository interface {
	Create(ctx context.Context, f *Fulfillment) error
	Get(ctx context.Context, id FulfillmentID) (*Fulfillment, error)
	Update(ctx context.Context, f *Fulfillment) error
	Delete(ctx context.Context, id FulfillmentID) error
}

// DeploymentRepository persists and retrieves the thin deployment
// aggregate. Mutations that affect orchestration state go through
// [FulfillmentRepository].
type DeploymentRepository interface {
	Create(ctx context.Context, d Deployment) error
	Get(ctx context.Context, name ResourceName) (Deployment, error)
	GetView(ctx context.Context, name ResourceName) (DeploymentView, error)
	ListView(ctx context.Context) ([]DeploymentView, error)
	Delete(ctx context.Context, name ResourceName) error
}

// InventoryRepository persists and retrieves inventory items.
type InventoryRepository interface {
	Create(ctx context.Context, item InventoryItem) error
	CreateOrUpdate(ctx context.Context, item InventoryItem) error
	Get(ctx context.Context, id InventoryItemID) (InventoryItem, error)
	List(ctx context.Context) ([]InventoryItem, error)
	ListByType(ctx context.Context, t InventoryItemType) ([]InventoryItem, error)
	Update(ctx context.Context, item InventoryItem) error
	Delete(ctx context.Context, id InventoryItemID) error
}

// DeliveryRepository persists deliveries for each fulfillment-target pair.
type DeliveryRepository interface {
	Put(ctx context.Context, d Delivery) error
	Get(ctx context.Context, id DeliveryID) (Delivery, error)
	GetByFulfillmentTarget(ctx context.Context, fID FulfillmentID, tID TargetID) (Delivery, error)
	ListByFulfillment(ctx context.Context, fID FulfillmentID) ([]Delivery, error)
	ListActive(ctx context.Context, targetIDs []TargetID) ([]Delivery, error)
	DeleteByFulfillment(ctx context.Context, fID FulfillmentID) error
}

// ExtensionResourceRepository persists extension resource types,
// versioned intents, instance records, and managed state. Grouped into
// a single repository because these tables form a cohesive aggregate
// boundary for the extension resource model.
//
// Intent versioning is owned by the [ExtensionResource] aggregate (via
// [ManagedState]). Create reads pending intents from the aggregate's
// [ExtensionResource.Snapshot] and flushes them to storage. The
// aggregate is only valid within the scope of a single transaction; on
// the next read, [ExtensionResourceFromSnapshot] naturally produces an
// aggregate with no pending intents.
type ExtensionResourceRepository interface {
	// Type registration
	CreateType(ctx context.Context, def ExtensionResourceType) error
	GetType(ctx context.Context, rt ResourceType) (ExtensionResourceType, error)
	ListTypes(ctx context.Context) ([]ExtensionResourceType, error)
	DeleteType(ctx context.Context, rt ResourceType) error

	// Instance aggregate
	Create(ctx context.Context, r *ExtensionResource) error
	Get(ctx context.Context, name FullResourceName) (*ExtensionResource, error)
	GetByUID(ctx context.Context, uid ExtensionResourceUID) (*ExtensionResource, error)
	ListByResourceType(ctx context.Context, rt ResourceType) ([]*ExtensionResource, error)

	// Delete removes the extension resource, along with its derived
	// representation (see [ResourceRepresentation]'s doc) and its own
	// reported-alias payload (see [InventoryReplacement.Aliases]) --
	// there is nothing else to reconcile, since that payload was
	// never folded into any cross-resource state.
	Delete(ctx context.Context, name FullResourceName) error

	// Read views (join extension resource + managed state + intent + fulfillment + inventory)
	GetView(ctx context.Context, name FullResourceName) (ExtensionResourceView, error)
	ListViewsByType(ctx context.Context, rt ResourceType) ([]ExtensionResourceView, error)

	// Versioned intent (read-only; writes go through the aggregate drain).
	// Intents are owned by their extension resource; ON DELETE CASCADE
	// handles cleanup when the parent is deleted.
	GetIntent(ctx context.Context, uid ExtensionResourceUID, version IntentVersion) (ResourceIntent, error)

	// Inventory mutations -- natural-key-addressed, narrow command
	// methods (not a general Save). Unlike the rest of this
	// interface, these resolve-or-create the extension_resources row
	// themselves (see [InventoryReplacement]/[InventoryDelta]'s natural
	// key doc) rather than requiring the row to already exist.
	//
	// ReplaceInventory treats each [InventoryReplacement] as the
	// complete latest inventory state for its resource: fields absent
	// from the replacement are cleared/deleted from latest state, with
	// the exception of Observation -- see its field doc. Aliases are
	// stored as a pending, unreconciled payload -- see
	// [InventoryReplacement.Aliases]'s doc -- so this never fails or
	// reports a conflict on account of Aliases; the only errors are
	// the usual infrastructure/argument-validation ones.
	ReplaceInventory(ctx context.Context, replacements []InventoryReplacement) error

	// ApplyInventoryDeltas applies incremental, field-level changes:
	// fields absent from an [InventoryDelta] are left unchanged. Like
	// ReplaceInventory, alias-bearing fields never cause a conflict
	// error -- see [InventoryDelta.UpsertAliases]'s doc.
	ApplyInventoryDeltas(ctx context.Context, deltas []InventoryDelta) error

	// Observation history (append-only). Neither ReplaceInventory nor
	// ApplyInventoryDeltas populates this synchronously today -- see
	// their doc above -- so this always returns an empty result until
	// a future asynchronous writer exists. The method and its backing
	// tables are kept for that writer; see
	// poc/inventory-identity-reconciliation for the archived
	// synchronous-history design this replaced.
	ListObservations(ctx context.Context, uid ExtensionResourceUID, limit int) ([]Observation, error)

	// Condition transition history (append-only). Like
	// ListObservations above, neither ReplaceInventory nor
	// ApplyInventoryDeltas populates this synchronously today, so this
	// always returns an empty result until a future asynchronous
	// writer exists.
	ListConditionTransitions(ctx context.Context, uid ExtensionResourceUID, conditionType *ConditionType, limit int) ([]ConditionTransition, error)
}

// InventoryReplacement is a command DTO -- not a domain object --
// describing the complete latest inventory state for a single
// extension resource, identified by its natural key (ResourceType,
// Name) rather than an [ExtensionResourceUID] resolved ahead of time
// by the caller. See [ExtensionResourceRepository.ReplaceInventory].
//
// CandidateUID is generated by the caller (see
// [NewExtensionResourceUID]) and used only if this natural key has no
// existing extension_resources row: the repository resolves-or-creates
// within the same statement as the inventory write, so the caller
// never needs a UID lookup round trip of its own. When the row
// already exists, CandidateUID is discarded and the row's own UID is
// used instead.
//
// Aliases is the complete set of aliases *this extension resource*
// currently reports for Name. Unlike Labels/Conditions below, Aliases
// is not reconciled against any other extension resource's
// contributions or against existing platform identity at write time:
// callers supply it as an already-canonical [AliasSet], and the
// repository stores that pending payload on this extension resource's
// own row (see
// [ExtensionResource.ReportedAliases]), replacing whatever this same
// extension resource previously reported -- a full replace, not a
// cross-contributor merge. If the input repeats the same
// (namespace, key), [AliasSet]'s construction semantics apply and the
// later value wins. The zero value of Aliases stores an empty payload,
// which is itself meaningful ("this extension resource asserts no
// aliases now"), not a no-op.
//
// This is a deliberate simplification from an earlier design that
// classified aliases against cross-resource claims/contributions
// state synchronously, at write time, and could fail the write with
// an alias conflict. That work is valuable context (see
// poc/inventory-identity-reconciliation for the executable reference
// and its README for the reasoning) but adds too much cost to the hot
// report path for the common case. Aliases reported here are pending
// input for a future, asynchronous reconciliation process that
// decides which extension resource's assertions -- if any conflict --
// become the platform's accepted identity; see
// [ResourceIdentityRepository]'s doc. Until that process exists,
// reported aliases are not trusted by alias resolution
// ([ResourceIdentityRepository.ResolveAliasesBatch]) or platform
// representation reads.
//
// Labels is the complete observed label set; nil and empty both
// normalize to an empty latest label set. Conditions is the complete
// current condition set -- conditions absent from the replacement are
// deleted from latest state (without a transition row in this pass).
//
// Observation is the one field that does not follow the
// "absence = deletion" rule that governs Labels/Conditions above: a
// nil Observation, or a non-nil Observation pointing to the JSON
// literal null, leaves the latest observation untouched -- there is
// no "clear the observation" operation. Any other non-nil value
// replaces the latest observation. Neither case appends a history
// row today; see [ExtensionResourceRepository.ListObservations]'s doc.
type InventoryReplacement struct {
	ResourceType ResourceType
	Name         ResourceName
	CandidateUID ExtensionResourceUID
	Aliases      AliasSet

	Labels      map[string]string
	Observation *json.RawMessage
	Conditions  []Condition
	ObservedAt  time.Time
	ReceivedAt  time.Time
}

// InventoryDelta is a command DTO -- not a domain object -- describing
// incremental, field-level changes to a single extension resource's
// inventory state, identified by natural key. See
// [InventoryReplacement]'s doc for the natural-key resolve-or-create
// semantics, shared with [ExtensionResourceRepository.ApplyInventoryDeltas].
//
// Aliases are identity-bearing, unlike Labels/Conditions below, so
// unlike those two fields' Set/Upsert-plus-Delete shape, they get a
// third op mirroring [InventoryReplacement.Aliases]'s "this is my
// complete state" shape: UpsertAliases, DeleteAliases, and
// ReplaceAliases. Per [InventoryReplacement.Aliases]'s doc, reported
// aliases are a pending payload, not reconciled or conflict-checked
// synchronously at write time.
//
// UpsertAliases is currently the only one of the three actually
// implemented against the reported-alias payload: it merges the given,
// already-canonical alias set into this extension resource's existing
// ReportedAliases (by (namespace, key), replacing that key's prior
// value if already present). If the merged payload is unchanged,
// repositories may skip the alias payload write and leave the
// extension resource's own UpdatedAt unchanged.
// DeleteAliases and ReplaceAliases are not yet implemented against the
// payload -- see extensionresourcerepotest's delta alias tests for the
// target contract ahead of that landing -- so [ValidateInventoryDelta]
// rejects any delta setting either one with [ErrUnimplemented] rather
// than accepting it and silently leaving stale pending aliases in
// place.
//
// Fields left at their zero value are unchanged: SetLabels/DeleteLabels
// only touch the named keys, and UpsertConditions/DeleteConditions only
// touch the named condition types. A delta with no field-level changes
// is a valid heartbeat that still bumps resource-level freshness.
//
// Observation follows the same pointer semantics as
// [InventoryReplacement.Observation]: nil, or non-nil pointing to the
// JSON literal null, leaves the latest observation untouched; any
// other non-nil value replaces latest. Neither case appends a history
// row today; see [ExtensionResourceRepository.ListObservations]'s doc.
type InventoryDelta struct {
	ResourceType ResourceType
	Name         ResourceName
	CandidateUID ExtensionResourceUID

	// UpsertAliases adds or updates specific (namespace, key)
	// contributions from this extension resource -- see this type's
	// doc above.
	UpsertAliases AliasSet
	// DeleteAliases would retract specific (namespace, key)
	// contributions this extension resource previously made,
	// regardless of their current value (see [AliasRef]'s doc for why
	// no value is needed). Not yet implemented -- see this type's doc
	// above -- so any non-empty value here is rejected outright by
	// [ValidateInventoryDelta].
	DeleteAliases []AliasRef
	// ReplaceAliases would, if non-empty, replace this extension
	// resource's entire alias contribution in one shot -- see this
	// type's doc above. Not yet implemented, so any non-empty value
	// here is rejected outright by [ValidateInventoryDelta].
	ReplaceAliases AliasSet

	SetLabels    map[string]string
	DeleteLabels []string

	Observation *json.RawMessage

	UpsertConditions []Condition
	DeleteConditions []ConditionType

	ObservedAt time.Time
	ReceivedAt time.Time
}

// ValidateInventoryDelta rejects a delta whose SetLabels/DeleteLabels
// or UpsertConditions/DeleteConditions contradict each other -- the
// same key present on both sides of a pair -- and rejects any delta
// that sets DeleteAliases or ReplaceAliases at all, since neither is
// implemented against the reported-alias payload yet (see
// [InventoryDelta]'s doc). Rejecting outright, rather than silently
// accepting and ignoring them, means a caller that expects a delete or
// replace to take effect finds out immediately instead of leaving
// stale pending aliases in place with no indication anything went
// wrong.
//
// The label/condition checks can't be left for either backend's
// ApplyInventoryDeltas to resolve on its own: Postgres's
// applyInventoryDeltasCoreCTEs runs a pair's set/upsert and delete
// sides as sibling writable CTEs with no defined execution order
// between them when they touch the same table, while SQLite's Go
// orchestration happens to run them as ordered sequential statements
// -- so the very same contradictory delta would silently resolve
// differently per backend if it ever reached either one. Both
// [ExtensionResourceRepository.ApplyInventoryDeltas] implementations
// call this for every delta before building any batch argument, so
// every rejection here is always caught in Go before any SQL runs,
// regardless of caller.
func ValidateInventoryDelta(d InventoryDelta) error {
	for _, k := range d.DeleteLabels {
		if _, ok := d.SetLabels[k]; ok {
			return fmt.Errorf("%w: label %q is present in both SetLabels and DeleteLabels", ErrInvalidArgument, k)
		}
	}
	deletedConditions := make(map[ConditionType]struct{}, len(d.DeleteConditions))
	for _, t := range d.DeleteConditions {
		deletedConditions[t] = struct{}{}
	}
	for _, c := range d.UpsertConditions {
		if _, ok := deletedConditions[c.Type()]; ok {
			return fmt.Errorf("%w: condition type %q is present in both UpsertConditions and DeleteConditions", ErrInvalidArgument, c.Type())
		}
	}
	if len(d.DeleteAliases) > 0 {
		return fmt.Errorf("%w: DeleteAliases is not yet implemented against the reported-alias payload", ErrUnimplemented)
	}
	if d.ReplaceAliases.Len() > 0 {
		return fmt.Errorf("%w: ReplaceAliases is not yet implemented against the reported-alias payload", ErrUnimplemented)
	}
	return nil
}

// ResourceIdentityRepository persists and retrieves canonical platform
// resource identities. The [PlatformResource] aggregate owns its child
// entities (representations, aliases, relationships); the repository
// reconciles the full aggregate state on Create/Update.
//
// A platform resource has no UID (see [NewPlatformResource]'s doc), so
// every method here addresses resources by [ResourceName] or
// [CollectionName]. GetByName and ListByCollection fall back to a
// *virtual* resource -- synthesized on read, with no physical
// platform_resources row -- when a name has representations, aliases,
// or relationships but has never needed its own labels: see
// resource_identity_and_api.md's "virtual platform resources" section.
//
// The aliases this aggregate exposes ([PlatformResource.Aliases]) are
// accepted platform identity, populated by [PlatformResource.AddAlias]
// and Create/Update -- a separate, deliberately-not-yet-connected
// concept from [InventoryReplacement.Aliases]'s pending, per-extension-
// resource reported payload. Inventory reporting's ReplaceInventory/
// ApplyInventoryDeltas do not call into this repository's aliases at
// all in this iteration; a future asynchronous reconciliation process
// is what will eventually decide which reported aliases become
// accepted here.
type ResourceIdentityRepository interface {
	Create(ctx context.Context, r *PlatformResource) error
	GetByName(ctx context.Context, name ResourceName) (*PlatformResource, error)
	Update(ctx context.Context, r *PlatformResource) error
	ListByCollection(ctx context.Context, collection CollectionName) ([]*PlatformResource, error)

	// ResolveAliasesBatch resolves a batch of aliases to their owning
	// platform resource's [ResourceName] in a single round trip.
	// Aliases that don't resolve to any platform resource are simply
	// absent from the result map -- callers distinguish "unresolved"
	// from "resolved" by map membership.
	ResolveAliasesBatch(ctx context.Context, aliases []Alias) (map[Alias]ResourceName, error)
}
