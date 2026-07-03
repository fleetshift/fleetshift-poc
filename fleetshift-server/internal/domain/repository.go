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

	// Delete removes the extension resource, along with its
	// derived representation (see [ResourceRepresentation]'s doc).
	// Like a later report that omits a previously-contributed alias
	// (see [InventoryReplacement.Aliases]), deleting the extension
	// resource retracts every alias it contributed, unless another
	// extension resource representing the same [ResourceName] is
	// still asserting it.
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
	// the exception of Observation -- see its field doc. Returns any
	// [AliasConflict]s encountered folding replacements' Aliases into
	// resource_alias_claims/resource_alias_contributions.
	ReplaceInventory(ctx context.Context, replacements []InventoryReplacement) ([]AliasConflict, error)

	// ApplyInventoryDeltas applies incremental, field-level changes:
	// fields absent from an [InventoryDelta] are left unchanged.
	ApplyInventoryDeltas(ctx context.Context, deltas []InventoryDelta) ([]AliasConflict, error)

	// Observation history (append-only; populated as a side effect of
	// ReplaceInventory/ApplyInventoryDeltas, never written directly).
	ListObservations(ctx context.Context, uid ExtensionResourceUID, limit int) ([]Observation, error)

	// Condition transition history (append-only; populated as a side
	// effect of ReplaceInventory/ApplyInventoryDeltas when a supplied
	// [Condition] represents a genuine state change).
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
// currently contributes for Name -- the same "absence = removal" rule
// as Labels below, but scoped to this one contributor rather than to
// Name as a whole. Many different extension resources (identified by
// their own natural key above) can represent the same Name at once
// -- e.g. a cluster represented by both a cloud-provider addon and a
// Kubernetes addon -- and each one's alias contribution is replaced
// independently: an alias this extension resource stops reporting is
// retracted, unless another extension resource currently representing
// the same Name is still contributing it, in which case it remains in
// effect. Across different contributing extension resources, aliases
// are additive: agreeing on the same (namespace, key, value) is fine,
// but two contributors for the same Name asserting different values
// for the same (namespace, key), or two contributors resolving to
// different Names asserting the very same (namespace, key, value),
// are both rejected -- see [AliasConflict].
//
// Labels is the complete observed label set; nil and empty both
// normalize to an empty latest label set. Conditions is the complete
// current condition set -- conditions absent from the replacement are
// deleted from latest state (without a transition row in this pass).
//
// Observation is the one field that does not follow the
// "absence = deletion" rule that governs Labels/Conditions above: a
// nil Observation, or a non-nil Observation pointing to the JSON
// literal null, leaves the latest observation untouched and appends
// no history row -- there is no "clear the observation" operation.
// Any other non-nil value replaces the latest observation and appends
// a history row, unless it's identical to the current latest
// observation (repositories dedup to avoid noisy history on repeat
// resyncs).
type InventoryReplacement struct {
	ResourceType ResourceType
	Name         ResourceName
	CandidateUID ExtensionResourceUID
	Aliases      []Alias

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
// third op: UpsertAliases and DeleteAliases mirror SetLabels/
// DeleteLabels -- add or update named (namespace, key) contributions,
// or retract named ones this extension resource previously made --
// but ReplaceAliases additionally offers the same "this is my
// complete state" convenience [InventoryReplacement.Aliases] gets,
// scoped to just this delta's alias contribution rather than the
// whole resource: every alias this extension resource previously
// contributed that's absent from ReplaceAliases is retracted, exactly
// like a ReplaceInventory call's Aliases field (see its doc for the
// full per-contributor, additive-across-contributors contract, shared
// verbatim here, including conflict detection -- see [AliasConflict]).
// ReplaceAliases is mutually exclusive with UpsertAliases/DeleteAliases
// in the same delta: combining them is rejected by
// [ValidateInventoryDelta] as an ambiguous request, not merged. A
// delta using UpsertAliases/DeleteAliases only (no ReplaceAliases)
// behaves exactly like every other Delta field below: an alias
// contribution absent from both is simply left unchanged, never
// retracted -- retracting *every* alias in one call (replacing with
// none) needs an explicit, non-empty-vs-omitted signal that a plain
// slice can't carry (see the nil-vs-empty note on
// [InventoryReplacement.Aliases]'s sibling fields), so that one narrow
// case still requires a full [InventoryReplacement] call.
//
// Fields left at their zero value are unchanged: SetLabels/DeleteLabels
// only touch the named keys, and UpsertConditions/DeleteConditions only
// touch the named condition types. A delta with no field-level changes
// is a valid heartbeat that still bumps resource-level freshness.
//
// Observation follows the same pointer semantics as
// [InventoryReplacement.Observation]: nil, or non-nil pointing to the
// JSON literal null, leaves the latest observation untouched and
// appends no history row; any other non-nil value replaces latest and
// appends a history row (subject to the same dedup rule).
type InventoryDelta struct {
	ResourceType ResourceType
	Name         ResourceName
	CandidateUID ExtensionResourceUID

	// UpsertAliases adds or updates specific (namespace, key)
	// contributions from this extension resource -- see this type's
	// doc above.
	UpsertAliases []Alias
	// DeleteAliases retracts specific (namespace, key) contributions
	// this extension resource previously made, regardless of their
	// current value (see [AliasRef]'s doc for why no value is
	// needed). A no-op for any key this extension resource never
	// contributed.
	DeleteAliases []AliasRef
	// ReplaceAliases, if non-empty, replaces this extension
	// resource's entire alias contribution in one shot -- see this
	// type's doc above. Mutually exclusive with UpsertAliases/
	// DeleteAliases in the same delta.
	ReplaceAliases []Alias

	SetLabels    map[string]string
	DeleteLabels []string

	Observation *json.RawMessage

	UpsertConditions []Condition
	DeleteConditions []ConditionType

	ObservedAt time.Time
	ReceivedAt time.Time
}

// ValidateInventoryDelta rejects a delta whose SetLabels/DeleteLabels,
// UpsertConditions/DeleteConditions, or UpsertAliases/DeleteAliases
// contradict each other -- the same key present on both sides of a
// pair -- or whose ReplaceAliases is combined with UpsertAliases/
// DeleteAliases in the same delta. The label/condition/alias-pair
// checks can't be left for either backend's ApplyInventoryDeltas to
// resolve on its own: Postgres's applyInventoryDeltasCoreCTEs runs a
// pair's set/upsert and delete sides as sibling writable CTEs with no
// defined execution order between them when they touch the same
// table, while SQLite's Go orchestration happens to run them as
// ordered sequential statements -- so the very same contradictory
// delta would silently resolve differently per backend if it ever
// reached either one. The ReplaceAliases check has no such
// per-backend divergence to guard against -- it's simply an
// underspecified request, since UpsertAliases/DeleteAliases can't be
// reconciled against a same-call "this is my complete state" that
// doesn't mention them either way. Both
// [ExtensionResourceRepository.ApplyInventoryDeltas] implementations
// call this for every delta before building any batch argument, so
// every contradiction here is always caught in Go before any SQL
// runs, regardless of caller.
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
	if len(d.ReplaceAliases) > 0 && (len(d.UpsertAliases) > 0 || len(d.DeleteAliases) > 0) {
		return fmt.Errorf("%w: ReplaceAliases cannot be combined with UpsertAliases or DeleteAliases in the same delta", ErrInvalidArgument)
	}
	deletedAliases := make(map[AliasRef]struct{}, len(d.DeleteAliases))
	for _, ref := range d.DeleteAliases {
		deletedAliases[ref] = struct{}{}
	}
	for _, a := range d.UpsertAliases {
		if _, ok := deletedAliases[AliasRef{Namespace: a.Namespace, Key: a.Key}]; ok {
			return fmt.Errorf("%w: alias %s/%s is present in both UpsertAliases and DeleteAliases", ErrInvalidArgument, a.Namespace, a.Key)
		}
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
type ResourceIdentityRepository interface {
	Create(ctx context.Context, r *PlatformResource) error
	GetByName(ctx context.Context, name ResourceName) (*PlatformResource, error)
	Update(ctx context.Context, r *PlatformResource) error
	ListByCollection(ctx context.Context, collection CollectionName) ([]*PlatformResource, error)

	// Cross-resource lookups (can't live on the aggregate).
	ResolveAlias(ctx context.Context, alias Alias) (ResourceName, error)
	GetRepresentation(ctx context.Context, name FullResourceName) (ResourceRepresentation, error)

	// ResolveAliasesBatch resolves a batch of aliases to their owning
	// platform resource's [ResourceName] in a single round trip.
	// Aliases that don't resolve to any platform resource are simply
	// absent from the result map -- callers distinguish "unresolved"
	// from "resolved" by map membership.
	ResolveAliasesBatch(ctx context.Context, aliases []Alias) (map[Alias]ResourceName, error)
}

// AliasConflictKind classifies why an alias submitted alongside an
// inventory report ([InventoryReplacement.Aliases] /
// [InventoryDelta.UpsertAliases] / [InventoryDelta.ReplaceAliases])
// did not take effect.
//
// Aliases are contributed per extension resource (see
// [InventoryReplacement.Aliases]'s doc for the full contract): many
// different extension resources can legitimately represent the same
// platform [ResourceName] at once -- e.g. one per addon -- and each
// contributes its own aliases, additively. The two kinds below are
// what keeps that additive union consistent:
//
//   - AliasConflictResourceHasDifferentValue catches two contributing
//     extension resources for the *same* Name disagreeing about the
//     value of the *same* (namespace, key).
//   - AliasConflictValueClaimedByOther catches the same
//     (namespace, key, value) being claimed by extension resources
//     that resolve to two *different* Names.
//
// Neither fires when a single extension resource replaces its own
// prior contribution (a value change, or dropping a key it no longer
// asserts) with no other contributor left disagreeing.
type AliasConflictKind int

const (
	// AliasConflictValueClaimedByOther means (Alias.Namespace,
	// Alias.Key, Alias.Value) already exists, owned by a platform
	// resource other than the one the report targeted. ActualName
	// identifies the actual owner. This holds regardless of which
	// extension resource(s) contributed the existing claim.
	AliasConflictValueClaimedByOther AliasConflictKind = iota + 1

	// AliasConflictResourceHasDifferentValue means another extension
	// resource currently representing the report's target Name has
	// already contributed a different value for (Alias.Namespace,
	// Alias.Key). ActualValue holds that value. This does not fire
	// when the report's own extension resource previously contributed
	// the old value itself -- that's a replace, not a conflict; see
	// [InventoryReplacement.Aliases].
	AliasConflictResourceHasDifferentValue
)

// AliasConflict reports that an alias submitted alongside an inventory
// report already existed in a way that contradicts the report. See
// [AliasConflictKind].
type AliasConflict struct {
	Alias       Alias
	Kind        AliasConflictKind
	TargetName  ResourceName // the resource name the report targeted.
	ActualName  ResourceName // set when Kind == AliasConflictValueClaimedByOther.
	ActualValue AliasValue   // set when Kind == AliasConflictResourceHasDifferentValue.
}
