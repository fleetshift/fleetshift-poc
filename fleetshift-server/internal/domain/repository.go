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
	// TransitionState compare-and-swaps id's lifecycle state to to when the
	// stored state equals from, without rewriting other columns. It is
	// idempotent when the target is already in to (returns nil). Missing
	// targets return [ErrNotFound]. A target in any other state returns
	// [ErrIllegalStateTransition].
	//
	// When from is [TargetStateReady], an empty stored state is treated
	// as ready so the transition matches the readiness convention used
	// elsewhere.
	TransitionState(ctx context.Context, id TargetID, from, to TargetState) error
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
	// UpdateType persists capability metadata (management / inventory)
	// and updated_at for an existing type. API identity fields
	// (resource type, version, collection) are matched by primary key
	// and are not rewritten beyond what the caller supplies on def.
	UpdateType(ctx context.Context, def ExtensionResourceType) error
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
	// key doc) rather than requiring the row to already exist -- except
	// for [InventoryReplacement.IsDelete] entries, which never
	// resolve-or-create (see that field's doc and
	// [ValidateInventoryReplacements]).
	//
	// TODO: Consider requiring that these validate the type(s) actually
	// have inventory capabilities in their specs. This MUST be doable
	// with at most one additional DB lookup for the whole batch,
	// in that case.
	//
	// ReplaceInventory accepts a mixed batch of replacements and
	// exact-name deletes ([InventoryReplacement.IsDelete]). Non-delete
	// replacements are each treated as the complete latest inventory
	// state for their resource: fields absent from the replacement are
	// cleared/deleted from latest state, with the exception of
	// Observation -- see its field doc. Aliases on non-delete
	// replacements are stored as a pending, unreconciled payload -- see
	// [InventoryReplacement.Aliases]'s doc -- so this never fails or
	// reports a conflict on account of Aliases. Deletes hard-delete the
	// matching extension_resources row (full resource type including
	// type_name, never alias-resolved), treat a missing row as success,
	// and run the same cascade / orphaned alias-claim cleanup as
	// [Delete]. The whole mixed slice is applied together: callers that
	// wrap this call in a transaction get one atomic commit or
	// rollback. Validation errors from
	// [ValidateInventoryReplacements] fail before any write.
	ReplaceInventory(ctx context.Context, replacements []InventoryReplacement) error

	// ApplyInventoryDeltas applies incremental, field-level changes:
	// fields absent from an [InventoryDelta] are left unchanged. Like
	// ReplaceInventory, alias-bearing fields never cause a conflict
	// error -- see [InventoryDelta.UpsertAliases]'s doc.
	// Whole-resource deletion is not expressible here; use
	// [InventoryReplacement.IsDelete] via ReplaceInventory instead.
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
// describing either the complete latest inventory state for a single
// extension resource, or an exact-name whole-resource delete, identified
// by its natural key (ResourceType, Name) rather than an
// [ExtensionResourceUID] resolved ahead of time by the caller. See
// [ExtensionResourceRepository.ReplaceInventory].
//
// When IsDelete is false, the fields below describe the complete latest
// inventory state for the named resource.
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
//
// When IsDelete is true, this command hard-deletes the matching
// extension_resources row by exact ResourceType (including type_name)
// and Name. Deletes never resolve through aliases, never allocate or
// consume CandidateUID, and never create a missing row: a missing
// match is an idempotent success. Payload fields other than
// ResourceType and Name must be empty -- see
// [ValidateInventoryReplacements]. Inventory-only type policy (has
// inventory metadata, no management metadata) is enforced at the
// application layer when type metadata is available there, not by the
// repository.
type InventoryReplacement struct {
	ResourceType ResourceType
	Name         ResourceName
	// IsDelete marks this entry as a whole-resource hard delete rather
	// than an inventory replacement. See this type's doc.
	IsDelete     bool
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
// they get the same Upsert/Delete/Replace shape but with pending-
// payload semantics mirroring [InventoryReplacement.Aliases]'s "this
// is my complete state" replace: UpsertAliases, DeleteAliases, and
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
// Labels and conditions share the same Upsert/Delete/Replace shape as
// aliases (see [InventoryDelta.UpsertLabels],
// [InventoryDelta.DeleteLabels], [InventoryDelta.ReplaceLabels], and
// the matching condition fields). Replace* is mutually exclusive with
// the incremental ops on the same field (see [ValidateInventoryDelta]).
// Nil ReplaceLabels / ReplaceConditions means unchanged; a non-nil
// value (including empty) replaces the entire latest map/set. A delta
// with no field-level changes is a valid heartbeat that still bumps
// resource-level freshness.
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

	// ReplaceLabels, when non-nil (including empty), replaces the
	// entire latest local_labels map. Nil leaves labels unchanged.
	// Mutually exclusive with UpsertLabels and DeleteLabels.
	ReplaceLabels map[string]string
	// UpsertLabels adds or updates the named keys in latest
	// local_labels. Mutually exclusive with
	// [InventoryDelta.ReplaceLabels].
	UpsertLabels map[string]string
	// DeleteLabels removes the named keys from latest local_labels.
	// Mutually exclusive with [InventoryDelta.ReplaceLabels].
	DeleteLabels []string

	Observation *json.RawMessage

	// ReplaceConditions, when non-nil (including empty), replaces the
	// entire latest condition set. Nil leaves conditions unchanged.
	// Mutually exclusive with UpsertConditions and DeleteConditions.
	ReplaceConditions []Condition
	// UpsertConditions adds or updates the named condition types.
	// Mutually exclusive with [InventoryDelta.ReplaceConditions].
	UpsertConditions []Condition
	// DeleteConditions removes the named condition types.
	// Mutually exclusive with [InventoryDelta.ReplaceConditions].
	DeleteConditions []ConditionType

	ObservedAt time.Time
	ReceivedAt time.Time
}

// InventoryResourceRef identifies an inventory-owned extension
// resource by the same natural key shape [InventoryReplacement] and
// [InventoryDelta] use, for
// [ExtensionResourceRepository.DeleteInventoryResources].
type InventoryResourceRef struct {
	ResourceType ResourceType
	Name         ResourceName
}

// InventoryCollectionRef identifies one exact inventory collection for
// one resource type, for
// [ExtensionResourceRepository.PruneInventoryCollection]. Collection is
// a full collection path (e.g. "targets/prod/resourceTypes/widgets/objects"),
// not a prefix.
type InventoryCollectionRef struct {
	ResourceType ResourceType
	Collection   CollectionName
}

// InventorySubtreeRef identifies every inventory resource of one
// resource type whose collection lies below a parent resource-name
// subtree, for
// [ExtensionResourceRepository.DeleteInventorySubtree]. Parent is a
// parsed [ResourceName] such as "targets/prod", not a raw string
// prefix.
type InventorySubtreeRef struct {
	ResourceType ResourceType
	Parent       ResourceName
}

// ValidateInventoryReplacements validates a mixed
// [InventoryReplacement] batch before any repository write:
//
//   - every entry must set ResourceType and Name;
//   - IsDelete entries must not set Aliases, Labels, Observation,
//     Conditions, ObservedAt, ReceivedAt, or CandidateUID;
//   - the same natural key (ResourceType + Name) must not appear more
//     than once as a delete, more than once as a replacement, or as
//     both a delete and a replacement in the same batch.
//
// Both [ExtensionResourceRepository.ReplaceInventory] implementations
// call this for the full slice before partitioning deletes from
// upserts, so contradictory or unsafe shapes never partially apply.
func ValidateInventoryReplacements(replacements []InventoryReplacement) error {
	type keyFate int
	const (
		fateUpsert keyFate = 1 << iota
		fateDelete
	)
	seen := make(map[FullResourceName]keyFate, len(replacements))
	for i, rep := range replacements {
		if rep.ResourceType == "" {
			return fmt.Errorf("%w: replacement %d: ResourceType is required", ErrInvalidArgument, i)
		}
		if rep.Name == "" {
			return fmt.Errorf("%w: replacement %d: Name is required", ErrInvalidArgument, i)
		}
		if rep.IsDelete {
			if rep.Aliases.Len() > 0 {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects non-empty Aliases", ErrInvalidArgument, i)
			}
			if len(rep.Labels) > 0 {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects non-empty Labels", ErrInvalidArgument, i)
			}
			if rep.Observation != nil {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects Observation", ErrInvalidArgument, i)
			}
			if len(rep.Conditions) > 0 {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects non-empty Conditions", ErrInvalidArgument, i)
			}
			if !rep.ObservedAt.IsZero() || !rep.ReceivedAt.IsZero() {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects ObservedAt/ReceivedAt", ErrInvalidArgument, i)
			}
			if !rep.CandidateUID.IsZero() {
				return fmt.Errorf("%w: replacement %d: IsDelete rejects CandidateUID", ErrInvalidArgument, i)
			}
		}
		full := rep.ResourceType.FullName(rep.Name)
		fate := fateUpsert
		if rep.IsDelete {
			fate = fateDelete
		}
		prev, ok := seen[full]
		if !ok {
			seen[full] = fate
			continue
		}
		if prev&fate != 0 {
			kind := "replacement"
			if fate == fateDelete {
				kind = "delete"
			}
			return fmt.Errorf("%w: resource %s appears more than once as a %s in this batch", ErrInvalidArgument, full, kind)
		}
		return fmt.Errorf("%w: resource %s has both a replacement and a delete in this batch", ErrInvalidArgument, full)
	}
	return nil
}

// ValidateInventoryDelta rejects a delta whose label or condition ops
// contradict each other, and rejects any delta that sets
// DeleteAliases or ReplaceAliases at all, since neither is
// implemented against the reported-alias payload yet (see
// [InventoryDelta]'s doc). Rejecting outright, rather than silently
// accepting and ignoring them, means a caller that expects a delete or
// replace to take effect finds out immediately instead of leaving
// stale pending aliases in place with no indication anything went
// wrong.
//
// Label/condition mutual exclusion:
//   - ReplaceLabels may not be combined with UpsertLabels or
//     DeleteLabels
//   - UpsertLabels and DeleteLabels may not name the same key
//   - ReplaceConditions may not be combined with UpsertConditions or
//     DeleteConditions
//   - UpsertConditions and DeleteConditions may not name the same type
//
// These checks can't be left for either backend's ApplyInventoryDeltas
// to resolve on its own: Postgres's applyInventoryDeltasCoreCTEs runs
// a pair's upsert and delete sides as sibling writable CTEs with no
// defined execution order between them when they touch the same
// table, while SQLite's Go orchestration happens to run them as
// ordered sequential statements -- so the very same contradictory
// delta would silently resolve differently per backend if it ever
// reached either one. Both
// [ExtensionResourceRepository.ApplyInventoryDeltas] implementations
// call this for every delta before building any batch argument, so
// every rejection here is always caught in Go before any SQL runs,
// regardless of caller.
func ValidateInventoryDelta(d InventoryDelta) error {
	if d.ReplaceLabels != nil && (len(d.UpsertLabels) > 0 || len(d.DeleteLabels) > 0) {
		return fmt.Errorf("%w: ReplaceLabels cannot be combined with UpsertLabels or DeleteLabels", ErrInvalidArgument)
	}
	deletedLabels := make(map[string]struct{}, len(d.DeleteLabels))
	for _, k := range d.DeleteLabels {
		deletedLabels[k] = struct{}{}
	}
	for k := range d.UpsertLabels {
		if _, ok := deletedLabels[k]; ok {
			return fmt.Errorf("%w: label key %q is present in both UpsertLabels and DeleteLabels", ErrInvalidArgument, k)
		}
	}
	if d.ReplaceConditions != nil && (len(d.UpsertConditions) > 0 || len(d.DeleteConditions) > 0) {
		return fmt.Errorf("%w: ReplaceConditions cannot be combined with UpsertConditions or DeleteConditions", ErrInvalidArgument)
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
// ApplyInventoryDeltas do not call into this repository's aliases;
// a future asynchronous reconciliation process is what will
// eventually decide which reported aliases become accepted here.
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
