package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryReportService is the application-layer entry point for
// inventory reporting. It resolves reporter-supplied identity (name
// and/or aliases) into a [domain.ResourceName], creating the
// underlying [domain.ExtensionResource] as needed, and issues
// natural-key-addressed repository commands. Reporters never need to
// know a [domain.ExtensionResourceUID], and no platform-resource
// identity is ever resolved or minted at this layer at all: a
// platform resource has no UID (see [domain.NewPlatformResource]'s
// doc), so its identity is exactly its [domain.ResourceName] -- a
// struct literal, not a lookup.
//
// The two methods correspond to the two batched reporting modes:
// [InventoryReportService.ReplaceBatch] treats each report as the
// complete latest inventory state, while
// [InventoryReportService.ApplyDeltaBatch] applies incremental,
// field-level changes.
type InventoryReportService struct {
	store     domain.Store
	now       func() time.Time
	chunkSize int
}

// defaultReportChunkSize caps the number of reports resolved and
// written together in a single round of SQL calls. Even a
// pathologically large call is split into chunks of this size, which
// keeps every chunk's multi-row statements safely under Postgres's
// hard per-statement parameter limit (65535) and bounds the cost of
// any one round trip. Chunking only bounds per-statement size: it
// does not change all-or-nothing batch semantics or split the
// transaction -- every chunk runs inside the same transaction and
// commits together (see [reportResolver], which tracks duplicates
// across chunk boundaries).
const defaultReportChunkSize = 1000

// InventoryReportServiceOption configures an [InventoryReportService].
type InventoryReportServiceOption func(*InventoryReportService)

// WithInventoryReportClock overrides the wall-clock used to capture
// ReceivedAt once per batch. Defaults to [time.Now]. A nil fn is
// treated as a no-op to prevent nil-dereference panics at runtime.
func WithInventoryReportClock(fn func() time.Time) InventoryReportServiceOption {
	return func(s *InventoryReportService) {
		if fn != nil {
			s.now = fn
		}
	}
}

// WithInventoryReportChunkSize overrides [defaultReportChunkSize].
// Primarily useful for tests that want to exercise chunk-boundary
// behavior without constructing an enormous batch. n <= 0 is treated
// as a no-op.
func WithInventoryReportChunkSize(n int) InventoryReportServiceOption {
	return func(s *InventoryReportService) {
		if n > 0 {
			s.chunkSize = n
		}
	}
}

// NewInventoryReportService creates a service with the given store and options.
func NewInventoryReportService(store domain.Store, opts ...InventoryReportServiceOption) *InventoryReportService {
	s := &InventoryReportService{
		store:     store,
		now:       time.Now,
		chunkSize: defaultReportChunkSize,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// InventoryReplacementBatchInput is the input for
// [InventoryReportService.ReplaceBatch].
type InventoryReplacementBatchInput struct {
	Reports []InventoryReplacementInput
}

// InventoryReplacementInput describes the complete latest inventory
// state for a single extension resource, identified by resource type
// plus name and/or aliases (never by [domain.ExtensionResourceUID]).
//
// Labels is the complete observed label set; nil and empty both
// normalize to an empty latest label set. Conditions is the complete
// current condition set -- conditions absent from the report are
// deleted from latest state. Observation is the exception to that
// rule: nil, or non-nil pointing to the JSON literal null, leaves the
// latest observation untouched; any other non-nil value replaces the
// latest observation. Neither case appends a history row today -- see
// [domain.ExtensionResourceRepository.ListObservations]'s doc.
type InventoryReplacementInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName
	Aliases      domain.AliasSet

	Labels      map[string]string
	Observation *json.RawMessage
	Conditions  []domain.Condition
	ObservedAt  time.Time
}

// InventoryDeltaBatchInput is the input for
// [InventoryReportService.ApplyDeltaBatch].
type InventoryDeltaBatchInput struct {
	Reports []InventoryDeltaInput
}

// InventoryDeltaInput describes incremental, field-level changes to a
// single extension resource's inventory state, identified by resource
// type plus name and/or aliases (never by [domain.ExtensionResourceUID]).
// Alias identity for *resolving* the report (when Name is nil) is
// drawn from UpsertAliases/ReplaceAliases only -- see
// [reportIdentity]'s construction in ApplyDeltaBatch -- since
// DeleteAliases's whole point is identifying a contribution to retract
// by (namespace, key) alone, with no value to resolve by.
//
// Labels and conditions share the same Upsert/Delete/Replace shape as
// aliases (see [domain.InventoryDelta]). Nil Replace* means unchanged;
// a non-nil value (including empty) replaces the entire latest
// map/set. Replace* is mutually exclusive with the incremental ops on
// the same field. A delta with no field-level changes is a valid
// heartbeat that still bumps resource-level freshness. See
// [domain.InventoryDelta]'s doc for the full Upsert/Delete/Replace
// alias contract this passes straight through.
//
// Observation follows the same pointer semantics as
// [InventoryReplacementInput.Observation]: nil, or non-nil pointing to
// the JSON literal null, leaves the latest observation untouched; any
// other non-nil value replaces latest. Neither case appends a history
// row today.
type InventoryDeltaInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName

	UpsertAliases  domain.AliasSet
	DeleteAliases  []domain.AliasRef
	ReplaceAliases domain.AliasSet

	ReplaceLabels map[string]string
	UpsertLabels  map[string]string
	DeleteLabels  []string

	Observation *json.RawMessage

	ReplaceConditions []domain.Condition
	UpsertConditions  []domain.Condition
	DeleteConditions  []domain.ConditionType

	ObservedAt time.Time
}

// ReplaceBatch resolves identity for every report -- in pure Go for a
// by-Name report, or via one batched alias lookup for an alias-only
// report -- then replaces each resolved resource's latest inventory
// state, using each command's already-canonical [domain.AliasSet]
// directly. The batch is all-or-nothing: a duplicate resolved
// [domain.ResourceName]
// within the batch, a type without inventory metadata, or an
// unresolvable/ambiguous alias-only report fails the whole call
// before any inventory write commits, regardless of how many chunks
// (see [defaultReportChunkSize]) the batch is split across
// internally. Aliases themselves never fail the write: per
// [domain.InventoryReplacement.Aliases]'s doc, reported aliases are
// stored as a pending payload with no synchronous cross-resource
// conflict detection, so two reports (even in the same batch) may
// freely assert the same alias for different resources.
func (s *InventoryReportService) ReplaceBatch(ctx context.Context, in InventoryReplacementBatchInput) error {
	if len(in.Reports) == 0 {
		return nil
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := s.now()
	res := newReportResolver(tx)

	err = forEachReportChunk(len(in.Reports), s.chunkSize, func(start, end int) error {
		chunk := in.Reports[start:end]
		identities := make([]reportIdentity, len(chunk))
		for i, report := range chunk {
			identities[i] = reportIdentity{
				ResourceType: report.ResourceType,
				Name:         report.Name,
				Aliases:      report.Aliases,
			}
		}
		targets, err := res.resolveBatch(ctx, identities, start)
		if err != nil {
			return err
		}

		replacements := make([]domain.InventoryReplacement, len(chunk))
		for i, report := range chunk {
			replacements[i] = domain.InventoryReplacement{
				ResourceType: report.ResourceType,
				Name:         targets[i],
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      identities[i].Aliases,
				Labels:       report.Labels,
				Observation:  report.Observation,
				Conditions:   report.Conditions,
				ObservedAt:   report.ObservedAt,
				ReceivedAt:   now,
			}
		}
		if err := tx.ExtensionResources().ReplaceInventory(ctx, replacements); err != nil {
			return fmt.Errorf("replace inventory: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ApplyDeltaBatch resolves identity for every report the same way
// [InventoryReportService.ReplaceBatch] does, then applies each
// resolved resource's incremental inventory changes, using the
// already-canonical alias sets supplied on each command directly. The
// batch is all-or-nothing: a duplicate resolved [domain.ResourceName] within
// the batch, an internally conflicting report, a type without
// inventory metadata, or an unresolvable/ambiguous alias-only report
// fails the whole call before any inventory write commits, regardless
// of how many chunks (see [defaultReportChunkSize]) the batch is
// split across internally. Like [InventoryReportService.ReplaceBatch],
// alias-bearing fields never fail the write on their own -- see
// [domain.InventoryDelta.UpsertAliases]'s doc.
func (s *InventoryReportService) ApplyDeltaBatch(ctx context.Context, in InventoryDeltaBatchInput) error {
	if len(in.Reports) == 0 {
		return nil
	}

	for _, report := range in.Reports {
		// TODO: this is awkward
		if err := validateDeltaReport(report); err != nil {
			return err
		}
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := s.now()
	res := newReportResolver(tx)

	err = forEachReportChunk(len(in.Reports), s.chunkSize, func(start, end int) error {
		chunk := in.Reports[start:end]
		identities := make([]reportIdentity, len(chunk))
		for i, report := range chunk {
			identities[i] = reportIdentity{
				ResourceType: report.ResourceType,
				Name:         report.Name,
				Aliases:      report.UpsertAliases.Merge(report.ReplaceAliases),
			}
		}
		targets, err := res.resolveBatch(ctx, identities, start)
		if err != nil {
			return err
		}

		deltas := make([]domain.InventoryDelta, len(chunk))
		for i, report := range chunk {
			deltas[i] = domain.InventoryDelta{
				ResourceType:      report.ResourceType,
				Name:              targets[i],
				CandidateUID:      domain.NewExtensionResourceUID(),
				UpsertAliases:     report.UpsertAliases,
				DeleteAliases:     report.DeleteAliases,
				ReplaceAliases:    report.ReplaceAliases,
				ReplaceLabels:     report.ReplaceLabels,
				UpsertLabels:      report.UpsertLabels,
				DeleteLabels:      report.DeleteLabels,
				Observation:       report.Observation,
				ReplaceConditions: report.ReplaceConditions,
				UpsertConditions:  report.UpsertConditions,
				DeleteConditions:  report.DeleteConditions,
				ObservedAt:        report.ObservedAt,
				ReceivedAt:        now,
			}
		}
		if err := tx.ExtensionResources().ApplyInventoryDeltas(ctx, deltas); err != nil {
			return fmt.Errorf("apply inventory deltas: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

// validateDeltaReport catches internally conflicting delta fields
// before any identity resolution or persistence is attempted, per the
// rework doc's delta semantics. It delegates to
// [domain.ValidateInventoryDelta] -- the same check the repository
// layer now also runs -- rather than duplicating it, so failing fast
// here before a single round trip and defending the repository's own
// contract stay backed by exactly one implementation.
func validateDeltaReport(report InventoryDeltaInput) error {
	return domain.ValidateInventoryDelta(domain.InventoryDelta{
		UpsertAliases:     report.UpsertAliases,
		DeleteAliases:     report.DeleteAliases,
		ReplaceAliases:    report.ReplaceAliases,
		ReplaceLabels:     report.ReplaceLabels,
		UpsertLabels:      report.UpsertLabels,
		DeleteLabels:      report.DeleteLabels,
		ReplaceConditions: report.ReplaceConditions,
		UpsertConditions:  report.UpsertConditions,
		DeleteConditions:  report.DeleteConditions,
	})
}

// InventoryDeleteBatchInput is the input for
// [InventoryReportService.DeleteBatch].
type InventoryDeleteBatchInput struct {
	Resources []InventoryDeleteInput
}

// InventoryDeleteInput identifies a single inventory-owned extension
// resource to delete, by resource type and explicit name. Unlike
// [InventoryReplacementInput]/[InventoryDeltaInput], there is no
// alias-only form: deletes are destructive, so identity must be
// exact rather than resolved through the unreconciled reported-alias
// payload (see [domain.InventoryReplacement.Aliases]'s doc).
type InventoryDeleteInput struct {
	ResourceType domain.ResourceType
	Name         domain.ResourceName
}

// DeleteBatch validates that every resource type has inventory
// metadata, rejects a batch that names the same (ResourceType, Name)
// more than once, and hard-deletes every named resource in one
// transaction via
// [domain.ExtensionResourceRepository.DeleteInventoryResources]. A
// resource that doesn't exist is treated as already-deleted, not an
// error -- see that method's doc.
func (s *InventoryReportService) DeleteBatch(ctx context.Context, in InventoryDeleteBatchInput) error {
	if len(in.Resources) == 0 {
		return nil
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	res := newReportResolver(tx)
	refs := make([]domain.InventoryResourceRef, len(in.Resources))
	seen := make(map[domain.FullResourceName]int, len(in.Resources))
	for i, d := range in.Resources {
		if _, err := res.lookupInventoryType(ctx, d.ResourceType); err != nil {
			return err
		}
		full := d.ResourceType.FullName(d.Name)
		if first, dup := seen[full]; dup {
			return fmt.Errorf("%w: resource %s deleted more than once in this batch (entries %d and %d)",
				domain.ErrInvalidArgument, full, first, i)
		}
		seen[full] = i
		refs[i] = domain.InventoryResourceRef{ResourceType: d.ResourceType, Name: d.Name}
	}

	if err := tx.ExtensionResources().DeleteInventoryResources(ctx, refs); err != nil {
		return fmt.Errorf("delete inventory resources: %w", err)
	}
	return tx.Commit()
}

// InventoryCollectionReplacementInput is the input for
// [InventoryReportService.ReplaceCollection]: the complete latest
// contents of one exact inventory collection
type InventoryCollectionReplacementInput struct {
	ResourceType domain.ResourceType
	Collection   domain.CollectionName
	Reports      []InventoryReplacementInput
}

// ReplaceCollection writes every report's complete latest inventory
// state, then deletes any resource previously stored in the exact
// collection that this call's Reports didn't include -- the platform
// primitive for resync: a caller that just LISTed the complete
// contents of a source collection reports the whole set here, and
// anything else stored under that collection is proven absent from
// the source and pruned. Every report must set Name (no alias-only
// reports -- this is a scoped source-of-truth operation, not an
// identity-resolved one) and Name must be inside Collection, i.e.
// [domain.ResourceName.Collection] must equal Collection exactly.
// Aliases, if set, are still passed through to
// [domain.ExtensionResourceRepository.ReplaceInventory] as reported
// metadata; they play no role in selecting which resource belongs to
// the collection. An empty Reports is valid and prunes the entire
// collection, the same as [InventoryReportService.DeleteCollection].
// All writes and the prune happen in one transaction.
func (s *InventoryReportService) ReplaceCollection(ctx context.Context, in InventoryCollectionReplacementInput) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	res := newReportResolver(tx)
	if _, err := res.lookupInventoryType(ctx, in.ResourceType); err != nil {
		return err
	}

	now := s.now()
	replacements := make([]domain.InventoryReplacement, len(in.Reports))
	keepIDs := make([]domain.ResourceID, len(in.Reports))
	for i, report := range in.Reports {
		if report.Name == nil {
			return fmt.Errorf("%w: report %d must set Name", domain.ErrInvalidArgument, i)
		}
		if report.Name.Collection() != in.Collection {
			return fmt.Errorf("%w: report %d name %q is not inside collection %q",
				domain.ErrInvalidArgument, i, *report.Name, in.Collection)
		}
		replacements[i] = domain.InventoryReplacement{
			ResourceType: in.ResourceType,
			Name:         *report.Name,
			CandidateUID: domain.NewExtensionResourceUID(),
			Aliases:      report.Aliases,
			Labels:       report.Labels,
			Observation:  report.Observation,
			Conditions:   report.Conditions,
			ObservedAt:   report.ObservedAt,
			ReceivedAt:   now,
		}
		keepIDs[i] = report.Name.ID()
	}

	if len(replacements) > 0 {
		if err := tx.ExtensionResources().ReplaceInventory(ctx, replacements); err != nil {
			return fmt.Errorf("replace inventory: %w", err)
		}
	}

	scope := domain.InventoryCollectionRef{ResourceType: in.ResourceType, Collection: in.Collection}
	if err := tx.ExtensionResources().PruneInventoryCollection(ctx, scope, keepIDs); err != nil {
		return fmt.Errorf("prune inventory collection: %w", err)
	}

	return tx.Commit()
}

// InventoryCollectionDeleteInput is the input for
// [InventoryReportService.DeleteCollection]: the address of one exact
// inventory collection to remove entirely, e.g. because indexing of a
// Kubernetes target+GVR stopped.
type InventoryCollectionDeleteInput struct {
	ResourceType domain.ResourceType
	Collection   domain.CollectionName
}

// DeleteCollection deletes every resource in the exact named
// collection via
// [domain.ExtensionResourceRepository.PruneInventoryCollection] with
// an explicit empty keep set -- a named, auditable workflow for what
// is, underneath, a prune-with-nothing-to-keep.
func (s *InventoryReportService) DeleteCollection(ctx context.Context, in InventoryCollectionDeleteInput) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	res := newReportResolver(tx)
	if _, err := res.lookupInventoryType(ctx, in.ResourceType); err != nil {
		return err
	}

	scope := domain.InventoryCollectionRef{ResourceType: in.ResourceType, Collection: in.Collection}
	if err := tx.ExtensionResources().PruneInventoryCollection(ctx, scope, []domain.ResourceID{}); err != nil {
		return fmt.Errorf("prune inventory collection: %w", err)
	}
	return tx.Commit()
}

// forEachReportChunk calls fn once per contiguous chunk of at most
// size report indices spanning [0, n), passing each chunk's [start,
// end) bounds. size <= 0 or size >= n runs fn exactly once, covering
// the whole range in a single "chunk." A caller stops iterating and
// propagates the first error fn returns.
func forEachReportChunk(n, size int, fn func(start, end int) error) error {
	if size <= 0 || size > n {
		size = n
	}
	for start := 0; start < n; start += size {
		end := start + size
		if end > n {
			end = n
		}
		if err := fn(start, end); err != nil {
			return err
		}
	}
	return nil
}

// reportIdentity is the identity-resolution-relevant subset shared by
// [InventoryReplacementInput] and [InventoryDeltaInput].
type reportIdentity struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName
	Aliases      domain.AliasSet
}

// reportResolver implements the shared identity/type resolution path
// used by both [InventoryReportService.ReplaceBatch] and
// [InventoryReportService.ApplyDeltaBatch]. Resolving an entire batch
// costs one type lookup per distinct [domain.ResourceType] (cached),
// plus at most one [domain.ResourceIdentityRepository.ResolveAliasesBatch]
// round trip covering every by-Alias-only report in the chunk --
// skipped entirely when the chunk has none. A by-Name report's target
// is its own [domain.ResourceName]: no lookup, no mutation, no SQL at
// all, since a platform resource has no UID to mint or claim (see
// [domain.NewPlatformResource]'s doc) and its representations are
// derived rather than reconciled.
//
// ResolveAliasesBatch only ever sees *accepted* platform identity
// (populated by [domain.PlatformResource.AddAlias] via
// [domain.ResourceIdentityRepository], independent of inventory
// reporting) -- never this same call's own reported Aliases, which
// travel with each report into [domain.InventoryReplacement]/
// [domain.InventoryDelta] purely as a pending payload the repository
// canonicalizes through [domain.AliasSet] before storing (see
// [domain.InventoryReplacement.Aliases]'s doc).
// So an alias-only report only resolves when some other process has
// already accepted that alias; a brand-new alias this same batch is
// the first to report can never resolve a Name-less report to a
// target, by design.
//
// A single resolver instance is reused across every chunk (see
// [defaultReportChunkSize]) of one [InventoryReportService.ReplaceBatch]/
// [InventoryReportService.ApplyDeltaBatch] call, both to keep the type
// cache warm and so duplicate-report detection (seenNames) spans the
// whole call, not just one chunk.
type reportResolver struct {
	tx domain.Tx

	typeCache map[domain.ResourceType]domain.ExtensionResourceType
	seenNames map[domain.FullResourceName]int
}

func newReportResolver(tx domain.Tx) *reportResolver {
	return &reportResolver{
		tx:        tx,
		typeCache: make(map[domain.ResourceType]domain.ExtensionResourceType),
		seenNames: make(map[domain.FullResourceName]int),
	}
}

// resolveBatch resolves every report in items to its target
// [domain.ResourceName], in the same order. baseIndex is items[0]'s
// position within the overall call (0 unless the caller is
// chunking), used only to produce accurate cross-chunk
// duplicate-report error messages. It's all-or-nothing: any
// resolution failure (a type without inventory metadata, a missing
// Name/Aliases, an unresolvable or contradictory alias, or a
// duplicate resolved target anywhere in the call, even in an earlier
// chunk) returns an error before any inventory write for items is
// attempted. None of this is persisted until the caller commits the
// transaction.
func (r *reportResolver) resolveBatch(ctx context.Context, items []reportIdentity, baseIndex int) ([]domain.ResourceName, error) {
	if len(items) == 0 {
		return nil, nil
	}

	for _, in := range items {
		if _, err := r.lookupInventoryType(ctx, in.ResourceType); err != nil {
			return nil, err
		}
		if in.Name == nil && in.Aliases.Len() == 0 {
			return nil, fmt.Errorf("%w: report must set Name or Aliases", domain.ErrInvalidArgument)
		}
	}

	var aliasQuery []domain.Alias
	for _, in := range items {
		if in.Name == nil {
			aliasQuery = append(aliasQuery, in.Aliases.Slice()...)
		}
	}
	aliasResolved, err := r.tx.ResourceIdentities().ResolveAliasesBatch(ctx, aliasQuery)
	if err != nil {
		return nil, fmt.Errorf("resolve aliases batch: %w", err)
	}

	targets := make([]domain.ResourceName, len(items))
	for i, in := range items {
		if in.Name != nil {
			targets[i] = *in.Name
			continue
		}
		target, err := resolveByAliases(in.Aliases, aliasResolved)
		if err != nil {
			return nil, err
		}
		targets[i] = target
	}

	if err := r.rejectDuplicateReports(items, targets, baseIndex); err != nil {
		return nil, err
	}

	return targets, nil
}

// lookupInventoryType resolves and caches an [domain.ExtensionResourceType],
// rejecting types that lack inventory metadata.
//
// TODO: this could be maintained as global cache, potentially always kept up to date as addons are activated/deactivated.
func (r *reportResolver) lookupInventoryType(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	if typeDef, ok := r.typeCache[rt]; ok {
		return typeDef, nil
	}
	typeDef, err := r.tx.ExtensionResources().GetType(ctx, rt)
	if err != nil {
		return domain.ExtensionResourceType{}, fmt.Errorf("lookup type %q: %w", rt, err)
	}
	if typeDef.Inventory() == nil {
		return domain.ExtensionResourceType{}, fmt.Errorf(
			"%w: type %q has no inventory metadata", domain.ErrInvalidArgument, rt)
	}
	r.typeCache[rt] = typeDef
	return typeDef, nil
}

// resolveByAliases picks the single platform resource that every
// alias in aliases agrees on, using resolved (the whole batch's
// [domain.ResourceIdentityRepository.ResolveAliasesBatch] result) to
// look each one up. It never auto-creates an identity -- at least one
// alias must resolve to an existing platform resource, and any two
// that do resolve must agree on the same one.
func resolveByAliases(aliases domain.AliasSet, resolved map[domain.Alias]domain.ResourceName) (domain.ResourceName, error) {
	var target domain.ResourceName
	var found bool
	for alias := range aliases.All() {
		name, ok := resolved[alias]
		if !ok {
			continue
		}
		if found && name != target {
			return "", fmt.Errorf("%w: aliases resolve to different platform resources", domain.ErrInvalidArgument)
		}
		target, found = name, true
	}
	if !found {
		return "", fmt.Errorf("%w: no alias resolved to an existing platform resource", domain.ErrNotFound)
	}
	return target, nil
}

// rejectDuplicateReports catches two reports resolving to the exact
// same extension resource before any inventory-write SQL runs --
// whether because two reports named the same resource directly, or
// because a by-Name and a by-Alias report both resolved to the same
// underlying resource. This is a superset of the old by-Name-only
// check: it runs after every report's target is known, so it catches
// both shapes with one pass. Tracking r.seenNames on the resolver
// itself, rather than locally to this call, catches a duplicate split
// across two different chunks of the same overall batch.
func (r *reportResolver) rejectDuplicateReports(items []reportIdentity, targets []domain.ResourceName, baseIndex int) error {
	for i, in := range items {
		full := in.ResourceType.FullName(targets[i])
		if first, dup := r.seenNames[full]; dup {
			return fmt.Errorf("%w: resource %s reported more than once in this batch (reports %d and %d)",
				domain.ErrInvalidArgument, full, first, baseIndex+i)
		}
		r.seenNames[full] = baseIndex + i
	}
	return nil
}
