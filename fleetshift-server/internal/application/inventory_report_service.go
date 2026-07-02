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
// The two methods correspond to the two batched reporting modes
// described in scratch/inventory_phase4b_report_contract_rework_plan.md:
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
// any one round trip -- see the nameless-platform-identity plan's
// "cost model" section for the round-trip analysis this is based on.
// Chunking only bounds per-statement size: it does not change
// all-or-nothing batch semantics or split the transaction -- every
// chunk runs inside the same transaction and commits together (see
// [reportResolver], which tracks duplicates across chunk boundaries).
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
// latest observation untouched and appends no history row; any other
// non-nil value replaces the latest observation and appends a history
// row (subject to repository-level deduplication against the current
// latest).
type InventoryReplacementInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName
	Aliases      []domain.Alias

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
// Fields left at their zero value are unchanged: SetLabels/DeleteLabels
// only touch the named keys, and UpsertConditions/DeleteConditions only
// touch the named condition types. A delta with no field-level changes
// is a valid heartbeat that still bumps resource-level freshness. See
// [domain.InventoryDelta]'s doc for the full Upsert/Delete/Replace
// alias contract this passes straight through.
//
// Observation follows the same pointer semantics as
// [InventoryReplacementInput.Observation]: nil, or non-nil pointing to
// the JSON literal null, leaves the latest observation untouched and
// appends no history row; any other non-nil value replaces latest and
// appends a history row (subject to the same dedup rule).
type InventoryDeltaInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName

	UpsertAliases  []domain.Alias
	DeleteAliases  []domain.AliasRef
	ReplaceAliases []domain.Alias

	SetLabels    map[string]string
	DeleteLabels []string

	Observation *json.RawMessage

	UpsertConditions []domain.Condition
	DeleteConditions []domain.ConditionType

	ObservedAt time.Time
}

// ReplaceBatch resolves identity for every report -- in pure Go for a
// by-Name report, or via one batched alias lookup for an alias-only
// report -- then replaces each resolved resource's latest inventory
// state, folding any supplied aliases into the same statement. The
// batch is all-or-nothing: a duplicate resolved
// [domain.ResourceName] within the batch, a contradictory alias, a
// type without inventory metadata, or an [domain.AliasConflict]
// returned by the write fails the whole call before any inventory
// write commits, regardless of how many chunks (see
// [defaultReportChunkSize]) the batch is split across internally.
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
				Aliases:      report.Aliases,
				Labels:       report.Labels,
				Observation:  report.Observation,
				Conditions:   report.Conditions,
				ObservedAt:   report.ObservedAt,
				ReceivedAt:   now,
			}
		}
		conflicts, err := tx.ExtensionResources().ReplaceInventory(ctx, replacements)
		if err != nil {
			return fmt.Errorf("replace inventory: %w", err)
		}
		if err := rejectAliasConflicts(conflicts); err != nil {
			return err
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
// resolved resource's incremental inventory changes, folding any
// supplied aliases into the same statement. The batch is
// all-or-nothing: a duplicate resolved [domain.ResourceName] within
// the batch, a contradictory alias, an internally conflicting report,
// a type without inventory metadata, or an [domain.AliasConflict]
// returned by the write fails the whole call before any inventory
// write commits, regardless of how many chunks (see
// [defaultReportChunkSize]) the batch is split across internally.
func (s *InventoryReportService) ApplyDeltaBatch(ctx context.Context, in InventoryDeltaBatchInput) error {
	if len(in.Reports) == 0 {
		return nil
	}

	for _, report := range in.Reports {
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
				Aliases:      deltaIdentityAliases(report),
			}
		}
		targets, err := res.resolveBatch(ctx, identities, start)
		if err != nil {
			return err
		}

		deltas := make([]domain.InventoryDelta, len(chunk))
		for i, report := range chunk {
			deltas[i] = domain.InventoryDelta{
				ResourceType:     report.ResourceType,
				Name:             targets[i],
				CandidateUID:     domain.NewExtensionResourceUID(),
				UpsertAliases:    report.UpsertAliases,
				DeleteAliases:    report.DeleteAliases,
				ReplaceAliases:   report.ReplaceAliases,
				SetLabels:        report.SetLabels,
				DeleteLabels:     report.DeleteLabels,
				Observation:      report.Observation,
				UpsertConditions: report.UpsertConditions,
				DeleteConditions: report.DeleteConditions,
				ObservedAt:       report.ObservedAt,
				ReceivedAt:       now,
			}
		}
		conflicts, err := tx.ExtensionResources().ApplyInventoryDeltas(ctx, deltas)
		if err != nil {
			return fmt.Errorf("apply inventory deltas: %w", err)
		}
		if err := rejectAliasConflicts(conflicts); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

// rejectAliasConflicts turns a non-empty []domain.AliasConflict from
// ReplaceInventory/ApplyInventoryDeltas into an error, which the
// caller's deferred tx.Rollback() then applies to the whole batch --
// an alias conflict is rejected exactly like an inventory-level error
// would be, not partially applied.
func rejectAliasConflicts(conflicts []domain.AliasConflict) error {
	if len(conflicts) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d alias(es) conflict with existing or concurrently reported identity: %+v",
		domain.ErrInvalidArgument, len(conflicts), conflicts)
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
		UpsertAliases:    report.UpsertAliases,
		DeleteAliases:    report.DeleteAliases,
		ReplaceAliases:   report.ReplaceAliases,
		SetLabels:        report.SetLabels,
		DeleteLabels:     report.DeleteLabels,
		UpsertConditions: report.UpsertConditions,
		DeleteConditions: report.DeleteConditions,
	})
}

// deltaIdentityAliases returns the aliases eligible to resolve
// report's target when Name is nil: UpsertAliases and ReplaceAliases,
// both of which assert a (namespace, key, value) triple a resource can
// be looked up by, but never DeleteAliases -- an [domain.AliasRef] has
// no value to resolve by, precisely because retracting a contribution
// doesn't require knowing it (see [domain.AliasRef]'s doc). A delta
// that only wants to retract an alias must identify its target by
// Name.
func deltaIdentityAliases(report InventoryDeltaInput) []domain.Alias {
	if len(report.ReplaceAliases) == 0 {
		return report.UpsertAliases
	}
	out := make([]domain.Alias, 0, len(report.UpsertAliases)+len(report.ReplaceAliases))
	out = append(out, report.UpsertAliases...)
	out = append(out, report.ReplaceAliases...)
	return out
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
	Aliases      []domain.Alias
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
// [domain.NewPlatformResource]'s doc) and its representations/aliases
// are derived/additive rather than reconciled. Aliases themselves are
// never upserted here -- every report's Aliases travel with it into
// [domain.InventoryReplacement]/[domain.InventoryDelta], and the
// repository folds their upsert into the same statement that writes
// inventory (see extension_resource_repo.go's alias fold-in).
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
		if in.Name == nil && len(in.Aliases) == 0 {
			return nil, fmt.Errorf("%w: report must set Name or Aliases", domain.ErrInvalidArgument)
		}
	}

	var aliasQuery []domain.Alias
	for _, in := range items {
		if in.Name == nil {
			aliasQuery = append(aliasQuery, in.Aliases...)
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
func resolveByAliases(aliases []domain.Alias, resolved map[domain.Alias]domain.ResourceName) (domain.ResourceName, error) {
	var target domain.ResourceName
	var found bool
	for _, alias := range aliases {
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
