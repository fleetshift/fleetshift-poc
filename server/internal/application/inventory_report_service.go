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

// InventoryReplacementInput describes either the complete latest
// inventory state for a single extension resource, or an exact-name
// whole-resource delete, identified by resource type plus name and/or
// aliases (never by [domain.ExtensionResourceUID]).
//
// When IsDelete is false, Labels is the complete observed label set;
// nil and empty both normalize to an empty latest label set.
// Conditions is the complete current condition set -- conditions
// absent from the report are deleted from latest state. Observation is
// the exception to that rule: nil, or non-nil pointing to the JSON
// literal null, leaves the latest observation untouched; any other
// non-nil value replaces the latest observation. Neither case appends
// a history row today -- see
// [domain.ExtensionResourceRepository.ListObservations]'s doc.
//
// When IsDelete is true, Name and ResourceType are required (no
// alias-only identity), payload fields must be empty, and the type
// must be inventory-only (inventory metadata, no management
// metadata). See [InventoryReportService.ReplaceBatch].
type InventoryReplacementInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName
	Aliases      domain.AliasSet
	// IsDelete marks this entry as a whole-resource hard delete rather
	// than an inventory replacement. See this type's doc.
	IsDelete bool

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
// directly. Entries with [InventoryReplacementInput.IsDelete] true are
// exact-name hard deletes: they never resolve aliases, never allocate
// CandidateUID, reject non-empty payload fields, and are allowed only
// for inventory-only types. Upserts and deletes in the same call are
// one mixed [domain.ExtensionResourceRepository.ReplaceInventory]
// batch (chunked only for statement size) inside one transaction.
//
// The batch is all-or-nothing: a duplicate resolved
// [domain.ResourceName] within the batch (including contradictory
// delete+replacement for the same key, even across chunk boundaries),
// a type without inventory metadata, a delete against a
// managed-plus-inventory type, or an unresolvable/ambiguous alias-only
// report fails the whole call before any inventory write commits,
// regardless of how many chunks (see [defaultReportChunkSize]) the
// batch is split across internally. Aliases on non-delete reports
// never fail the write: per [domain.InventoryReplacement.Aliases]'s
// doc, reported aliases are stored as a pending payload with no
// synchronous cross-resource conflict detection, so two reports (even
// in the same batch) may freely assert the same alias for different
// resources.
func (s *InventoryReportService) ReplaceBatch(ctx context.Context, in InventoryReplacementBatchInput) error {
	if len(in.Reports) == 0 {
		return nil
	}

	for i, report := range in.Reports {
		if report.IsDelete {
			if err := validateDeleteReplacementInput(i, report); err != nil {
				return err
			}
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
				Aliases:      report.Aliases,
				IsDelete:     report.IsDelete,
			}
		}
		targets, err := res.resolveBatch(ctx, identities, start)
		if err != nil {
			return err
		}

		replacements := make([]domain.InventoryReplacement, len(chunk))
		for i, report := range chunk {
			if report.IsDelete {
				replacements[i] = domain.InventoryReplacement{
					ResourceType: report.ResourceType,
					Name:         targets[i],
					IsDelete:     true,
				}
				continue
			}
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

// validateDeltaReport rejects internally conflicting delta fields
// before identity resolution or persistence. It delegates to
// [domain.ValidateInventoryDelta], which [ApplyInventoryDeltas] also
// runs.
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

// validateDeleteReplacementInput rejects unsafe whole-resource delete
// shapes before identity resolution or persistence. Name must be
// non-nil on the application DTO (a nil pointer has no domain
// equivalent). Remaining delete rules are enforced by
// [domain.ValidateInventoryReplacements], which [ReplaceInventory] also
// runs.
func validateDeleteReplacementInput(i int, report InventoryReplacementInput) error {
	if report.Name == nil {
		return fmt.Errorf("%w: report %d: IsDelete requires Name", domain.ErrInvalidArgument, i)
	}
	return domain.ValidateInventoryReplacements([]domain.InventoryReplacement{{
		ResourceType: report.ResourceType,
		Name:         *report.Name,
		IsDelete:     true,
		Aliases:      report.Aliases,
		Labels:       report.Labels,
		Observation:  report.Observation,
		Conditions:   report.Conditions,
		ObservedAt:   report.ObservedAt,
	}})
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
	IsDelete     bool
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
// resolution failure (a type without inventory metadata, a delete
// against a managed-plus-inventory type, a missing Name/Aliases, an
// unresolvable or contradictory alias, or a duplicate resolved target
// anywhere in the call, even in an earlier chunk) returns an error
// before any inventory write for items is attempted. None of this is
// persisted until the caller commits the transaction.
func (r *reportResolver) resolveBatch(ctx context.Context, items []reportIdentity, baseIndex int) ([]domain.ResourceName, error) {
	if len(items) == 0 {
		return nil, nil
	}

	for _, in := range items {
		if in.IsDelete {
			if _, err := r.lookupInventoryOnlyType(ctx, in.ResourceType); err != nil {
				return nil, err
			}
			if in.Name == nil {
				return nil, fmt.Errorf("%w: IsDelete report must set Name", domain.ErrInvalidArgument)
			}
			continue
		}
		if _, err := r.lookupInventoryType(ctx, in.ResourceType); err != nil {
			return nil, err
		}
		if in.Name == nil && in.Aliases.Len() == 0 {
			return nil, fmt.Errorf("%w: report must set Name or Aliases", domain.ErrInvalidArgument)
		}
	}

	var aliasQuery []domain.Alias
	for _, in := range items {
		if !in.IsDelete && in.Name == nil {
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

// lookupInventoryOnlyType is [lookupInventoryType] plus the
// whole-resource-delete policy: the type must not also have management
// metadata. Managed-plus-inventory hard delete is intentionally not
// available through ordinary inventory reporting.
func (r *reportResolver) lookupInventoryOnlyType(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	typeDef, err := r.lookupInventoryType(ctx, rt)
	if err != nil {
		return domain.ExtensionResourceType{}, err
	}
	if typeDef.Management() != nil {
		return domain.ExtensionResourceType{}, fmt.Errorf(
			"%w: type %q has management metadata; whole-resource delete is allowed only for inventory-only types",
			domain.ErrInvalidArgument, rt)
	}
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
// underlying resource, or because a delete and a replacement target
// the same natural key. This is a superset of the old by-Name-only
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
