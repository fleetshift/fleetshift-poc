package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryReportService is the application-layer entry point for
// inventory reporting. It resolves reporter-supplied identity (name
// and/or aliases) into a [domain.PlatformResource] and
// [domain.ExtensionResource], creating either if needed, and then
// issues UID-addressed repository commands. Reporters never need to
// know an [domain.ExtensionResourceUID].
//
// The two methods correspond to the two batched reporting modes
// described in scratch/inventory_phase4b_report_contract_rework_plan.md:
// [InventoryReportService.ReplaceBatch] treats each report as the
// complete latest inventory state, while
// [InventoryReportService.ApplyDeltaBatch] applies incremental,
// field-level changes.
type InventoryReportService struct {
	store domain.Store
	now   func() time.Time
}

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

// NewInventoryReportService creates a service with the given store and options.
func NewInventoryReportService(store domain.Store, opts ...InventoryReportServiceOption) *InventoryReportService {
	s := &InventoryReportService{
		store: store,
		now:   time.Now,
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
//
// Fields left at their zero value are unchanged: SetLabels/DeleteLabels
// only touch the named keys, and UpsertConditions/DeleteConditions only
// touch the named condition types. A delta with no field-level changes
// is a valid heartbeat that still bumps resource-level freshness.
//
// Observation follows the same pointer semantics as
// [InventoryReplacementInput.Observation]: nil, or non-nil pointing to
// the JSON literal null, leaves the latest observation untouched and
// appends no history row; any other non-nil value replaces latest and
// appends a history row (subject to the same dedup rule).
type InventoryDeltaInput struct {
	ResourceType domain.ResourceType
	Name         *domain.ResourceName
	Aliases      []domain.Alias

	SetLabels    map[string]string
	DeleteLabels []string

	Observation *json.RawMessage

	UpsertConditions []domain.Condition
	DeleteConditions []domain.ConditionType

	ObservedAt time.Time
}

// ReplaceBatch resolves identity for every report, creating platform
// and extension resources as needed, then replaces each resolved
// resource's latest inventory state in a single transaction. The
// batch is all-or-nothing: a duplicate resolved
// [domain.ExtensionResourceUID] within the batch, a contradictory
// alias, or a type without inventory metadata fails the whole call
// before any inventory write.
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
	replacements := make([]domain.InventoryReplacement, 0, len(in.Reports))

	for _, report := range in.Reports {
		uid, err := res.resolve(ctx, reportIdentity{
			ResourceType: report.ResourceType,
			Name:         report.Name,
			Aliases:      report.Aliases,
		}, now)
		if err != nil {
			return err
		}

		replacements = append(replacements, domain.InventoryReplacement{
			ExtensionResourceUID: uid,
			Labels:               report.Labels,
			Observation:          report.Observation,
			Conditions:           report.Conditions,
			ObservedAt:           report.ObservedAt,
			ReceivedAt:           now,
		})
	}

	if err := tx.ExtensionResources().ReplaceInventory(ctx, replacements); err != nil {
		return fmt.Errorf("replace inventory: %w", err)
	}

	if err := res.persistTouchedIdentities(ctx); err != nil {
		return err
	}

	return tx.Commit()
}

// ApplyDeltaBatch resolves identity for every report, creating
// platform and extension resources as needed, then applies each
// resolved resource's incremental inventory changes in a single
// transaction. The batch is all-or-nothing: a duplicate resolved
// [domain.ExtensionResourceUID] within the batch, a contradictory
// alias, an internally conflicting report, or a type without
// inventory metadata fails the whole call before any inventory write.
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
	deltas := make([]domain.InventoryDelta, 0, len(in.Reports))

	for _, report := range in.Reports {
		uid, err := res.resolve(ctx, reportIdentity{
			ResourceType: report.ResourceType,
			Name:         report.Name,
			Aliases:      report.Aliases,
		}, now)
		if err != nil {
			return err
		}

		deltas = append(deltas, domain.InventoryDelta{
			ExtensionResourceUID: uid,
			SetLabels:            report.SetLabels,
			DeleteLabels:         report.DeleteLabels,
			Observation:          report.Observation,
			UpsertConditions:     report.UpsertConditions,
			DeleteConditions:     report.DeleteConditions,
			ObservedAt:           report.ObservedAt,
			ReceivedAt:           now,
		})
	}

	if err := tx.ExtensionResources().ApplyInventoryDeltas(ctx, deltas); err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}

	if err := res.persistTouchedIdentities(ctx); err != nil {
		return err
	}

	return tx.Commit()
}

// validateDeltaReport catches internally conflicting delta fields
// before any identity resolution or persistence is attempted, per the
// rework doc's delta semantics.
func validateDeltaReport(report InventoryDeltaInput) error {
	for _, k := range report.DeleteLabels {
		if _, ok := report.SetLabels[k]; ok {
			return fmt.Errorf("%w: label %q is present in both SetLabels and DeleteLabels", domain.ErrInvalidArgument, k)
		}
	}
	deleted := make(map[domain.ConditionType]struct{}, len(report.DeleteConditions))
	for _, t := range report.DeleteConditions {
		deleted[t] = struct{}{}
	}
	for _, c := range report.UpsertConditions {
		if _, ok := deleted[c.Type()]; ok {
			return fmt.Errorf("%w: condition type %q is present in both UpsertConditions and DeleteConditions", domain.ErrInvalidArgument, c.Type())
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

// reportResolver implements the shared identity/type/representation
// resolution path used by both [InventoryReportService.ReplaceBatch]
// and [InventoryReportService.ApplyDeltaBatch]. It caches looked-up
// types, tracks every platform resource touched so callers persist
// each exactly once, and rejects a resolved
// [domain.ExtensionResourceUID] that repeats within the same batch.
type reportResolver struct {
	tx domain.Tx

	typeCache map[domain.ResourceType]domain.ExtensionResourceType
	touched   map[domain.PlatformResourceUID]*domain.PlatformResource
	resolved  map[domain.ExtensionResourceUID]struct{}
}

func newReportResolver(tx domain.Tx) *reportResolver {
	return &reportResolver{
		tx:        tx,
		typeCache: make(map[domain.ResourceType]domain.ExtensionResourceType),
		touched:   make(map[domain.PlatformResourceUID]*domain.PlatformResource),
		resolved:  make(map[domain.ExtensionResourceUID]struct{}),
	}
}

// resolve resolves in to an [domain.ExtensionResourceUID], claiming or
// loading the platform resource, getting or creating the extension
// resource, and attaching the representation and any supplied
// aliases. None of this is persisted until the caller commits the
// transaction.
func (r *reportResolver) resolve(ctx context.Context, in reportIdentity, now time.Time) (domain.ExtensionResourceUID, error) {
	typeDef, err := r.lookupInventoryType(ctx, in.ResourceType)
	if err != nil {
		return domain.ExtensionResourceUID{}, err
	}

	pr, err := r.resolveIdentity(ctx, in, now)
	if err != nil {
		return domain.ExtensionResourceUID{}, err
	}

	fullName := in.ResourceType.FullName(pr.Name())
	er, err := r.tx.ExtensionResources().Get(ctx, fullName)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		er = domain.NewExtensionResource(domain.NewExtensionResourceUID(), in.ResourceType, pr.Name(), now)
		if err := r.tx.ExtensionResources().Create(ctx, er); err != nil {
			return domain.ExtensionResourceUID{}, fmt.Errorf("create extension resource %s: %w", fullName, err)
		}
	case err != nil:
		return domain.ExtensionResourceUID{}, fmt.Errorf("get extension resource %s: %w", fullName, err)
	}

	if _, dup := r.resolved[er.UID()]; dup {
		return domain.ExtensionResourceUID{}, fmt.Errorf(
			"%w: extension resource %s reported more than once in this batch", domain.ErrInvalidArgument, er.UID())
	}
	r.resolved[er.UID()] = struct{}{}

	if err := pr.AttachRepresentation(domain.AttachRepresentationInput{
		ServiceName:          typeDef.APIServiceName(),
		Version:              typeDef.APIVersion(),
		ExtensionResourceUID: er.UID(),
	}, now); err != nil {
		return domain.ExtensionResourceUID{}, fmt.Errorf("attach representation: %w", err)
	}
	r.touched[pr.UID()] = pr

	return er.UID(), nil
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

// resolveIdentity claims or loads the platform resource named or
// aliased by in, then attaches any additional supplied aliases via
// [domain.PlatformResource.AddAlias] before any persistence so a
// contradicting alias fails the whole call before writes. A
// newly-claimed platform resource is seeded with no labels: reporter-
// observed labels belong on the [domain.InventoryResource] (handled
// separately via ReplaceInventory/ApplyInventoryDeltas), not on the
// platform-level identity, which has its own separate user-managed
// label concept.
func (r *reportResolver) resolveIdentity(ctx context.Context, in reportIdentity, now time.Time) (*domain.PlatformResource, error) {
	repo := r.tx.ResourceIdentities()

	var pr *domain.PlatformResource
	switch {
	case in.Name != nil:
		var err error
		pr, err = domain.ClaimOrGetIdentity(ctx, repo, *in.Name, nil, now)
		if err != nil {
			return nil, fmt.Errorf("claim identity %s: %w", *in.Name, err)
		}
	case len(in.Aliases) > 0:
		var err error
		pr, err = r.resolveByAliases(ctx, in.Aliases)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: report must set Name or Aliases", domain.ErrInvalidArgument)
	}

	for _, alias := range in.Aliases {
		if err := pr.AddAlias(alias); err != nil {
			return nil, fmt.Errorf("add alias %+v: %w", alias, err)
		}
	}

	return pr, nil
}

// resolveByAliases resolves a platform resource purely from aliases,
// requiring that every alias that does resolve agrees on the same
// [domain.PlatformResourceUID]. It never auto-creates an identity --
// at least one alias must resolve to an existing platform resource.
func (r *reportResolver) resolveByAliases(ctx context.Context, aliases []domain.Alias) (*domain.PlatformResource, error) {
	repo := r.tx.ResourceIdentities()

	var resolved domain.PlatformResourceUID
	var found bool
	for _, alias := range aliases {
		uid, err := repo.ResolveAlias(ctx, alias)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve alias %+v: %w", alias, err)
		}
		if found && uid != resolved {
			return nil, fmt.Errorf("%w: aliases resolve to different platform resources", domain.ErrInvalidArgument)
		}
		resolved, found = uid, true
	}
	if !found {
		return nil, fmt.Errorf("%w: no alias resolved to an existing platform resource", domain.ErrNotFound)
	}

	pr, err := repo.Get(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("get platform resource %s: %w", resolved, err)
	}
	return pr, nil
}

// persistTouchedIdentities flushes every platform resource touched
// during resolution, exactly once each.
func (r *reportResolver) persistTouchedIdentities(ctx context.Context) error {
	for _, pr := range r.touched {
		if err := r.tx.ResourceIdentities().Update(ctx, pr); err != nil {
			return fmt.Errorf("update platform resource %s: %w", pr.UID(), err)
		}
	}
	return nil
}
