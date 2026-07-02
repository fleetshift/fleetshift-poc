package application_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"context"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const inventoryReportTestType domain.ResourceType = "kind.fleetshift.io/Cluster"

// seedInventoryType registers an extension resource type that
// supports inventory reporting (and nothing else).
func seedInventoryType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	def := domain.NewExtensionResourceType(inventoryReportTestType, "v1", "clusters", time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// seedManagedOnlyType registers an extension resource type that
// supports management but has no inventory metadata.
func seedManagedOnlyType(t *testing.T, store domain.Store, rt domain.ResourceType) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	relation := domain.NewRegisteredSelfTarget(domain.TargetID("addon-widget"), domain.ManifestType("api.test.widget"))
	def := domain.NewExtensionResourceType(rt, "v1", "widgets", time.Now(),
		domain.WithManagement(relation, domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		}))
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func getExtensionResource(t *testing.T, store domain.Store, name domain.ResourceName) *domain.ExtensionResource {
	t.Helper()
	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	er, err := tx.ExtensionResources().Get(ctx, inventoryReportTestType.FullName(name))
	if err != nil {
		t.Fatalf("Get(%s): %v", name, err)
	}
	return er
}

func mustCondition(t *testing.T, ct domain.ConditionType, status domain.ConditionStatus, reason, message string, transitionTime time.Time) domain.Condition {
	t.Helper()
	c, err := domain.NewCondition(ct, status, reason, message, transitionTime)
	if err != nil {
		t.Fatalf("NewCondition: %v", err)
	}
	return c
}

func rawMsg(s string) *json.RawMessage {
	r := json.RawMessage(s)
	return &r
}

func TestInventoryReportService_ReplaceBatch_ByName_CreatesIdentityAndInventory(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cond := mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "all good", observedAt)

	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Name:         &name,
			Labels:       map[string]string{"env": "prod"},
			Observation:  rawMsg(`{"cpu":"4"}`),
			Conditions:   []domain.Condition{cond},
			ObservedAt:   observedAt,
		}},
	})
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}

	// Platform identity created with a representation pointing at the
	// extension resource.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		tx.Rollback()
		t.Fatalf("GetByName: %v", err)
	}
	if len(pr.Representations()) != 1 {
		t.Fatalf("Representations len = %d, want 1", len(pr.Representations()))
	}
	if pr.Representations()[0].ServiceName() != "kind.fleetshift.io" {
		t.Errorf("representation service = %q, want kind.fleetshift.io", pr.Representations()[0].ServiceName())
	}
	tx.Rollback()

	// Extension resource exists with the resolved inventory state.
	er := getExtensionResource(t, store, name)
	inv := er.Inventory()
	if inv == nil {
		t.Fatal("Inventory() is nil")
	}
	if inv.Labels()["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want prod", inv.Labels()["env"])
	}
	if inv.Observation() == nil || string(*inv.Observation()) != `{"cpu":"4"}` {
		t.Errorf("Observation = %v, want {\"cpu\":\"4\"}", inv.Observation())
	}
	if len(inv.Conditions()) != 1 || inv.Conditions()[0].Type() != "Ready" {
		t.Fatalf("Conditions = %+v, want one Ready condition", inv.Conditions())
	}

	// Observation history and condition transition were recorded.
	tx2, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx2.Rollback()
	obs, err := tx2.ExtensionResources().ListObservations(ctx, er.UID(), 10)
	if err != nil {
		t.Fatalf("ListObservations: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("ListObservations len = %d, want 1", len(obs))
	}
	transitions, err := tx2.ExtensionResources().ListConditionTransitions(ctx, er.UID(), nil, 10)
	if err != nil {
		t.Fatalf("ListConditionTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("ListConditionTransitions len = %d, want 1", len(transitions))
	}
}

func TestInventoryReportService_ReplaceBatch_ByNamePlusAlias_AttachesAliasAtomically(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	alias, err := domain.NewAlias("gcp", "project_id", "my-project-123")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Name:         &name,
			Aliases:      []domain.Alias{alias},
			ObservedAt:   time.Now(),
		}},
	})
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if len(pr.Aliases()) != 1 || pr.Aliases()[0] != alias {
		t.Fatalf("Aliases = %+v, want [%+v]", pr.Aliases(), alias)
	}
}

func TestInventoryReportService_ReplaceBatch_AliasesOnly_ResolvesExistingIdentity(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	alias, err := domain.NewAlias("gcp", "project_id", "my-project-123")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	// First report establishes the identity (by name) and the alias.
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Name:         &name,
			Aliases:      []domain.Alias{alias},
			Observation:  rawMsg(`{"v":1}`),
			ObservedAt:   time.Now(),
		}},
	}); err != nil {
		t.Fatalf("first ReplaceBatch: %v", err)
	}
	first := getExtensionResource(t, store, name)

	// Second report resolves purely by alias and should update the
	// SAME extension resource, not create a new one.
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Aliases:      []domain.Alias{alias},
			Observation:  rawMsg(`{"v":2}`),
			ObservedAt:   time.Now(),
		}},
	}); err != nil {
		t.Fatalf("second ReplaceBatch: %v", err)
	}

	second := getExtensionResource(t, store, name)
	if second.UID() != first.UID() {
		t.Fatalf("UID changed across alias-only report: first=%s second=%s", first.UID(), second.UID())
	}
	if string(*second.Inventory().Observation()) != `{"v":2}` {
		t.Errorf("Observation = %s, want {\"v\":2}", *second.Inventory().Observation())
	}
}

func TestInventoryReportService_ReplaceBatch_AliasesOnly_NoMatchRejected(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	alias, err := domain.NewAlias("gcp", "project_id", "unregistered-project")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Aliases:      []domain.Alias{alias},
			ObservedAt:   time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("ReplaceBatch err = %v, want ErrNotFound", err)
	}
}

func TestInventoryReportService_ReplaceBatch_AliasesOnly_ContradictoryAliasesFail(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name1 := domain.ResourceName("clusters/c1")
	name2 := domain.ResourceName("clusters/c2")
	aliasA, err := domain.NewAlias("gcp", "project_id", "project-a")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	aliasB, err := domain.NewAlias("aws", "account_id", "account-b")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name1, Aliases: []domain.Alias{aliasA}, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed report 1: %v", err)
	}
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name2, Aliases: []domain.Alias{aliasB}, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed report 2: %v", err)
	}

	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Aliases:      []domain.Alias{aliasA, aliasB},
			Observation:  rawMsg(`{"should":"not persist"}`),
			ObservedAt:   time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// Neither resource's inventory should have been touched.
	er1 := getExtensionResource(t, store, name1)
	if er1.Inventory().Observation() != nil {
		t.Errorf("c1 Observation = %v, want nil (untouched)", er1.Inventory().Observation())
	}
	er2 := getExtensionResource(t, store, name2)
	if er2.Inventory().Observation() != nil {
		t.Errorf("c2 Observation = %v, want nil (untouched)", er2.Inventory().Observation())
	}
}

func TestInventoryReportService_ReplaceBatch_CrossReportNewAliasContradictionRejected(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name1 := domain.ResourceName("clusters/c1")
	name2 := domain.ResourceName("clusters/c2")
	contested, err := domain.NewAlias("gcp", "project_id", "contested-project")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	// Two different, brand-new reports in the same batch both claim
	// the very same never-before-seen alias for different resources.
	// A single multi-row INSERT ON CONFLICT DO NOTHING can't detect
	// this on its own (neither row conflicts with anything that
	// existed before the statement started), so this must be caught
	// in Go before any SQL is issued.
	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name1, Aliases: []domain.Alias{contested}, ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name2, Aliases: []domain.Alias{contested}, ObservedAt: time.Now()},
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// No partial writes: the whole transaction must have rolled back,
	// including the platform identities that would otherwise have
	// been claimed before the alias contradiction was discovered.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ResourceIdentities().GetByName(ctx, name1); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetByName(%s) err = %v, want ErrNotFound (no partial write)", name1, err)
	}
	if _, err := tx.ResourceIdentities().GetByName(ctx, name2); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetByName(%s) err = %v, want ErrNotFound (no partial write)", name2, err)
	}
}

// TestInventoryReportService_ReplaceBatch_CrossChunkAliasContradictionRejected
// is CrossReportNewAliasContradictionRejected's harder sibling: the
// two contradicting reports land in *different* chunks of the same
// batch/transaction (forced via a chunk size of 1), so
// reportResolver's per-chunk checkAliasBatchConsistency-equivalent
// (scoped to whatever aliasCandidates its own chunk assembled) never
// sees both claims at once and cannot catch the contradiction in Go.
// It must still be caught -- this time by the second chunk's SQL
// seeing the first chunk's alias row, already written earlier in the
// very same transaction -- and still roll back the whole batch.
func TestInventoryReportService_ReplaceBatch_CrossChunkAliasContradictionRejected(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store, application.WithInventoryReportChunkSize(1))
	ctx := context.Background()

	name1 := domain.ResourceName("clusters/cc1")
	name2 := domain.ResourceName("clusters/cc2")
	contested, err := domain.NewAlias("gcp", "project_id", "cross-chunk-contested")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name1, Aliases: []domain.Alias{contested}, ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name2, Aliases: []domain.Alias{contested}, ObservedAt: time.Now()},
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// No partial writes: chunk 1 committing nothing until the whole
	// transaction commits is what makes this catchable at all, so
	// this is also a check that the guarantee actually holds.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ResourceIdentities().GetByName(ctx, name1); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetByName(%s) err = %v, want ErrNotFound (no partial write)", name1, err)
	}
	if _, err := tx.ResourceIdentities().GetByName(ctx, name2); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetByName(%s) err = %v, want ErrNotFound (no partial write)", name2, err)
	}
}

func TestInventoryReportService_ReplaceBatch_CrossReportSameKeyDifferentValueRejected(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	zoneA, err := domain.NewAlias("gcp", "zone", "us-central1-a")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	zoneB, err := domain.NewAlias("gcp", "zone", "us-central1-b")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	// A single report asserting two different values for the same
	// (namespace, key) is internally contradictory.
	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name, Aliases: []domain.Alias{zoneA, zoneB}, ObservedAt: time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}
}

func TestInventoryReportService_ReplaceBatch_LargeMixedBatchResolvesEveryReport(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	// A deliberately small, non-round chunk size forces this batch
	// across many chunk boundaries (~43 chunks for 300 reports),
	// exercising forEachReportChunk without needing a batch large
	// enough to hit the real default chunk size.
	svc := application.NewInventoryReportService(store, application.WithInventoryReportChunkSize(7))
	ctx := context.Background()

	const total = 300
	const preexistingEvery = 4 // every 4th resource is pre-seeded (by name) before the batch
	const aliasEvery = 5       // every 5th resource is additionally resolved via a pre-seeded alias

	names := make([]domain.ResourceName, total)
	aliasFor := make(map[int]domain.Alias)
	for i := 0; i < total; i++ {
		names[i] = domain.ResourceName(fmt.Sprintf("clusters/mixed-%03d", i))
	}

	// Pre-seed every preexistingEvery'th resource (and, for a subset
	// of those, an alias) via its own prior ReplaceBatch call, before
	// the single large batch under test runs.
	for i := 0; i < total; i += preexistingEvery {
		report := application.InventoryReplacementInput{
			ResourceType: inventoryReportTestType,
			Name:         &names[i],
			Observation:  rawMsg(`{"seed":true}`),
			ObservedAt:   time.Now(),
		}
		if i%aliasEvery == 0 {
			alias, err := domain.NewAlias("gcp", "project_id", domain.AliasValue(fmt.Sprintf("proj-%03d", i)))
			if err != nil {
				t.Fatalf("NewAlias: %v", err)
			}
			report.Aliases = []domain.Alias{alias}
			aliasFor[i] = alias
		}
		if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{Reports: []application.InventoryReplacementInput{report}}); err != nil {
			t.Fatalf("seed report %d: %v", i, err)
		}
	}

	// Build one large batch: resources with a pre-seeded alias
	// resolve purely by alias; everything else resolves by name,
	// whether or not it already exists.
	reports := make([]application.InventoryReplacementInput, total)
	for i := 0; i < total; i++ {
		observation := rawMsg(fmt.Sprintf(`{"v":%d}`, i))
		if alias, ok := aliasFor[i]; ok {
			reports[i] = application.InventoryReplacementInput{
				ResourceType: inventoryReportTestType,
				Aliases:      []domain.Alias{alias},
				Observation:  observation,
				ObservedAt:   time.Now(),
			}
			continue
		}
		reports[i] = application.InventoryReplacementInput{
			ResourceType: inventoryReportTestType,
			Name:         &names[i],
			Observation:  observation,
			ObservedAt:   time.Now(),
		}
	}

	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{Reports: reports}); err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}

	for i := 0; i < total; i++ {
		er := getExtensionResource(t, store, names[i])
		want := fmt.Sprintf(`{"v":%d}`, i)
		if er.Inventory().Observation() == nil || string(*er.Inventory().Observation()) != want {
			t.Fatalf("resource %d Observation = %v, want %s", i, er.Inventory().Observation(), want)
		}
	}
}

func TestInventoryReportService_ReplaceBatch_DuplicateAcrossChunkBoundaryFails(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	// Chunk size 2 puts report 0 and report 3 in different chunks
	// ([0,1], [2,3], [4]), so the duplicate can only be caught by
	// duplicate tracking that spans the whole call, not just one
	// chunk's own reportResolver.resolveBatch invocation.
	svc := application.NewInventoryReportService(store, application.WithInventoryReportChunkSize(2))
	ctx := context.Background()

	name0 := domain.ResourceName("clusters/chunked-0")
	name1 := domain.ResourceName("clusters/chunked-1")
	name2 := domain.ResourceName("clusters/chunked-2")

	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name0, Observation: rawMsg(`{"v":0}`), ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name1, Observation: rawMsg(`{"v":1}`), ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name2, Observation: rawMsg(`{"v":2}`), ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name0, Observation: rawMsg(`{"v":3}`), ObservedAt: time.Now()}, // duplicate of report 0, in a later chunk
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// No partial writes: earlier chunks must not have been committed
	// just because the duplicate was only discovered in a later one.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	for _, name := range []domain.ResourceName{name0, name1, name2} {
		if _, err := tx.ResourceIdentities().GetByName(ctx, name); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetByName(%s) err = %v, want ErrNotFound (no partial write across chunks)", name, err)
		}
	}
}

func TestInventoryReportService_ReplaceBatch_RejectsTypeWithoutInventoryMetadata(t *testing.T) {
	store := newStore(t)
	const managedOnlyType domain.ResourceType = "kind.fleetshift.io/Widget"
	seedManagedOnlyType(t, store, managedOnlyType)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("widgets/w1")
	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: managedOnlyType,
			Name:         &name,
			ObservedAt:   time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}
}

func TestInventoryReportService_ReplaceBatch_DuplicateResolvedUIDWithinBatchFails(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name, Observation: rawMsg(`{"v":1}`), ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Name: &name, Observation: rawMsg(`{"v":2}`), ObservedAt: time.Now()},
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// No partial writes: nothing should have been committed at all.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExtensionResources().Get(ctx, inventoryReportTestType.FullName(name)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound (no partial write)", err)
	}
}

// TestInventoryReportService_ReplaceBatch_MixedNameAndAliasDuplicateRejected
// exercises the specific new shape rejectDuplicateReports's doc
// comment calls out: not two reports naming the same resource
// directly (DuplicateResolvedUIDWithinBatchFails, above), but one
// report identifying a resource by Name and another identifying the
// very same resource purely by an alias it already owns.
func TestInventoryReportService_ReplaceBatch_MixedNameAndAliasDuplicateRejected(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/mixed-dup")
	alias, err := domain.NewAlias("gcp", "project_id", "mixed-dup-project")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	// Seed the resource and its alias via one prior report.
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name, Aliases: []domain.Alias{alias}, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed ReplaceBatch: %v", err)
	}

	err = svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name, Observation: rawMsg(`{"v":1}`), ObservedAt: time.Now()},
			{ResourceType: inventoryReportTestType, Aliases: []domain.Alias{alias}, Observation: rawMsg(`{"v":2}`), ObservedAt: time.Now()},
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}

	// Neither report's observation should have overwritten the seed.
	er := getExtensionResource(t, store, name)
	if er.Inventory().Observation() != nil {
		t.Errorf("Observation = %v, want nil (untouched by the rejected batch)", er.Inventory().Observation())
	}
}

// TestInventoryReportService_ReplaceBatch_RejectsReportWithNeitherNameNorAliases
// covers resolveBatch's first identity precondition: a report can't
// be resolved to any target at all without at least one of Name or
// Aliases set.
func TestInventoryReportService_ReplaceBatch_RejectsReportWithNeitherNameNorAliases(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Observation: rawMsg(`{"v":1}`), ObservedAt: time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ReplaceBatch err = %v, want ErrInvalidArgument", err)
	}
}

func TestInventoryReportService_ReplaceBatch_ObservationNilLeavesLatestUnchanged(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name, Observation: rawMsg(`{"v":1}`), ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("first ReplaceBatch: %v", err)
	}

	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name, Observation: nil, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("second ReplaceBatch: %v", err)
	}

	er := getExtensionResource(t, store, name)
	if er.Inventory().Observation() == nil || string(*er.Inventory().Observation()) != `{"v":1}` {
		t.Errorf("Observation = %v, want {\"v\":1} (untouched, not cleared)", er.Inventory().Observation())
	}

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	obs, err := tx.ExtensionResources().ListObservations(ctx, er.UID(), 10)
	if err != nil {
		t.Fatalf("ListObservations: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("ListObservations len = %d, want 1 (nil observation must not append history)", len(obs))
	}
}

// TestInventoryReportService_ApplyDeltaBatch_RejectsLabelInBothSetAndDelete
// covers validateDeltaReport's first guard: a key present in both
// SetLabels and DeleteLabels is self-contradictory and must be
// rejected before identity resolution or any SQL runs, rather than
// left for the repository to interpret (see the label CTEs' doc
// comments for why the repository itself does not define an ordering
// between the two).
func TestInventoryReportService_ApplyDeltaBatch_RejectsLabelInBothSetAndDelete(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			Labels: map[string]string{"zone": "us-east-1"}, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed ReplaceBatch: %v", err)
	}

	err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			SetLabels:    map[string]string{"zone": "us-west-2"},
			DeleteLabels: []string{"zone"},
			ObservedAt:   time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ApplyDeltaBatch err = %v, want ErrInvalidArgument", err)
	}

	// Rejected before any write: the label must be untouched.
	er := getExtensionResource(t, store, name)
	if got := er.Inventory().Labels()["zone"]; got != "us-east-1" {
		t.Errorf("Labels[zone] = %q, want unchanged %q", got, "us-east-1")
	}
}

// TestInventoryReportService_ApplyDeltaBatch_RejectsConditionInBothUpsertAndDelete
// is RejectsLabelInBothSetAndDelete's condition counterpart, covering
// validateDeltaReport's second guard.
func TestInventoryReportService_ApplyDeltaBatch_RejectsConditionInBothUpsertAndDelete(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	ready := mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", time.Now())
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			Conditions: []domain.Condition{ready}, ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed ReplaceBatch: %v", err)
	}

	degraded := mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", time.Now())
	err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			UpsertConditions: []domain.Condition{degraded},
			DeleteConditions: []domain.ConditionType{"Ready"},
			ObservedAt:       time.Now(),
		}},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("ApplyDeltaBatch err = %v, want ErrInvalidArgument", err)
	}

	// Rejected before any write: the condition must be untouched.
	er := getExtensionResource(t, store, name)
	conds := er.Inventory().Conditions()
	if len(conds) != 1 || conds[0].Status() != domain.ConditionTrue {
		t.Errorf("Conditions = %+v, want unchanged [Ready=True]", conds)
	}
}

func TestInventoryReportService_ApplyDeltaBatch_ObservationNilVsReplace(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name, Observation: rawMsg(`{"v":0}`), ObservedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed ReplaceBatch: %v", err)
	}
	er := getExtensionResource(t, store, name)

	// Observation == nil: no write, no history.
	if err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			Observation: nil,
			ObservedAt:  time.Now(),
		}},
	}); err != nil {
		t.Fatalf("nil-observation ApplyDeltaBatch: %v", err)
	}
	after := getExtensionResource(t, store, name)
	if string(*after.Inventory().Observation()) != `{"v":0}` {
		t.Errorf("after nil observation, Observation = %s, want {\"v\":0}", *after.Inventory().Observation())
	}

	// Observation != nil: write + history.
	if err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			Observation: rawMsg(`{"v":1}`),
			ObservedAt:  time.Now(),
		}},
	}); err != nil {
		t.Fatalf("non-nil-observation ApplyDeltaBatch: %v", err)
	}
	after = getExtensionResource(t, store, name)
	if string(*after.Inventory().Observation()) != `{"v":1}` {
		t.Errorf("after non-nil observation, Observation = %s, want {\"v\":1}", *after.Inventory().Observation())
	}

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	obs, err := tx.ExtensionResources().ListObservations(ctx, er.UID(), 10)
	if err != nil {
		t.Fatalf("ListObservations: %v", err)
	}
	if len(obs) != 2 {
		t.Fatalf("ListObservations len = %d, want 2 (seed + non-nil; nil appended none)", len(obs))
	}
}

func TestInventoryReportService_ApplyDeltaBatch_SetsLabelsAndConditionsLeavingOthersUntouched(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	now := time.Now()
	ready := mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "", now)
	healthy := mustCondition(t, "Healthy", domain.ConditionTrue, "Nominal", "", now)
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType, Name: &name,
			Labels:     map[string]string{"env": "prod", "region": "us-east-1"},
			Conditions: []domain.Condition{ready, healthy},
			ObservedAt: now,
		}},
	}); err != nil {
		t.Fatalf("seed ReplaceBatch: %v", err)
	}

	if err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType:     inventoryReportTestType,
			Name:             &name,
			SetLabels:        map[string]string{"region": "us-west-2"},
			DeleteLabels:     []string{"env"},
			UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "", now)},
			ObservedAt:       now,
		}},
	}); err != nil {
		t.Fatalf("ApplyDeltaBatch: %v", err)
	}

	er := getExtensionResource(t, store, name)
	inv := er.Inventory()
	if _, ok := inv.Labels()["env"]; ok {
		t.Errorf("Labels[env] should be deleted, got %q", inv.Labels()["env"])
	}
	if inv.Labels()["region"] != "us-west-2" {
		t.Errorf("Labels[region] = %q, want us-west-2", inv.Labels()["region"])
	}
	var readyCond, healthyCond *domain.Condition
	for i, c := range inv.Conditions() {
		switch c.Type() {
		case "Ready":
			readyCond = &inv.Conditions()[i]
		case "Healthy":
			healthyCond = &inv.Conditions()[i]
		}
	}
	if readyCond == nil || readyCond.Status() != domain.ConditionFalse {
		t.Fatalf("Ready condition = %+v, want status False", readyCond)
	}
	if healthyCond == nil || healthyCond.Status() != domain.ConditionTrue {
		t.Fatalf("Healthy condition should be untouched, got %+v", healthyCond)
	}
}

func TestInventoryReportService_ReceivedAtCapturedOnceViaClock(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)

	var calls int
	clockTimes := []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	svc := application.NewInventoryReportService(store, application.WithInventoryReportClock(func() time.Time {
		tm := clockTimes[calls]
		calls++
		return tm
	}))
	ctx := context.Background()

	name1 := domain.ResourceName("clusters/c1")
	name2 := domain.ResourceName("clusters/c2")
	err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{
			{ResourceType: inventoryReportTestType, Name: &name1, Observation: rawMsg(`{}`), ObservedAt: clockTimes[0]},
			{ResourceType: inventoryReportTestType, Name: &name2, Observation: rawMsg(`{}`), ObservedAt: clockTimes[0]},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}
	if calls != 1 {
		t.Fatalf("clock called %d times, want exactly 1 per batch", calls)
	}

	er1 := getExtensionResource(t, store, name1)
	er2 := getExtensionResource(t, store, name2)
	if !er1.Inventory().UpdatedAt().Equal(clockTimes[0]) {
		t.Errorf("c1 UpdatedAt = %v, want %v", er1.Inventory().UpdatedAt(), clockTimes[0])
	}
	if !er2.Inventory().UpdatedAt().Equal(clockTimes[0]) {
		t.Errorf("c2 UpdatedAt = %v, want %v", er2.Inventory().UpdatedAt(), clockTimes[0])
	}
}

// getPlatformResource is a test helper mirroring getExtensionResource
// but for the platform-level identity.
func getPlatformResource(t *testing.T, store domain.Store, name domain.ResourceName) *domain.PlatformResource {
	t.Helper()
	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		t.Fatalf("GetByName(%s): %v", name, err)
	}
	return pr
}

func TestInventoryReportService_ReplaceBatch_NewIdentityGetsNoLabelsFromReport(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := svc.ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: inventoryReportTestType,
			Name:         &name,
			Labels:       map[string]string{"env": "prod"},
			ObservedAt:   time.Now(),
		}},
	}); err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}

	// The reporter-observed label belongs on the InventoryResource, not
	// on the platform-level identity, which has its own separate
	// user-managed label concept.
	pr := getPlatformResource(t, store, name)
	if len(pr.Labels()) != 0 {
		t.Errorf("PlatformResource.Labels() = %+v, want empty (report labels are extension-resource-scoped)", pr.Labels())
	}

	er := getExtensionResource(t, store, name)
	if er.Inventory().Labels()["env"] != "prod" {
		t.Errorf("Inventory.Labels()[env] = %q, want prod", er.Inventory().Labels()["env"])
	}
}

func TestInventoryReportService_ApplyDeltaBatch_NewIdentityGetsNoLabelsFromReport(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := svc.ApplyDeltaBatch(ctx, application.InventoryDeltaBatchInput{
		Reports: []application.InventoryDeltaInput{{
			ResourceType: inventoryReportTestType,
			Name:         &name,
			SetLabels:    map[string]string{"env": "prod"},
			ObservedAt:   time.Now(),
		}},
	}); err != nil {
		t.Fatalf("ApplyDeltaBatch: %v", err)
	}

	pr := getPlatformResource(t, store, name)
	if len(pr.Labels()) != 0 {
		t.Errorf("PlatformResource.Labels() = %+v, want empty (report labels are extension-resource-scoped)", pr.Labels())
	}

	er := getExtensionResource(t, store, name)
	if er.Inventory().Labels()["env"] != "prod" {
		t.Errorf("Inventory.Labels()[env] = %q, want prod", er.Inventory().Labels()["env"])
	}
}
