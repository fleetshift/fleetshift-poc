package postgres_test

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

// TestApplyInventoryDeltas_ConcurrentDeltasForDifferentKeysDoNotLoseUpdates
// reproduces the lost-update race ExtensionResourceRepo.ApplyInventoryDeltas's
// doc comment describes: two concurrent calls setting different label
// keys on the same, already-existing resource must both end up
// present, not have the second overwrite the first's change with a
// merge computed from data read before the first committed.
//
// The test drives this deterministically, without any fixed sleep,
// by using two real transactions and Postgres's own row locking: txA
// applies its delta and stays open (uncommitted), so its row lock on
// extension_resource_inventory is held; txB's delta for the very same
// resource is issued concurrently in a goroutine, which must block
// inside Postgres waiting for that lock. The test polls pg_locks (see
// waitForBlockedBackend) until it observes txB's backend genuinely
// blocked -- confirming txB's own merge computation already ran
// against pre-commit state -- before committing txA and letting txB
// proceed. On the old design (merging against a prev_inv CTE read
// once at statement start, then writing via
// `ON CONFLICT DO UPDATE SET labels = EXCLUDED.labels`), txB's
// unblocked write would still use its stale, pre-computed EXCLUDED
// value and clobber txA's label. On the current design (merging
// inline against extension_resource_inventory's own current row in a
// plain UPDATE, which Postgres re-evaluates against the latest
// committed row once the lock is granted), both labels survive.
func TestApplyInventoryDeltas_ConcurrentDeltasForDifferentKeysDoNotLoseUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := postgres.OpenTestDB(t)

	rt := domain.ResourceType("inv.fleetshift.io/Node")
	name := domain.ResourceName("nodes/concurrent-delta")
	seedRepo := postgres.ExtensionResourceRepo{DB: db}
	if err := seedRepo.CreateType(ctx, domain.NewExtensionResourceType(
		rt, "v1", "nodes", time.Unix(0, 0).UTC(), domain.WithInventory(),
	)); err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	// Seed an existing inventory row first -- both concurrent deltas
	// below must hit ApplyInventoryDeltas's merge-against-existing-row
	// path (updated_inv), not the create-new-row path, since that's
	// the path the old design computed from a stale snapshot.
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if err := seedRepo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
		ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
		SetLabels:  map[string]string{"seed": "1"},
		ObservedAt: t0, ReceivedAt: t0,
	}}); err != nil {
		t.Fatalf("seed ApplyInventoryDeltas: %v", err)
	}

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	defer txA.Rollback()
	repoA := postgres.ExtensionResourceRepo{DB: txA}
	tA := t0.Add(time.Minute)
	if err := repoA.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
		ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
		SetLabels:  map[string]string{"a": "1"},
		ObservedAt: tA, ReceivedAt: tA,
	}}); err != nil {
		t.Fatalf("repoA.ApplyInventoryDeltas: %v", err)
	}

	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin txB: %v", err)
	}
	defer txB.Rollback()
	var pidB int
	if err := txB.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pidB); err != nil {
		t.Fatalf("txB pg_backend_pid: %v", err)
	}
	repoB := postgres.ExtensionResourceRepo{DB: txB}
	tB := t0.Add(2 * time.Minute)
	done := make(chan error, 1)
	go func() {
		done <- repoB.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
			ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
			SetLabels:  map[string]string{"b": "1"},
			ObservedAt: tB, ReceivedAt: tB,
		}})
	}()

	waitForBlockedBackend(t, db, pidB)

	if err := txA.Commit(); err != nil {
		t.Fatalf("commit txA: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("repoB.ApplyInventoryDeltas: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("repoB.ApplyInventoryDeltas did not unblock after txA committed")
	}
	if err := txB.Commit(); err != nil {
		t.Fatalf("commit txB: %v", err)
	}

	finalRepo := postgres.ExtensionResourceRepo{DB: db}
	got, err := finalRepo.Get(ctx, rt.FullName(name))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := map[string]string{"seed": "1", "a": "1", "b": "1"}
	if got := got.Inventory().Labels(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Labels() = %+v, want %+v (a concurrent delta's label was lost)", got, want)
	}
}

// TestApplyInventoryDeltas_ConcurrentAliasUpsertsDoNotLoseUpdates is
// TestApplyInventoryDeltas_ConcurrentDeltasForDifferentKeysDoNotLoseUpdates's
// counterpart for the alias half of ApplyInventoryDeltas's
// lost-update fix: two concurrent UpsertAliases deltas for the same
// resource, each contributing a different alias, must both survive.
//
// Unlike the labels/conditions case (fixed by merging inline in a
// single UPDATE), the alias merge is fixed by a dedicated
// SELECT ... FOR UPDATE (applyInventoryDeltasCurrentAliasesSQL) that
// holds its lock across ApplyInventoryDeltas's Go-side merge and its
// final write. This test drives the same real-transactions-plus-
// pg_locks-polling approach to force that interleaving deterministically:
// txA's ApplyInventoryDeltas call runs to completion (including its
// own FOR UPDATE read and merge-write) but stays uncommitted, so its
// row lock on extension_resources is held; txB's concurrent call for
// the same resource must block waiting for that lock before it can
// even read current aliases, let alone merge against them.
func TestApplyInventoryDeltas_ConcurrentAliasUpsertsDoNotLoseUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := postgres.OpenTestDB(t)

	rt := domain.ResourceType("inv.fleetshift.io/Node")
	name := domain.ResourceName("nodes/concurrent-alias")
	seedRepo := postgres.ExtensionResourceRepo{DB: db}
	if err := seedRepo.CreateType(ctx, domain.NewExtensionResourceType(
		rt, "v1", "nodes", time.Unix(0, 0).UTC(), domain.WithInventory(),
	)); err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	seedAlias, err := domain.NewAlias("net.example/ip", "seed", "1.2.3.4")
	if err != nil {
		t.Fatalf("NewAlias(seed): %v", err)
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if err := seedRepo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
		ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
		UpsertAliases: []domain.Alias{seedAlias},
		ObservedAt:    t0, ReceivedAt: t0,
	}}); err != nil {
		t.Fatalf("seed ApplyInventoryDeltas: %v", err)
	}

	aliasA, err := domain.NewAlias("net.example/ip", "a", "10.0.0.1")
	if err != nil {
		t.Fatalf("NewAlias(a): %v", err)
	}
	aliasB, err := domain.NewAlias("net.example/ip", "b", "10.0.0.2")
	if err != nil {
		t.Fatalf("NewAlias(b): %v", err)
	}

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	defer txA.Rollback()
	repoA := postgres.ExtensionResourceRepo{DB: txA}
	tA := t0.Add(time.Minute)
	if err := repoA.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
		ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
		UpsertAliases: []domain.Alias{aliasA},
		ObservedAt:    tA, ReceivedAt: tA,
	}}); err != nil {
		t.Fatalf("repoA.ApplyInventoryDeltas: %v", err)
	}

	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin txB: %v", err)
	}
	defer txB.Rollback()
	var pidB int
	if err := txB.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pidB); err != nil {
		t.Fatalf("txB pg_backend_pid: %v", err)
	}
	repoB := postgres.ExtensionResourceRepo{DB: txB}
	tB := t0.Add(2 * time.Minute)
	done := make(chan error, 1)
	go func() {
		done <- repoB.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
			ResourceType: rt, Name: name, CandidateUID: domain.NewExtensionResourceUID(),
			UpsertAliases: []domain.Alias{aliasB},
			ObservedAt:    tB, ReceivedAt: tB,
		}})
	}()

	waitForBlockedBackend(t, db, pidB)

	if err := txA.Commit(); err != nil {
		t.Fatalf("commit txA: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("repoB.ApplyInventoryDeltas: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("repoB.ApplyInventoryDeltas did not unblock after txA committed")
	}
	if err := txB.Commit(); err != nil {
		t.Fatalf("commit txB: %v", err)
	}

	finalRepo := postgres.ExtensionResourceRepo{DB: db}
	got, err := finalRepo.Get(ctx, rt.FullName(name))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := []domain.Alias{seedAlias, aliasA, aliasB}
	gotAliases := append([]domain.Alias(nil), got.ReportedAliases()...)
	sortAliases(gotAliases)
	wantSorted := append([]domain.Alias(nil), want...)
	sortAliases(wantSorted)
	if !reflect.DeepEqual(gotAliases, wantSorted) {
		t.Fatalf("ReportedAliases() = %+v, want %+v (a concurrent delta's alias was lost)", gotAliases, wantSorted)
	}
}

func sortAliases(aliases []domain.Alias) {
	sort.Slice(aliases, func(i, j int) bool {
		if aliases[i].Namespace != aliases[j].Namespace {
			return aliases[i].Namespace < aliases[j].Namespace
		}
		if aliases[i].Key != aliases[j].Key {
			return aliases[i].Key < aliases[j].Key
		}
		return aliases[i].Value < aliases[j].Value
	})
}

// waitForBlockedBackend polls pg_locks until pid has at least one
// ungranted (i.e. waiting) lock, or fails the test if that never
// happens within the deadline.
//
// This polls with a short fixed interval rather than using a
// deterministic Go-level signal because there isn't one to use: pid
// is a *different* Postgres backend/connection blocked deep inside
// the server's own lock manager, and pg_locks is the only window into
// that state. This is the same category of exception the "never
// time.Sleep in tests" rule carves out for OS-coupled abstractions --
// here the abstraction is Postgres's row locking, which the very
// thing under test depends on.
func waitForBlockedBackend(t *testing.T, db *sql.DB, pid int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)`, pid,
		).Scan(&blocked); err != nil {
			t.Fatalf("poll pg_locks: %v", err)
		}
		if blocked {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("backend pid %d never became blocked waiting on a lock within the deadline", pid)
}
