package steps

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

// Suite-owned Kind topologies. Workload tests borrow these; only
// TestKindClusterLifecycle creates and deletes a private cluster.
const (
	sharedKindIDA = "kind-e2e-a"
	sharedKindIDB = "kind-e2e-b"
)

var (
	sharedKindMu     sync.Mutex
	sharedKindAReady bool
	sharedKindBReady bool
)

// sharedKindSlot is one suite pool cluster and its ready flag.
type sharedKindSlot struct {
	id    string
	ready *bool
}

// sharedKindSlots returns the suite pool in create order (a, then b).
func sharedKindSlots() []sharedKindSlot {
	return []sharedKindSlot{
		{id: sharedKindIDA, ready: &sharedKindAReady},
		{id: sharedKindIDB, ready: &sharedKindBReady},
	}
}

// SharedKind returns the suite's default single-node Kind cluster, creating
// it on first use (ready + OIDC). Workload tests must not delete it.
func SharedKind(t *testing.T, f *harness.Fixture) string {
	t.Helper()
	ensureSharedKind(t, f, 1)
	return sharedKindIDA
}

// SharedKindPair returns two suite Kind clusters for multi-target tests,
// creating them on first use. The first id is the same as SharedKind.
func SharedKindPair(t *testing.T, f *harness.Fixture) (a, b string) {
	t.Helper()
	ensureSharedKind(t, f, 2)
	return sharedKindIDA, sharedKindIDB
}

// ReleaseSharedKindClusters deletes the lazy shared pool through fleetctl
// while the AIO is still up. TestMain calls this after m.Run, after go test
// has already printed PASS. Logs to stderr so the wait is visible. Best-effort.
func ReleaseSharedKindClusters(f *harness.Fixture) {
	if f == nil {
		return
	}
	sharedKindMu.Lock()
	defer sharedKindMu.Unlock()

	slots := sharedKindSlots()
	var names []string
	for _, s := range slots {
		if *s.ready || kindClusterPresent(f, s.id) {
			names = append(names, s.id)
		}
	}
	if len(names) == 0 {
		return
	}
	fmt.Fprintf(os.Stderr, "e2e/backend: tests finished; deleting shared kind clusters %s (can take a minute; do not interrupt)\n", strings.Join(names, ", "))
	for _, name := range names {
		fmt.Fprintf(os.Stderr, "e2e/backend: deleting shared kind %s\n", name)
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		res := f.Run(ctx, "resource", "delete", kindClusterType, name)
		cancel()
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "e2e/backend: delete shared kind %s: %s\n", name, fleetctlDetail(res))
		}
	}
	waitSharedKindGone(f, names)
	for _, s := range slots {
		*s.ready = false
	}
}

// waitSharedKindGone polls until none of names remain, or the wait budget expires.
func waitSharedKindGone(f *harness.Fixture, names []string) {
	log := stderrPollLog()
	deadline := time.Now().Add(kindClusterWaitTimeout)
	for time.Now().Before(deadline) {
		var still []string
		for _, name := range names {
			if kindClusterPresent(f, name) {
				still = append(still, name)
			}
		}
		if len(still) == 0 {
			fmt.Fprintf(os.Stderr, "e2e/backend: shared kind clusters gone\n")
			return
		}
		log.logf("waiting for shared kind clusters to be gone: %s", strings.Join(still, ", "))
		time.Sleep(clusterPollInterval)
	}
	fmt.Fprintf(os.Stderr, "e2e/backend: timed out waiting for shared kind clusters to be gone\n")
}

// ensureSharedKind creates n of the suite clusters (1 or 2), submitting
// creates before waiting so a pair overlaps.
func ensureSharedKind(t *testing.T, f *harness.Fixture, n int) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	if n < 1 || n > 2 {
		t.Fatalf("ensureSharedKind: n=%d, want 1 or 2", n)
	}

	sharedKindMu.Lock()
	defer sharedKindMu.Unlock()

	slots := sharedKindSlots()[:n]
	for _, s := range slots {
		startSharedKind(t, f, s.id, *s.ready)
	}
	for _, s := range slots {
		finishSharedKind(t, f, s.id, s.ready)
	}
}

// startSharedKind submits resource create when the cluster is not already
// present. ready clusters are left alone.
func startSharedKind(t *testing.T, f *harness.Fixture, name string, ready bool) {
	t.Helper()
	if ready {
		return
	}
	if kindClusterPresent(f, name) {
		t.Logf("shared kind cluster %s already created", name)
		return
	}
	t.Logf("creating shared kind cluster %s", name)
	CreateKindCluster(t, f, name)
}

// finishSharedKind waits until name is ACTIVE, Ready, and OIDC-usable.
func finishSharedKind(t *testing.T, f *harness.Fixture, name string, ready *bool) {
	t.Helper()
	if *ready {
		return
	}
	WaitForKindClusterReady(t, f, name)
	WaitForKindOIDC(t, f, name)
	*ready = true
}

// kindClusterPresent reports whether fleetctl resource get succeeds for this Kind cluster id.
func kindClusterPresent(f *harness.Fixture, name string) bool {
	if f == nil || name == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "resource", "get", kindClusterType, name)
	return res.Err == nil
}

// isSharedKindCluster reports whether name is a suite pool id.
func isSharedKindCluster(name string) bool {
	return name == sharedKindIDA || name == sharedKindIDB
}
