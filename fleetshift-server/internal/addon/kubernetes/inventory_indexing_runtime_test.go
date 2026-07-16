package kubernetes

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeIndexerClients struct {
	dynamic   func(*rest.Config) (dynamic.Interface, error)
	discovery func(*rest.Config) (discovery.DiscoveryInterface, error)
}

func (f fakeIndexerClients) Dynamic(cfg *rest.Config) (dynamic.Interface, error) {
	if f.dynamic != nil {
		return f.dynamic(cfg)
	}
	return newFakeDynamicClient(podsGVR(), crdGVR), nil
}

func (f fakeIndexerClients) Discovery(cfg *rest.Config) (discovery.DiscoveryInterface, error) {
	if f.discovery != nil {
		return f.discovery(cfg)
	}
	return newFakeDiscovery([]*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	}}), nil
}

// testIndexingRuntime builds a host with fake clients suitable for EnsureIndexer
// unit tests.
func testIndexingRuntime(t *testing.T, clients ...IndexerClients) *KubernetesInProcessIndexHost {
	t.Helper()
	var c IndexerClients = fakeIndexerClients{}
	if len(clients) > 0 && clients[0] != nil {
		c = clients[0]
	}
	return NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		c,
		slog.New(slog.DiscardHandler),
	)
}

// testClusterResourceName returns a clusters/{id} name for test target IDs.
func testClusterResourceName(targetID string) domain.ResourceName {
	return domain.ResourceName("clusters/" + targetID)
}

// testIndexInput returns a minimal valid [IndexRuntimeInput] for tests.
func testIndexInput(id string, gen domain.Generation, cred string) IndexRuntimeInput {
	in, err := NewIndexRuntimeInput(
		domain.TargetID(id),
		testClusterResourceName(id),
		"https://example",
		"",
		[]byte(cred),
		"",
		gen,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		panic(err)
	}
	return in
}

// TestIndexingRuntime_EnsureIndexerReadinessAndIdempotent verifies ready
// EnsureIndexer succeeds and repeats are no-ops until StopIndexer.
func TestIndexingRuntime_EnsureIndexerReadinessAndIdempotent(t *testing.T) {
	h := testIndexingRuntime(t)
	input := testIndexInput("pm-1", 1, "tok-a")

	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	if !h.HasIndexer("pm-1") {
		t.Fatal("expected indexer ready")
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("idempotent EnsureIndexer: %v", err)
	}

	if err := h.StopIndexer(context.Background(), "pm-1"); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	if h.HasIndexer("pm-1") {
		t.Fatal("expected stopped")
	}
}

// TestManagedIndexer_DoesNotRetainCredentialBytes locks the registry entry
// shape: handoff credential bytes must not be stored on managedIndexer.
func TestManagedIndexer_DoesNotRetainCredentialBytes(t *testing.T) {
	typ := reflect.TypeOf(managedIndexer{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
			t.Errorf("managedIndexer must not retain credential bytes; found []byte field %q", f.Name)
		}
	}

	h := testIndexingRuntime(t)
	cred := []byte("must-not-be-retained")
	if err := h.EnsureIndexer(context.Background(), testIndexInput("pm-noretain", 1, string(cred))); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	h.mu.Lock()
	entry := h.entries["pm-noretain"]
	h.mu.Unlock()
	if entry == nil {
		t.Fatal("expected registry entry")
	}
	if entry.secretRef != "" {
		t.Fatalf("secretRef = %q, want empty when start used handoff bytes only", entry.secretRef)
	}
	if entry.fingerprint == string(cred) {
		t.Fatal("fingerprint must not equal raw credential bytes")
	}
}

// TestIndexingRuntime_GenerationFence rejects a lower producer generation
// without replacing the newer process.
func TestIndexingRuntime_GenerationFence(t *testing.T) {
	h := testIndexingRuntime(t)
	if err := h.EnsureIndexer(context.Background(), testIndexInput("pm-gen", 2, "tok")); err != nil {
		t.Fatalf("EnsureIndexer gen=2: %v", err)
	}
	err := h.EnsureIndexer(context.Background(), testIndexInput("pm-gen", 1, "tok-other"))
	if !errors.Is(err, ErrStaleIndexerGeneration) {
		t.Fatalf("stale generation error = %v, want ErrStaleIndexerGeneration", err)
	}
	if !h.HasIndexer("pm-gen") {
		t.Fatal("expected gen=2 indexer retained")
	}
}

// TestIndexingRuntime_FingerprintReplace stop-and-replaces when the same
// generation presents a different runtime fingerprint.
func TestIndexingRuntime_FingerprintReplace(t *testing.T) {
	var discClients atomic.Int32
	h := testIndexingRuntime(t, fakeIndexerClients{
		discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
			discClients.Add(1)
			return newFakeDiscovery([]*metav1.APIResourceList{{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			}}), nil
		},
	})

	if err := h.EnsureIndexer(context.Background(), testIndexInput("pm-fp", 1, "tok-a")); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), testIndexInput("pm-fp", 1, "tok-b")); err != nil {
		t.Fatalf("fingerprint replace: %v", err)
	}
	if discClients.Load() < 2 {
		t.Fatalf("discovery clients = %d, want >= 2 after replace", discClients.Load())
	}
	if !h.HasIndexer("pm-fp") {
		t.Fatal("expected replaced indexer running")
	}
}

// TestIndexingRuntime_AllowListEmptyFailFast treats an explicit allow-list
// that matches nothing as a permanent validation error.
func TestIndexingRuntime_AllowListEmptyFailFast(t *testing.T) {
	h := testIndexingRuntime(t)
	input := testIndexInput("pm-allow", 1, "tok")
	input.IndexConfig.AllowList = []Resource{{
		ApiGroups: []string{"example.com"},
		Resources: []string{"nope"},
	}}
	err := h.EnsureIndexer(context.Background(), input)
	if !errors.Is(err, ErrIndexerAllowListEmpty) {
		t.Fatalf("error = %v, want ErrIndexerAllowListEmpty", err)
	}
	if h.HasIndexer("pm-allow") {
		t.Fatal("failed readiness must not leave a tracked indexer")
	}
}

// TestNewIndexRuntimeInput_EmptyCredential rejects an empty indexing credential.
func TestNewIndexRuntimeInput_EmptyCredential(t *testing.T) {
	_, err := NewIndexRuntimeInput(
		"pm-cred",
		testClusterResourceName("pm-cred"),
		"https://example",
		"",
		nil,
		"",
		1,
		IndexConfig{},
	)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

// TestIndexingRuntime_StopAllSuppressesRestart ensures StopAll does not
// leave a restarted indexer after a stuck runner is canceled.
func TestIndexingRuntime_StopAllSuppressesRestart(t *testing.T) {
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/pm-stopall/token": []byte("vault-token"),
	}}
	unblock := make(chan struct{})
	var blocked atomic.Bool

	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		vault,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR()), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return &readyThenBlockDiscovery{
					fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
						GroupVersion: "v1",
						APIResources: []metav1.APIResource{
							{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
						},
					}}),
					unblock: unblock,
					onBlock: func() { blocked.Store(true) },
				}, nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input, err := NewIndexRuntimeInput(
		"pm-stopall",
		testClusterResourceName("pm-stopall"),
		"https://example",
		"",
		[]byte("vault-token"),
		"targets/pm-stopall/token",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	awaitDiscoveryBlocked(t, &blocked)

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = h.StopAll(stopCtx) // may time out while runner is blocked after readiness
	close(unblock)

	deadline := time.After(2 * time.Second)
	for h.HasIndexer("pm-stopall") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for stop cleanup")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Give any suppressed restart a chance to fire (would be >=1s backoff).
	time.Sleep(50 * time.Millisecond)
	if h.HasIndexer("pm-stopall") {
		t.Fatal("StopAll must not leave a restarted indexer")
	}
}

// TestIndexingRuntime_IntentionalStopNoRestart verifies StopIndexer does
// not schedule a local restart even when a SecretRef is present.
func TestIndexingRuntime_IntentionalStopNoRestart(t *testing.T) {
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/pm-stop/token": []byte("vault-token"),
	}}
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		vault,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input, err := NewIndexRuntimeInput(
		"pm-stop",
		testClusterResourceName("pm-stop"),
		"https://example",
		"",
		[]byte("vault-token"),
		"targets/pm-stop/token",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	if err := h.StopIndexer(context.Background(), "pm-stop"); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if h.HasIndexer("pm-stop") {
		t.Fatal("intentional stop must not restart")
	}
}

// TestIndexingRuntime_UnexpectedExitWithoutSecretRefDoesNotRestart verifies
// that an unexpected indexer exit with no SecretRef on the start input does
// not restart the indexer. Restart requires a vault-resolvable SecretRef.
func TestIndexingRuntime_UnexpectedExitWithoutSecretRefDoesNotRestart(t *testing.T) {
	hostCtx, cancel := context.WithCancel(context.Background())
	h := NewKubernetesInProcessIndexHost(
		hostCtx,
		&indexHostTestVault{secrets: map[domain.SecretRef][]byte{}},
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	if err := h.EnsureIndexer(context.Background(), testIndexInput("pm-nosecret", 1, "handoff-token")); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	cancel() // host context cancel ends the indexer without StopIndexer

	deadline := time.After(2 * time.Second)
	for h.HasIndexer("pm-nosecret") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for indexer exit")
		case <-time.After(10 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond)
	if h.HasIndexer("pm-nosecret") {
		t.Fatal("unexpected exit without SecretRef must not restart")
	}
}

// withShortRestartBackoff shortens unexpected-exit restart delays for tests.
func withShortRestartBackoff(t *testing.T) {
	t.Helper()
	prev := unexpectedRestartBackoff
	unexpectedRestartBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { unexpectedRestartBackoff = prev })
}

// cancelIndexerUnexpectedly cancels a ready indexer's run context without
// StopIndexer, so onIndexerExit treats the exit as unexpected.
func cancelIndexerUnexpectedly(t *testing.T, h *KubernetesInProcessIndexHost, id domain.TargetID) {
	t.Helper()
	h.mu.Lock()
	entry := h.entries[id]
	if entry == nil || entry.cancel == nil {
		h.mu.Unlock()
		t.Fatalf("no cancelable indexer for %s", id)
	}
	cancel := entry.cancel
	h.mu.Unlock()
	cancel()
}

// awaitHasIndexer waits until HasIndexer matches want.
func awaitHasIndexer(t *testing.T, h *KubernetesInProcessIndexHost, id domain.TargetID, want bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for h.HasIndexer(id) != want {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for HasIndexer(%s)=%v", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestIndexingRuntime_UnexpectedExitRestartsFromVault covers the successful
// unexpected-exit path: resolveSecret + EnsureIndexer restart.
func TestIndexingRuntime_UnexpectedExitRestartsFromVault(t *testing.T) {
	withShortRestartBackoff(t)
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/pm-restart/token": []byte("vault-token"),
	}}
	var ensureDiscCalls atomic.Int32
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		vault,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				ensureDiscCalls.Add(1)
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input, err := NewIndexRuntimeInput(
		"pm-restart",
		testClusterResourceName("pm-restart"),
		"https://example",
		"",
		[]byte("handoff-token"),
		"targets/pm-restart/token",
		3,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	firstDisc := ensureDiscCalls.Load()
	cancelIndexerUnexpectedly(t, h, "pm-restart")

	deadline := time.After(2 * time.Second)
	for ensureDiscCalls.Load() <= firstDisc || !h.HasIndexer("pm-restart") {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for vault restart (discCalls=%d has=%v)",
				ensureDiscCalls.Load(), h.HasIndexer("pm-restart"))
		case <-time.After(5 * time.Millisecond):
		}
	}

	h.mu.Lock()
	entry := h.entries["pm-restart"]
	attempts := 0
	ready := false
	if entry != nil {
		attempts = entry.restartAttempts
		ready = entry.ready
	}
	h.mu.Unlock()
	if !ready {
		t.Fatal("expected restarted indexer to be ready")
	}
	if attempts < 1 {
		t.Fatalf("restartAttempts = %d, want >= 1", attempts)
	}
}

// TestIndexingRuntime_UnexpectedExitSkipsRestartWhenVaultMissing covers
// resolveSecret failure during unexpected-exit restart.
func TestIndexingRuntime_UnexpectedExitSkipsRestartWhenVaultMissing(t *testing.T) {
	withShortRestartBackoff(t)
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{}}
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		vault,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input, err := NewIndexRuntimeInput(
		"pm-novault",
		testClusterResourceName("pm-novault"),
		"https://example",
		"",
		[]byte("handoff-token"),
		"targets/pm-novault/missing",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	cancelIndexerUnexpectedly(t, h, "pm-novault")
	awaitHasIndexer(t, h, "pm-novault", false)
	time.Sleep(20 * time.Millisecond)
	if h.HasIndexer("pm-novault") {
		t.Fatal("missing vault secret must not restart indexer")
	}
}

// TestIndexingRuntime_UnexpectedExitRestartBudgetExhausted verifies local
// restart gives up after maxUnexpectedRestartAttempts successful restarts.
func TestIndexingRuntime_UnexpectedExitRestartBudgetExhausted(t *testing.T) {
	withShortRestartBackoff(t)
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/pm-budget/token": []byte("vault-token"),
	}}
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		vault,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input, err := NewIndexRuntimeInput(
		"pm-budget",
		testClusterResourceName("pm-budget"),
		"https://example",
		"",
		[]byte("handoff-token"),
		"targets/pm-budget/token",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}

	// maxUnexpectedRestartAttempts successful crash→restart cycles, then one
	// more crash that must not restart.
	for i := 0; i < maxUnexpectedRestartAttempts; i++ {
		h.mu.Lock()
		before := 0
		if e := h.entries["pm-budget"]; e != nil {
			before = e.restartAttempts
		}
		h.mu.Unlock()

		cancelIndexerUnexpectedly(t, h, "pm-budget")

		deadline := time.After(2 * time.Second)
		for {
			h.mu.Lock()
			e := h.entries["pm-budget"]
			ok := e != nil && e.ready && e.restartAttempts > before
			h.mu.Unlock()
			if ok {
				break
			}
			select {
			case <-deadline:
				t.Fatalf("timed out waiting for restart #%d", i+1)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	cancelIndexerUnexpectedly(t, h, "pm-budget")
	awaitHasIndexer(t, h, "pm-budget", false)
	time.Sleep(20 * time.Millisecond)
	if h.HasIndexer("pm-budget") {
		t.Fatal("expected no restart after budget exhausted")
	}
}

// TestResolveSecret exercises resolveSecret success and failure paths.
func TestResolveSecret(t *testing.T) {
	t.Run("empty ref", func(t *testing.T) {
		h := testIndexingRuntime(t)
		if _, err := h.resolveSecret(context.Background(), ""); err == nil {
			t.Fatal("expected missing secret ref error")
		}
	})
	t.Run("nil vault", func(t *testing.T) {
		h := testIndexingRuntime(t)
		if _, err := h.resolveSecret(context.Background(), "targets/x"); err == nil {
			t.Fatal("expected no vault configured error")
		}
	})
	t.Run("missing secret", func(t *testing.T) {
		h := NewKubernetesInProcessIndexHost(
			context.Background(),
			&indexHostTestVault{secrets: map[domain.SecretRef][]byte{}},
			&recordingReporter{},
			fakeIndexerClients{
				dynamic: func(*rest.Config) (dynamic.Interface, error) {
					return newFakeDynamicClient(podsGVR()), nil
				},
				discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
					return newFakeDiscovery(nil), nil
				},
			},
			nil,
		)
		if _, err := h.resolveSecret(context.Background(), "missing"); err == nil {
			t.Fatal("expected vault get error")
		}
	})
	t.Run("empty secret", func(t *testing.T) {
		h := NewKubernetesInProcessIndexHost(
			context.Background(),
			&indexHostTestVault{secrets: map[domain.SecretRef][]byte{"empty": {}}},
			&recordingReporter{},
			fakeIndexerClients{
				dynamic: func(*rest.Config) (dynamic.Interface, error) {
					return newFakeDynamicClient(podsGVR()), nil
				},
				discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
					return newFakeDiscovery(nil), nil
				},
			},
			nil,
		)
		if _, err := h.resolveSecret(context.Background(), "empty"); err == nil {
			t.Fatal("expected empty vault secret error")
		}
	})
	t.Run("success", func(t *testing.T) {
		h := NewKubernetesInProcessIndexHost(
			context.Background(),
			&indexHostTestVault{secrets: map[domain.SecretRef][]byte{"ok": []byte("tok")}},
			&recordingReporter{},
			fakeIndexerClients{
				dynamic: func(*rest.Config) (dynamic.Interface, error) {
					return newFakeDynamicClient(podsGVR()), nil
				},
				discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
					return newFakeDiscovery(nil), nil
				},
			},
			nil,
		)
		got, err := h.resolveSecret(context.Background(), "ok")
		if err != nil {
			t.Fatalf("resolveSecret: %v", err)
		}
		if string(got) != "tok" {
			t.Fatalf("got %q, want tok", got)
		}
	})
}

// TestNewIndexRuntimeInput_MissingFields covers permanent construction errors.
func TestNewIndexRuntimeInput_MissingFields(t *testing.T) {
	emptyCfg := IndexConfig{}
	if _, err := NewIndexRuntimeInput(
		"",
		testClusterResourceName("x"),
		"https://example",
		"",
		[]byte("tok"),
		"",
		0,
		emptyCfg,
	); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("missing target id: %v", err)
	}
	if _, err := NewIndexRuntimeInput(
		"x",
		testClusterResourceName("x"),
		"",
		"",
		[]byte("tok"),
		"",
		0,
		emptyCfg,
	); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("missing api server: %v", err)
	}
	if _, err := NewIndexRuntimeInput(
		"x",
		"",
		"https://example",
		"",
		[]byte("tok"),
		"",
		0,
		emptyCfg,
	); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("missing cluster resource name: %v", err)
	}
	if _, err := NewIndexRuntimeInput(
		"x",
		"nodes/n1",
		"https://example",
		"",
		[]byte("tok"),
		"",
		0,
		emptyCfg,
	); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("wrong collection: %v", err)
	}
}

// TestIndexingRuntime_FingerprintReplaceOnClusterResourceName stop-and-replaces
// when only the managed cluster parent changes at the same generation.
func TestIndexingRuntime_FingerprintReplaceOnClusterResourceName(t *testing.T) {
	var discClients atomic.Int32
	h := testIndexingRuntime(t, fakeIndexerClients{
		discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
			discClients.Add(1)
			return newFakeDiscovery([]*metav1.APIResourceList{{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			}}), nil
		},
	})

	first, err := NewIndexRuntimeInput(
		"pm-cluster-fp",
		testClusterResourceName("a"),
		"https://example",
		"",
		[]byte("tok"),
		"",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	second, err := NewIndexRuntimeInput(
		"pm-cluster-fp",
		testClusterResourceName("b"),
		"https://example",
		"",
		[]byte("tok"),
		"",
		1,
		IndexConfig{BatchInterval: time.Hour},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), first); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
	if err := h.EnsureIndexer(context.Background(), second); err != nil {
		t.Fatalf("cluster fingerprint replace: %v", err)
	}
	if discClients.Load() < 2 {
		t.Fatalf("discovery clients = %d, want >= 2 after cluster name replace", discClients.Load())
	}
}

// TestCheckDiscoveryReadiness_HardErrorAndEmptyFilter covers readiness failure
// branches beyond allow-list fail-fast.
func TestCheckDiscoveryReadiness_HardErrorAndEmptyFilter(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	t.Run("hard discovery error", func(t *testing.T) {
		disc := &fakeDiscoveryPartial{
			fakeDiscoveryWithPreferred: newFakeDiscovery(nil),
			err:                        errors.New("discovery unavailable"),
			nilResources:               true,
		}
		if err := checkDiscoveryReadiness(disc, IndexConfig{}, logger); err == nil {
			t.Fatal("expected hard discovery error")
		}
	})
	t.Run("empty filtered set without allow list", func(t *testing.T) {
		// Only default-denied resources → filtered set empty, no allow-list.
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "events", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		err := checkDiscoveryReadiness(disc, IndexConfig{}, logger)
		if err == nil {
			t.Fatal("expected empty filtered readiness error")
		}
		if errors.Is(err, ErrIndexerAllowListEmpty) {
			t.Fatalf("empty default-deny filter must not be allow-list error: %v", err)
		}
	})
}

// TestIndexingRuntime_StopDuringReadiness verifies StopIndexer can cancel
// an EnsureIndexer attempt that is blocked in discovery readiness.
func TestIndexingRuntime_StopDuringReadiness(t *testing.T) {
	unblock := make(chan struct{})
	var blocked atomic.Bool
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR()), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return &blockingDiscovery{
					fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
						GroupVersion: "v1",
						APIResources: []metav1.APIResource{
							{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
						},
					}}),
					unblock: unblock,
					onBlock: func() { blocked.Store(true) },
				}, nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	ensureErr := make(chan error, 1)
	go func() {
		ensureErr <- h.EnsureIndexer(context.Background(), testIndexInput("pm-readystop", 1, "tok"))
	}()
	awaitDiscoveryBlocked(t, &blocked)

	stopErr := make(chan error, 1)
	go func() {
		stopErr <- h.StopIndexer(context.Background(), "pm-readystop")
	}()
	// Wait until StopIndexer has marked intentionalStop (and cancelled
	// readiness) before unblocking discovery, so EnsureIndexer cannot
	// race ahead and succeed.
	awaitIndexerIntentionalStop(t, h, "pm-readystop")
	close(unblock)

	if err := <-stopErr; err != nil {
		t.Fatalf("StopIndexer during readiness: %v", err)
	}
	if err := <-ensureErr; err == nil {
		t.Fatal("expected EnsureIndexer to fail after stop during readiness")
	}
	if h.HasIndexer("pm-readystop") {
		t.Fatal("expected no tracked indexer after stop during readiness")
	}
}

// TestIndexingRuntime_JoinInFlightEnsureIndexer verifies concurrent callers
// with the same generation and fingerprint wait on one start attempt.
func TestIndexingRuntime_JoinInFlightEnsureIndexer(t *testing.T) {
	unblock := make(chan struct{})
	var blocked atomic.Bool
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return &blockingDiscovery{
					fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
						GroupVersion: "v1",
						APIResources: []metav1.APIResource{
							{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
						},
					}}),
					unblock: unblock,
					onBlock: func() { blocked.Store(true) },
				}, nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	input := testIndexInput("pm-join", 1, "tok")
	errCh := make(chan error, 2)
	go func() { errCh <- h.EnsureIndexer(context.Background(), input) }()
	awaitDiscoveryBlocked(t, &blocked)
	go func() { errCh <- h.EnsureIndexer(context.Background(), input) }()

	// Second caller is waiting on readyWait; release readiness.
	close(unblock)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("EnsureIndexer: %v", err)
		}
	}
	if !h.HasIndexer("pm-join") {
		t.Fatal("expected joined ensure to leave one ready indexer")
	}
}

// TestIndexingRuntime_StopAllDuringClientBuild_DoesNotHangOnMissingDone
// pins that StopAll during startReady (before cancel/done are published)
// returns promptly and does not wait on a done channel that will never close.
func TestIndexingRuntime_StopAllDuringClientBuild_DoesNotHangOnMissingDone(t *testing.T) {
	unblock := make(chan struct{})
	var blocked atomic.Bool
	h := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				blocked.Store(true)
				<-unblock
				return newFakeDynamicClient(podsGVR()), nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			},
		},
		slog.New(slog.DiscardHandler),
	)

	ensureErr := make(chan error, 1)
	go func() {
		ensureErr <- h.EnsureIndexer(context.Background(), testIndexInput("pm-clientbuild", 1, "tok"))
	}()
	awaitDiscoveryBlocked(t, &blocked)

	stopErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopErr <- h.StopAll(ctx)
	}()
	// Let StopAll observe starting state with nil done before client build resumes.
	time.Sleep(20 * time.Millisecond)
	close(unblock)

	if err := <-ensureErr; err == nil {
		t.Fatal("expected EnsureIndexer to fail after StopAll during client build")
	}
	if err := <-stopErr; err != nil {
		t.Fatalf("StopAll must not time out waiting for unpublished done: %v", err)
	}
	if h.HasIndexer("pm-clientbuild") {
		t.Fatal("expected no tracked indexer after cancelled start")
	}
}

// TestIndexingRuntime_ConcurrentEnsureAndStopAll exercises the cancel/done
// publish path under concurrent EnsureIndexer and StopAll. Run with
// go test -race to catch memory-model races on managedIndexer fields.
func TestIndexingRuntime_ConcurrentEnsureAndStopAll(t *testing.T) {
	h := testIndexingRuntime(t)
	input := testIndexInput("pm-race", 1, "tok")

	const rounds = 40
	for range rounds {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = h.EnsureIndexer(context.Background(), input)
		}()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = h.StopAll(ctx)
		}()
		wg.Wait()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.StopAll(ctx); err != nil {
		t.Fatalf("final StopAll: %v", err)
	}
	if h.HasIndexer("pm-race") {
		t.Fatal("expected no indexer after concurrent ensure/stop rounds")
	}
}
