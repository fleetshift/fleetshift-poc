package kubernetes

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// staticTargetLister returns a fixed target list or a fixed list error.
type staticTargetLister struct {
	targets []domain.TargetInfo
	err     error
}

// ListTargets implements [TargetLister].
func (l staticTargetLister) ListTargets(context.Context) ([]domain.TargetInfo, error) {
	if l.err != nil {
		return nil, l.err
	}
	out := make([]domain.TargetInfo, len(l.targets))
	copy(out, l.targets)
	return out, nil
}

func TestReplayPersistedIndexers_StartsResolvableKubernetesTargets(t *testing.T) {
	host := testIndexingRuntime(t)
	target := readyKubeTarget("replay-1", map[string]string{
		PropAPIServer:           "https://example",
		PropServiceAccountToken: "tok",
	})
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{targets: []domain.TargetInfo{target}},
		nil,
		host,
		slog.New(slog.DiscardHandler),
	)
	if !host.HasIndexer("replay-1") {
		t.Fatal("expected startup replay to EnsureIndexer for resolvable target")
	}
}

func TestReplayPersistedIndexers_ResolvesVaultCredential(t *testing.T) {
	host := testIndexingRuntime(t)
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/replay-vault/sa-token": []byte("vault-tok"),
	}}
	target := readyKubeTarget("replay-vault", map[string]string{
		PropAPIServer:              "https://example",
		PropServiceAccountTokenRef: "targets/replay-vault/sa-token",
	})
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{targets: []domain.TargetInfo{target}},
		vault,
		host,
		slog.New(slog.DiscardHandler),
	)
	if !host.HasIndexer("replay-vault") {
		t.Fatal("expected startup replay to EnsureIndexer using vault credential")
	}
}

func TestReplayPersistedIndexers_SkipsMissingCredentials(t *testing.T) {
	host := testIndexingRuntime(t)
	target := readyKubeTarget("replay-skip", map[string]string{
		PropAPIServer: "https://example",
	})
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{targets: []domain.TargetInfo{target}},
		nil,
		host,
		slog.New(slog.DiscardHandler),
	)
	if host.HasIndexer("replay-skip") {
		t.Fatal("expected target without credentials to be skipped")
	}
}

func TestReplayPersistedIndexers_SkipsNonKubernetesTargets(t *testing.T) {
	host := testIndexingRuntime(t)
	other := domain.NewTargetInfo(
		"other-1",
		domain.TargetType("kind"),
		"Other",
		domain.TargetStateReady,
		nil,
		map[string]string{
			PropAPIServer:           "https://example",
			PropServiceAccountToken: "tok",
		},
		nil,
	)
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{targets: []domain.TargetInfo{other}},
		nil,
		host,
		slog.New(slog.DiscardHandler),
	)
	if host.HasIndexer("other-1") {
		t.Fatal("expected non-kubernetes target to be skipped")
	}
}

func TestReplayPersistedIndexers_EnsureFailureIsSkipped(t *testing.T) {
	failing := &failEnsureRuntime{err: ErrStaleIndexerGeneration}
	target := readyKubeTarget("replay-fail", map[string]string{
		PropAPIServer:           "https://example",
		PropServiceAccountToken: "tok",
	})
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{targets: []domain.TargetInfo{target}},
		nil,
		failing,
		slog.New(slog.DiscardHandler),
	)
	if failing.calls != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1 (permanent fail-fast, continue)", failing.calls)
	}
}

func TestReplayPersistedIndexers_ListFailureContinues(t *testing.T) {
	host := testIndexingRuntime(t)
	ReplayPersistedIndexers(
		context.Background(),
		staticTargetLister{err: domain.ErrNotFound},
		nil,
		host,
		slog.New(slog.DiscardHandler),
	)
}

// TestReplayPersistedIndexers_MissesTargetsCommittedAfterList verifies that
// ReplayPersistedIndexers only starts targets returned by its ListTargets
// call. A target added to the lister after that call returns is not started.
func TestReplayPersistedIndexers_MissesTargetsCommittedAfterList(t *testing.T) {
	host := testIndexingRuntime(t)
	listed := readyKubeTarget("replay-listed", map[string]string{
		PropAPIServer:           "https://example",
		PropServiceAccountToken: "tok",
	})
	late := readyKubeTarget("replay-late", map[string]string{
		PropAPIServer:           "https://example",
		PropServiceAccountToken: "tok",
	})
	lister := &mutatingTargetLister{targets: []domain.TargetInfo{listed}}

	ReplayPersistedIndexers(
		context.Background(),
		lister,
		nil,
		host,
		slog.New(slog.DiscardHandler),
	)
	if !host.HasIndexer("replay-listed") {
		t.Fatal("expected listed target to be started")
	}

	// Target appears only after replay's list completed.
	lister.targets = append(lister.targets, late)
	if host.HasIndexer("replay-late") {
		t.Fatal("target absent from ListTargets snapshot must not be started by completed replay")
	}
}

// mutatingTargetLister returns a copy of its current targets slice so tests
// can append targets after ReplayPersistedIndexers has listed.
type mutatingTargetLister struct {
	targets []domain.TargetInfo
}

// ListTargets implements [TargetLister].
func (l *mutatingTargetLister) ListTargets(context.Context) ([]domain.TargetInfo, error) {
	out := make([]domain.TargetInfo, len(l.targets))
	copy(out, l.targets)
	return out, nil
}

// failEnsureRuntime is an IndexingRuntime that records EnsureIndexer calls
// and always returns err.
type failEnsureRuntime struct {
	calls int
	err   error
}

// EnsureIndexer increments calls and returns f.err.
func (f *failEnsureRuntime) EnsureIndexer(context.Context, IndexRuntimeInput) error {
	f.calls++
	return f.err
}

// StopIndexer implements [IndexingRuntime].
func (f *failEnsureRuntime) StopIndexer(context.Context, domain.TargetID) error { return nil }

// StopAll implements [IndexingRuntime].
func (f *failEnsureRuntime) StopAll(context.Context) error { return nil }
