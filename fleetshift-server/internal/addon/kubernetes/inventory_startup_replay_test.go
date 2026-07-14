package kubernetes

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type staticTargetLister struct {
	targets []domain.TargetInfo
	err     error
}

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

type failEnsureRuntime struct {
	calls int
	err   error
}

func (f *failEnsureRuntime) EnsureIndexer(context.Context, IndexRuntimeInput) error {
	f.calls++
	return f.err
}

func (f *failEnsureRuntime) StopIndexer(context.Context, domain.TargetID) error { return nil }

func (f *failEnsureRuntime) StopAll(context.Context) error { return nil }

