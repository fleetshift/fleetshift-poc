package kubernetes

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// indexHostTestVault is an in-memory [domain.Vault] for indexing unit tests.
type indexHostTestVault struct {
	secrets map[domain.SecretRef][]byte
}

// Get implements [domain.Vault].
func (v *indexHostTestVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	val, ok := v.secrets[ref]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return val, nil
}

// Put implements [domain.Vault].
func (v *indexHostTestVault) Put(_ context.Context, ref domain.SecretRef, val []byte) error {
	v.secrets[ref] = val
	return nil
}

// Delete implements [domain.Vault].
func (v *indexHostTestVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

// readyKubeTarget builds a ready Kubernetes [domain.TargetInfo] for tests.
// nil props get a default API server and direct token.
func readyKubeTarget(id string, props map[string]string) domain.TargetInfo {
	if props == nil {
		props = map[string]string{
			PropAPIServer:           "https://127.0.0.1:6443",
			PropServiceAccountToken: "token",
		}
	}
	return domain.NewTargetInfo(
		domain.TargetID(id),
		TargetType,
		"Test Cluster",
		domain.TargetStateReady,
		nil,
		props,
		nil,
	)
}

// ensureReadyTarget calls EnsureIndexer for target with cfg (defaulting BatchInterval).
func ensureReadyTarget(t *testing.T, host *KubernetesInProcessIndexHost, target domain.TargetInfo, cfg IndexConfig) {
	t.Helper()
	props := target.Properties()
	cred := props[PropServiceAccountToken]
	if cred == "" {
		cred = "token"
	}
	if cfg.BatchInterval == 0 {
		cfg.BatchInterval = time.Hour
	}
	input := IndexRuntimeInput{
		TargetID:    target.ID(),
		APIServer:   props[PropAPIServer],
		CACert:      props[PropCACert],
		Credential:  []byte(cred),
		Generation:  1,
		IndexConfig: cfg,
	}
	if input.APIServer == "" {
		input.APIServer = "https://example"
	}
	if err := host.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
}

// blockingDiscovery hangs in ServerPreferredResources until unblock is closed.
type blockingDiscovery struct {
	*fakeDiscoveryWithPreferred
	unblock <-chan struct{}
	onBlock func()
}

// ServerPreferredResources blocks until unblock is closed, then delegates.
func (d *blockingDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	if d.onBlock != nil {
		d.onBlock()
	}
	<-d.unblock
	return d.fakeDiscoveryWithPreferred.ServerPreferredResources()
}

// readyThenBlockDiscovery succeeds once for EnsureIndexer readiness, then
// blocks on later ServerPreferredResources calls until unblock is closed.
type readyThenBlockDiscovery struct {
	*fakeDiscoveryWithPreferred
	unblock <-chan struct{}
	onBlock func()
	calls   atomic.Int32
}

// ServerPreferredResources returns immediately on the first call, then blocks.
func (d *readyThenBlockDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	if d.calls.Add(1) == 1 {
		return d.fakeDiscoveryWithPreferred.ServerPreferredResources()
	}
	if d.onBlock != nil {
		d.onBlock()
	}
	<-d.unblock
	return d.fakeDiscoveryWithPreferred.ServerPreferredResources()
}

// newHostWithReadyThenBlockDiscovery returns a host whose discovery succeeds
// once (EnsureIndexer readiness), then blocks on later discovery calls, plus
// a flag set when the blocking call begins.
func newHostWithReadyThenBlockDiscovery(t *testing.T, unblock <-chan struct{}) (*KubernetesInProcessIndexHost, *atomic.Bool) {
	t.Helper()
	var blocked atomic.Bool
	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
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
		nil,
	)
	return host, &blocked
}

// awaitDiscoveryBlocked waits until the blocking discovery path has been entered.
func awaitDiscoveryBlocked(t *testing.T, blocked *atomic.Bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !blocked.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for discovery to block")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
