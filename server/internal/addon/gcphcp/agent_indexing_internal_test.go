package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type recordingIndexingRuntime struct {
	mu        sync.Mutex
	ensures   []kubernetes.IndexRuntimeInput
	stops     []domain.TargetID
	ensureErr error
	stopErr   error
}

func (r *recordingIndexingRuntime) EnsureIndexer(_ context.Context, input kubernetes.IndexRuntimeInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensures = append(r.ensures, input)
	return r.ensureErr
}

func (r *recordingIndexingRuntime) StopIndexer(_ context.Context, targetID domain.TargetID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stops = append(r.stops, targetID)
	return r.stopErr
}

func (r *recordingIndexingRuntime) StopAll(context.Context) error { return nil }

func (r *recordingIndexingRuntime) ensureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ensures)
}

func (r *recordingIndexingRuntime) stopIDs() []domain.TargetID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.TargetID, len(r.stops))
	copy(out, r.stops)
	return out
}

func (r *recordingIndexingRuntime) lastEnsure() kubernetes.IndexRuntimeInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ensures) == 0 {
		return kubernetes.IndexRuntimeInput{}
	}
	return r.ensures[len(r.ensures)-1]
}

func TestAgent_Deliver_EnsureIndexerBeforeDelivered(t *testing.T) {
	withAgentHooksStubbed(t)

	origTimeout := reconcileTimeout
	reconcileTimeout = 10 * time.Second
	t.Cleanup(func() { reconcileTimeout = origTimeout })

	workspaceDir, err := os.MkdirTemp("", "agent-ensure-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	buildCreateWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ []byte, _ string, cleanupCallbacks ...func() error) (*HypershiftWorkspace, error) {
		return &HypershiftWorkspace{
			Env:              []string{"PATH=/usr/bin"},
			JWKSPath:         workspaceDir + "/jwks.json",
			tempDir:          workspaceDir,
			cleanupCallbacks: cleanupCallbacks,
		}, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-new","name":"test-cls"}]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-new":
			fmt.Fprint(w, `{
				"id":"c-new","name":"test-cls","target_project_id":"proj",
				"spec":{
					"infraID":"test-cls","issuerURL":"https://oidc",
					"serviceAccountSigningKey":"key",
					"releaseVersion":"4.22.0","channelGroup":"stable",
					"platform":{"type":"GCP","gcp":{
						"projectID":"proj","region":"us-central1",
						"network":"net","subnet":"sub",
						"endpointAccess":"PublicAndPrivate",
						"workloadIdentity":{}
					}}
				}
			}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/v1/clusters/"):
			fmt.Fprint(w, `{"id":"c-new"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	reporter := newAgentTestReporter()
	runtime := &recordingIndexingRuntime{}
	agent := NewAgent(AgentDeps{
		Gateway:         GatewayConfig{URL: server.URL, Audience: "test-audience"},
		Infra:           NewInfraRunner(),
		Reporter:        reporter,
		IndexingRuntime: runtime,
	})

	spec := json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"w","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)

	err = agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{Properties: map[string]string{
			"id": "target-1", "gcp_project": "proj", "region": "us-central1",
			"workforce_pool": "pool", "workforce_provider": "prov",
			"broker_sa_email": "broker@example.com",
		}}),
		domain.DeliveryID("delivery-ensure"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Raw:          managedResourceRaw("clusters/test-cls", spec),
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		3,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("state = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}

	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1", runtime.ensureCount())
	}
	got := runtime.lastEnsure()
	if got.TargetID != GuestTargetID("test-cls") {
		t.Fatalf("Ensure TargetID = %q, want %q", got.TargetID, GuestTargetID("test-cls"))
	}
	if got.ClusterResourceName != "clusters/test-cls" {
		t.Fatalf("Ensure ClusterResourceName = %q, want clusters/test-cls", got.ClusterResourceName)
	}
	if got.Generation != 3 {
		t.Fatalf("Ensure Generation = %d, want 3", got.Generation)
	}
	if string(got.Credential) != "sa-token-value" {
		t.Fatalf("Ensure Credential = %q, want sa-token-value", got.Credential)
	}
	if got.APIServer != "https://guest.example:6443" {
		t.Fatalf("Ensure APIServer = %q, want https://guest.example:6443", got.APIServer)
	}
}

func TestAgent_Deliver_EnsureIndexerFailureFailsDelivery(t *testing.T) {
	withAgentHooksStubbed(t)

	origTimeout := reconcileTimeout
	reconcileTimeout = 10 * time.Second
	t.Cleanup(func() { reconcileTimeout = origTimeout })

	workspaceDir, err := os.MkdirTemp("", "agent-ensure-fail-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	buildCreateWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ []byte, _ string, cleanupCallbacks ...func() error) (*HypershiftWorkspace, error) {
		return &HypershiftWorkspace{
			Env:              []string{"PATH=/usr/bin"},
			JWKSPath:         workspaceDir + "/jwks.json",
			tempDir:          workspaceDir,
			cleanupCallbacks: cleanupCallbacks,
		}, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-new","name":"fail-cls"}]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-new":
			fmt.Fprint(w, `{
				"id":"c-new","name":"fail-cls","target_project_id":"proj",
				"spec":{
					"infraID":"fail-cls","issuerURL":"https://oidc",
					"serviceAccountSigningKey":"key",
					"releaseVersion":"4.22.0","channelGroup":"stable",
					"platform":{"type":"GCP","gcp":{
						"projectID":"proj","region":"us-central1",
						"network":"net","subnet":"sub",
						"endpointAccess":"PublicAndPrivate",
						"workloadIdentity":{}
					}}
				}
			}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/v1/clusters/"):
			fmt.Fprint(w, `{"id":"c-new"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	reporter := newAgentTestReporter()
	runtime := &recordingIndexingRuntime{ensureErr: kubernetes.ErrStaleIndexerGeneration}
	agent := NewAgent(AgentDeps{
		Gateway:         GatewayConfig{URL: server.URL, Audience: "test-audience"},
		Infra:           NewInfraRunner(),
		Reporter:        reporter,
		IndexingRuntime: runtime,
	})

	spec := json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"w","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)

	err = agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{Properties: map[string]string{
			"id": "target-1", "gcp_project": "proj", "region": "us-central1",
			"workforce_pool": "pool", "workforce_provider": "prov",
			"broker_sa_email": "broker@example.com",
		}}),
		domain.DeliveryID("delivery-ensure-fail"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Raw:          managedResourceRaw("clusters/fail-cls", spec),
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateFailed {
			t.Fatalf("state = %q, want %q; message = %q", result.State, domain.DeliveryStateFailed, result.Message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}

	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1 (permanent fail-fast)", runtime.ensureCount())
	}
}

func TestAgent_Remove_StopIndexer(t *testing.T) {
	withAgentHooksStubbed(t)

	buildDestroyWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		dir, err := os.MkdirTemp("", "agent-stop-test-*")
		if err != nil {
			return nil, err
		}
		return &HypershiftWorkspace{
			Env:     []string{"PATH=/usr/bin"},
			tempDir: dir,
		}, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-del","name":"stop-cls"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-del":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-del":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	reporter := newAgentTestReporter()
	runtime := &recordingIndexingRuntime{stopErr: domain.ErrNotFound}
	infra := &fakeCleanupInfra{}
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
			infra:   infra,
		},
		observer:        noopObserver{},
		reporter:        reporter,
		indexingRuntime: runtime,
		trustMap:        make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen:      make(map[string]domain.Generation),
	}

	spec := json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"w","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{Properties: map[string]string{
			"id": "target-1", "gcp_project": "proj", "region": "us-central1",
			"workforce_pool": "pool", "workforce_provider": "prov",
			"broker_sa_email": "broker@example.com",
		}}),
		domain.DeliveryID("remove-stop"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Raw:          managedResourceRaw("clusters/stop-cls", spec),
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("state = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reporter completion")
	}

	stops := runtime.stopIDs()
	if len(stops) != 1 || stops[0] != GuestTargetID("stop-cls") {
		t.Fatalf("StopIndexer calls = %v, want [%s]", stops, GuestTargetID("stop-cls"))
	}
}

// TestAgent_RecoverActiveDelete_StopIndexer verifies crash-recovery delete
// calls StopIndexer at the start of teardown.
func TestAgent_RecoverActiveDelete_StopIndexer(t *testing.T) {
	withAgentHooksStubbed(t)

	buildDestroyWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		dir, err := os.MkdirTemp("", "agent-recover-stop-*")
		if err != nil {
			return nil, err
		}
		return &HypershiftWorkspace{
			Env:     []string{"PATH=/usr/bin"},
			tempDir: dir,
		}, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-rec","name":"recover-del"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-rec":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-rec":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	spec := json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"w","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)
	ad := domain.ActiveDelivery{
		Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:         domain.DeliveryID("recovery-del-idx"),
			Generation: 2,
			State:      domain.DeliveryStateProgressing,
			Operation:  domain.DeliveryOperationRemove,
			Manifests: []domain.Manifest{{
				ManifestType: ClusterManifestType,
				ManifestID:   "f47ac10b-58cc-4372-a567-0e02b2c3d479",
				Raw:          managedResourceRaw("clusters/recover-del", spec),
			}},
		}),
		Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "target-1",
			Properties: map[string]string{
				"id": "target-1", "gcp_project": "proj", "region": "us-central1",
				"workforce_pool": "pool", "workforce_provider": "prov",
				"broker_sa_email": "broker@example.com",
			},
		}),
		Auth: domain.DeliveryAuth{Token: "caller-token"},
	}

	reporter := &indexingRecoveryReporter{
		agentTestReporter: newAgentTestReporter(),
		active:            []domain.ActiveDelivery{ad},
	}
	runtime := &recordingIndexingRuntime{}
	infra := &fakeCleanupInfra{}
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
			infra:   infra,
		},
		observer:        noopObserver{},
		reporter:        reporter,
		indexingRuntime: runtime,
		trustMap:        make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen:      make(map[string]domain.Generation),
	}

	if err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"}); err != nil {
		t.Fatalf("RecoverActiveDeliveries: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State == "" {
			t.Fatal("expected non-empty delivery state from recovered delete")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered delete result")
	}

	stops := runtime.stopIDs()
	if len(stops) != 1 || stops[0] != GuestTargetID("recover-del") {
		t.Fatalf("StopIndexer calls = %v, want [%s]", stops, GuestTargetID("recover-del"))
	}
}

// indexingRecoveryReporter lists fixed active deliveries for recovery tests.
type indexingRecoveryReporter struct {
	*agentTestReporter
	active []domain.ActiveDelivery
}

// ListActiveDeliveries returns the configured active deliveries.
func (r *indexingRecoveryReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return r.active, nil
}
