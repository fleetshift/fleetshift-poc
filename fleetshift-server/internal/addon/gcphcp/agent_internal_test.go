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
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type reconcileContextKey string

func TestNewReconcileContext_AddsDeadlineAndPreservesValues(t *testing.T) {
	origTimeout := reconcileTimeout
	reconcileTimeout = 25 * time.Millisecond
	defer func() {
		reconcileTimeout = origTimeout
	}()

	requestCtx, cancel := context.WithCancel(context.Background())
	requestCtx = context.WithValue(requestCtx, reconcileContextKey("trace-id"), "trace-123")
	cancel()

	runCtx, runCancel := newReconcileContext(context.WithoutCancel(requestCtx))
	defer runCancel()

	if got := runCtx.Value(reconcileContextKey("trace-id")); got != "trace-123" {
		t.Fatalf("context value = %v, want trace-123", got)
	}

	deadline, ok := runCtx.Deadline()
	if !ok {
		t.Fatal("expected reconcile context to have a deadline")
	}

	if remaining := time.Until(deadline); remaining <= 0 || remaining > reconcileTimeout+100*time.Millisecond {
		t.Fatalf("deadline remaining = %v, want within (0, %v]", remaining, reconcileTimeout+100*time.Millisecond)
	}

	select {
	case <-runCtx.Done():
		t.Fatal("reconcile context should not be done immediately")
	default:
	}
}

func TestAcceptGeneration_FirstDeliveryAccepted(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	if !agent.acceptGeneration("cluster-a", 5) {
		t.Fatal("first delivery should be accepted")
	}
}

func TestAcceptGeneration_AcceptsNewerGeneration(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 5)
	if !agent.acceptGeneration("cluster-a", 10) {
		t.Fatal("newer generation should be accepted")
	}
}

func TestAcceptGeneration_RejectsStaleGeneration(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if agent.acceptGeneration("cluster-a", 5) {
		t.Fatal("stale generation should be rejected")
	}
}

func TestAcceptGeneration_AcceptsSameGenerationRetry(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if !agent.acceptGeneration("cluster-a", 10) {
		t.Fatal("same-generation retry should be accepted")
	}
}

func TestAcceptGeneration_IndependentPerCluster(t *testing.T) {
	agent := &Agent{clusterGen: make(map[string]domain.Generation)}

	agent.acceptGeneration("cluster-a", 10)
	if !agent.acceptGeneration("cluster-b", 5) {
		t.Fatal("different cluster should track independently")
	}
}

func TestDeliveryResultForReconcileError_AuthExpiredReturnsAuthFailed(t *testing.T) {
	err := newAuthExpiredError(fmt.Errorf("CLS API GET /api/v1/clusters failed (HTTP 401): token expired"))
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "credentials expired") {
		t.Fatalf("message = %q, want 'credentials expired' context", result.Message)
	}
	if !strings.Contains(result.Message, "401") {
		t.Fatalf("message = %q, want wrapped cause mentioning 401", result.Message)
	}
}

func TestDeliveryResultForReconcileError_AuthExpiredTakesPrecedenceOverPostProvision(t *testing.T) {
	inner := newPostProvisionRegistrationError(fmt.Errorf("bootstrap failed"))
	err := newAuthExpiredError(inner)
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q — auth expired should take precedence", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliveryResultForReconcileError_WrappedAuthExpiredReturnsAuthFailed(t *testing.T) {
	authErr := newAuthExpiredError(fmt.Errorf("IAM returned status 401"))
	wrapped := fmt.Errorf("broker auth exchange: %w", authErr)
	result := deliveryResultForReconcileError(wrapped)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliveryResultForReconcileError_SubprocessInvalidGrantReturnsAuthFailed(t *testing.T) {
	// Simulates hypershift binary returning invalid_grant in stderr,
	// which is NOT wrapped with newAuthExpiredError because the error
	// comes from subprocess output, not from a parsed Go error chain.
	err := fmt.Errorf("create infra: hypershift create infra failed: exit status 1 " +
		`(stderr: {"error":"invalid_grant","error_description":"ID Token issued at 1779857657 is stale to sign-in."})`)
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q — invalid_grant in subprocess stderr should be detected as auth failure",
			result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "credentials expired") {
		t.Fatalf("message = %q, want 'credentials expired' context", result.Message)
	}
}

func TestDeliveryResultForReconcileError_WrappedSubprocessInvalidGrantReturnsAuthFailed(t *testing.T) {
	inner := fmt.Errorf("hypershift create infra failed: exit status 1 " +
		`(stderr: credentials: status code 400: {"error":"invalid_grant","error_description":"ID Token is stale"})`)
	err := fmt.Errorf("create infra: attempt 1: %w\nattempt 2: %w", inner, inner)
	result := deliveryResultForReconcileError(err)

	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliveryResultForReconcileError_PostProvisionRegistrationErrorUsesExplicitMessage(t *testing.T) {
	result := deliveryResultForReconcileError(
		newPostProvisionRegistrationError(fmt.Errorf("bootstrap guest cluster after 3 attempts: RBAC not ready")),
	)

	if result.State != domain.DeliveryStateFailed {
		t.Fatalf("state = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if !strings.Contains(result.Message, "cluster provisioned and management-plane ready") {
		t.Fatalf("message = %q, want management-plane ready context", result.Message)
	}
	if !strings.Contains(result.Message, "guest target registration did not complete") {
		t.Fatalf("message = %q, want guest registration context", result.Message)
	}
	if !strings.Contains(result.Message, "RBAC not ready") {
		t.Fatalf("message = %q, want wrapped bootstrap cause", result.Message)
	}
}

type agentTestReporter struct {
	mu      sync.Mutex
	results map[domain.DeliveryID]domain.DeliveryResult
	done    chan domain.DeliveryResult
}

func newAgentTestReporter() *agentTestReporter {
	return &agentTestReporter{
		results: make(map[domain.DeliveryID]domain.DeliveryResult),
		done:    make(chan domain.DeliveryResult, 10),
	}
}

func (r *agentTestReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, _ domain.DeliveryEvent) error {
	return nil
}

func (r *agentTestReporter) ReportResult(_ context.Context, id domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results[id] = result
	r.done <- result
	return nil
}

func (r *agentTestReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func withAgentHooksStubbed(t *testing.T) {
	t.Helper()
	origNewBrokerAuth := newBrokerAuth
	origBuildCreateWorkspace := buildCreateWorkspaceWithTokenURL
	origBuildDestroyWorkspace := buildDestroyWorkspaceWithTokenURL
	origReconcileNodepools := reconcileNodepoolsFn
	origPollClusterReady := pollClusterReadyFn
	origCompleteGuestRegistration := completeGuestRegistrationFn
	origPollDesiredNodepoolsHealthy := pollDesiredNodepoolsHealthyFn
	t.Cleanup(func() {
		newBrokerAuth = origNewBrokerAuth
		buildCreateWorkspaceWithTokenURL = origBuildCreateWorkspace
		buildDestroyWorkspaceWithTokenURL = origBuildDestroyWorkspace
		reconcileNodepoolsFn = origReconcileNodepools
		pollClusterReadyFn = origPollClusterReady
		completeGuestRegistrationFn = origCompleteGuestRegistration
		pollDesiredNodepoolsHealthyFn = origPollDesiredNodepoolsHealthy
	})

	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return &fakeBrokerAuth{
			result: BrokerAuthResult{
				BrokerToken:          "broker-token",
				BrokerEmail:          "broker@example.com",
				WorkforceToken:       "workforce-token",
				WorkforceTokenExpiry: time.Now().Add(time.Hour),
			},
		}
	}

	reconcileNodepoolsFn = func(context.Context, nodepoolReconcileClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}
	pollClusterReadyFn = func(context.Context, *CLSClient, string, *deliveryProgress) error {
		return nil
	}
	completeGuestRegistrationFn = func(_ context.Context, _ *CLSClient, _ string, _ string, targetID domain.TargetID, _ *deliveryProgress) (string, BootstrapResult, error) {
		return "https://guest.example:6443", BootstrapResult{
			SATokenRef: DeliverySecretRef(targetID),
			SAToken:    []byte("sa-token-value"),
		}, nil
	}
	pollDesiredNodepoolsHealthyFn = func(context.Context, nodepoolStatusClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}
}

func TestAgent_Deliver_SuccessReportsProvisionedTargetAndSecrets(t *testing.T) {
	withAgentHooksStubbed(t)

	origTimeout := reconcileTimeout
	reconcileTimeout = 10 * time.Second
	t.Cleanup(func() { reconcileTimeout = origTimeout })

	workspaceDir, err := os.MkdirTemp("", "agent-deliver-test-*")
	if err != nil {
		t.Fatalf("os.MkdirTemp() error = %v", err)
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
	infra := NewInfraRunner()
	agent := NewAgent(AgentDeps{
		Gateway:  GatewayConfig{URL: server.URL, Audience: "test-audience"},
		Infra:    infra,
		Reporter: reporter,
	})

	autoRepair := true
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
		domain.DeliveryID("delivery-success"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		1,
	)
	_ = autoRepair
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("state = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
		}
		if len(result.ProvisionedTargets) != 1 {
			t.Fatalf("ProvisionedTargets count = %d, want 1", len(result.ProvisionedTargets))
		}
		pt := result.ProvisionedTargets[0]
		if pt.Type != KubernetesTargetType {
			t.Fatalf("target type = %q, want %q", pt.Type, KubernetesTargetType)
		}
		if pt.Properties["api_server"] != "https://guest.example:6443" {
			t.Fatalf("api_server = %q, want https://guest.example:6443", pt.Properties["api_server"])
		}
		if len(result.ProducedSecrets) != 1 {
			t.Fatalf("ProducedSecrets count = %d, want 1", len(result.ProducedSecrets))
		}
		if string(result.ProducedSecrets[0].Value) != "sa-token-value" {
			t.Fatalf("secret value = %q, want sa-token-value", string(result.ProducedSecrets[0].Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}
}

func TestAgent_Remove_DeletesClusterViaReconciler(t *testing.T) {
	withAgentHooksStubbed(t)

	buildDestroyWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		dir, err := os.MkdirTemp("", "agent-remove-test-*")
		if err != nil {
			return nil, err
		}
		return &HypershiftWorkspace{
			Env:     []string{"PATH=/usr/bin"},
			tempDir: dir,
		}, nil
	}

	var deleteRequested atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-del","name":"test-cls"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-del":
			deleteRequested.Store(true)
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
	infra := &fakeCleanupInfra{}
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
			infra:   infra,
		},
		observer:   noopObserver{},
		reporter:   reporter,
		trustMap:   make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen: make(map[string]domain.Generation),
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
		domain.DeliveryID("remove-1"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if !deleteRequested.Load() {
		t.Fatal("expected CLS DeleteCluster to be called")
	}
	if infra.waitPSCCalls != 1 {
		t.Fatalf("expected PSC cleanup, got %d calls", infra.waitPSCCalls)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("reporter state = %q, want %q; message = %q",
				result.State, domain.DeliveryStateDelivered, result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reporter completion signal")
	}
}

func TestAgent_Remove_ClearsGenerationSoRecreateIsAccepted(t *testing.T) {
	withAgentHooksStubbed(t)

	buildDestroyWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		dir, err := os.MkdirTemp("", "agent-recreate-test-*")
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
			fmt.Fprint(w, `{"clusters":[{"id":"c-del","name":"test-cls"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-del":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-del":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	reporter := newAgentTestReporter()
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
			infra:   &fakeCleanupInfra{},
		},
		observer:   noopObserver{},
		reporter:   reporter,
		trustMap:   make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen: make(map[string]domain.Generation),
	}

	spec := json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"w","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{Properties: map[string]string{
		"id": "target-1", "gcp_project": "proj", "region": "us-central1",
		"workforce_pool": "pool", "workforce_provider": "prov",
		"broker_sa_email": "broker@example.com",
	}})
	manifest := domain.Manifest{
		ManifestType: ClusterManifestType,
		ManifestID:   "test-cls",
		Raw:          spec,
	}

	// Seed generation high-water mark as if prior deliveries happened.
	agent.acceptGeneration("test-cls", 5)

	// Remove with generation 6 — succeeds and should clear generation state.
	err := agent.Remove(
		context.Background(),
		target,
		domain.DeliveryID("remove-1"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		6,
	)
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("reporter state = %q, want %q", result.State, domain.DeliveryStateDelivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reporter completion signal")
	}

	// After delete, a fresh delivery with generation 0 must be accepted.
	if !agent.acceptGeneration("test-cls", 0) {
		t.Fatal("generation 0 should be accepted after delete cleared the high-water mark")
	}
}

func TestAgent_Remove_AuthExpiredSignalsAuthFailed(t *testing.T) {
	withAgentHooksStubbed(t)

	// Override newBrokerAuth to return an auth expired error.
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return &fakeBrokerAuth{
			err: newAuthExpiredError(fmt.Errorf("STS returned status 400: invalid_grant: ID Token is stale")),
		}
	}

	reporter := newAgentTestReporter()
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: "https://unused", Audience: "test-audience"},
			infra:   &fakeCleanupInfra{},
		},
		observer:   noopObserver{},
		reporter:   reporter,
		trustMap:   make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen: make(map[string]domain.Generation),
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
		domain.DeliveryID("remove-auth-fail"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "stale-token"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Remove() should return nil (auth failures signal via reporter), got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateAuthFailed {
			t.Fatalf("reporter state = %q, want %q; message = %q",
				result.State, domain.DeliveryStateAuthFailed, result.Message)
		}
		if !strings.Contains(result.Message, "auth expired") {
			t.Fatalf("reporter message = %q, want auth expired context", result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reporter auth failure signal")
	}
}

func TestAgent_Remove_SubprocessInvalidGrantSignalsAuthFailed(t *testing.T) {
	withAgentHooksStubbed(t)

	buildDestroyWorkspaceWithTokenURL = func(_ string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		dir, err := os.MkdirTemp("", "agent-remove-invalid-grant-*")
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
			fmt.Fprint(w, `{"clusters":[{"id":"c-del","name":"test-cls"}]}`)
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
	agent := &Agent{
		reconciler: &Reconciler{
			gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
			infra: &fakeCleanupInfra{
				destroyInfraErr: fmt.Errorf(
					`hypershift destroy infra failed: exit status 1 (output: credentials: status code 400: {"error":"invalid_grant","error_description":"cached workforce token expired"})`,
				),
			},
		},
		observer:   noopObserver{},
		reporter:   reporter,
		trustMap:   make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen: make(map[string]domain.Generation),
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
		domain.DeliveryID("remove-invalid-grant"),
		[]domain.Manifest{{
			ManifestType: ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "caller-token"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Remove() should return nil (auth failures signal via reporter), got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateAuthFailed {
			t.Fatalf("reporter state = %q, want %q; message = %q",
				result.State, domain.DeliveryStateAuthFailed, result.Message)
		}
		if !strings.Contains(result.Message, "auth expired") {
			t.Fatalf("reporter message = %q, want auth expired context", result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reporter auth failure signal")
	}
}
