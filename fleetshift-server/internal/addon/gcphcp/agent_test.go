package gcphcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type recordingReporter struct {
	mu      sync.Mutex
	results map[domain.DeliveryID]domain.DeliveryResult
	done    chan domain.DeliveryResult
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{
		results: make(map[domain.DeliveryID]domain.DeliveryResult),
		done:    make(chan domain.DeliveryResult, 10),
	}
}

func (r *recordingReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, _ domain.DeliveryEvent) error {
	return nil
}

func (r *recordingReporter) ReportResult(_ context.Context, id domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results[id] = result
	r.done <- result
	return nil
}

func (r *recordingReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func newTestAgent(reporter domain.DeliveryReporter) *gcphcp.Agent {
	return gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})
}

func TestAgent_Deliver_RejectsMissingName(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	manifest := domain.Manifest{
		ManifestType: gcphcp.ClusterManifestType,
		Raw:          json.RawMessage(`{}`),
	}

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "test-token"},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateFailed {
			t.Errorf("expected state %s, got %s", domain.DeliveryStateFailed, result.State)
		}
		if result.Message == "" {
			t.Error("expected non-empty error message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}
}

func TestAgent_Deliver_TrustBundleOnly(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	trustBundle := domain.TrustBundleEntry{
		IssuerURL:          "https://test-issuer",
		JWKSURI:            "https://test-jwks",
		EnrollmentAudience: "test-audience",
	}
	trustBundleJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("failed to marshal trust bundle: %v", err)
	}

	manifest := domain.Manifest{
		ManifestType: domain.TrustBundleManifestType,
		Raw:          json.RawMessage(trustBundleJSON),
	}

	err = agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("expected state %s, got %s", domain.DeliveryStateDelivered, result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != trustBundle.IssuerURL {
		t.Errorf("expected issuer URL %s, got %s", trustBundle.IssuerURL, bundles[0].IssuerURL)
	}
}

func TestAgent_Deliver_TrustBundleOnly_CompletesEvenIfRequestContextCanceled(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	trustBundle := domain.TrustBundleEntry{
		IssuerURL:          "https://test-issuer",
		JWKSURI:            "https://test-jwks",
		EnrollmentAudience: "test-audience",
	}
	trustBundleJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("failed to marshal trust bundle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = agent.Deliver(
		ctx,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{{
			ManifestType: domain.TrustBundleManifestType,
			Raw:          json.RawMessage(trustBundleJSON),
		}},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("expected state %s, got %s", domain.DeliveryStateDelivered, result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for completion signal")
	}
}

func TestAgent_Deliver_TrustBundleOnly_ReplacesExistingIssuerEntry(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks-1",
		EnrollmentAudience: "audience-1",
	})
	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks-2",
		EnrollmentAudience: "audience-2",
	})

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle after replacement, got %d", len(bundles))
	}
	if bundles[0].JWKSURI != "https://issuer.example.com/jwks-2" {
		t.Fatalf("JWKSURI = %q, want replacement value", bundles[0].JWKSURI)
	}
	if bundles[0].EnrollmentAudience != "audience-2" {
		t.Fatalf("EnrollmentAudience = %q, want replacement value", bundles[0].EnrollmentAudience)
	}
}

func TestAgent_TrustBundles_ReturnsEntriesSortedByIssuer(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer-b.example.com",
		JWKSURI:            "https://issuer-b.example.com/jwks",
		EnrollmentAudience: "audience-b",
	})
	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer-a.example.com",
		JWKSURI:            "https://issuer-a.example.com/jwks",
		EnrollmentAudience: "audience-a",
	})

	bundles := agent.TrustBundles()
	if len(bundles) != 2 {
		t.Fatalf("expected 2 trust bundles, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != "https://issuer-a.example.com" {
		t.Fatalf("bundles[0].IssuerURL = %q, want issuer-a first", bundles[0].IssuerURL)
	}
	if bundles[1].IssuerURL != "https://issuer-b.example.com" {
		t.Fatalf("bundles[1].IssuerURL = %q, want issuer-b second", bundles[1].IssuerURL)
	}
}

func TestAgent_Remove_TrustBundle_RemovesStoredIssuerEntry(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	entry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "audience-1",
	}
	deliverTrustBundle(t, agent, reporter, entry)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	if bundles := agent.TrustBundles(); len(bundles) != 0 {
		t.Fatalf("expected trust bundle removal, got %#v", bundles)
	}
}

func TestAgent_Deliver_RejectsStaleGeneration(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	spec := validClusterSpecJSON(t)
	manifest := domain.Manifest{
		ManifestType: gcphcp.ClusterManifestType,
		ManifestID:   "test-cls",
		Raw:          spec,
	}

	// First delivery with generation 10 — accepted (will fail async since no real backend, but generation is accepted)
	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("delivery-1"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "token"},
		nil,
		10,
	)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	// Drain the async result from delivery-1 (it will fail because there's no real backend)
	select {
	case <-reporter.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first delivery result")
	}

	// Second delivery with stale generation 5 — should be rejected
	err = agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("delivery-2"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "token"},
		nil,
		5,
	)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateFailed {
			t.Errorf("expected state %s, got %s", domain.DeliveryStateFailed, result.State)
		}
		if !strings.Contains(result.Message, "stale generation") {
			t.Errorf("expected stale generation message, got %q", result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stale delivery result")
	}
}

func TestAgent_Remove_RejectsStaleGeneration(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	spec := validClusterSpecJSON(t)
	manifest := domain.Manifest{
		ManifestType: gcphcp.ClusterManifestType,
		ManifestID:   "test-cls",
		Raw:          spec,
	}

	// First: accept generation 10 via Deliver (it will fail async, but the generation is recorded)
	_ = agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("delivery-1"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "token"},
		nil,
		10,
	)
	select {
	case <-reporter.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first delivery result")
	}

	// Remove with stale generation 5 — should skip the cluster (not error)
	err := agent.Remove(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("remove-1"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "token"},
		nil,
		5,
	)
	if err != nil {
		t.Fatalf("Remove() error = %v, want nil (stale removal is skipped, not errored)", err)
	}
}

func validClusterSpecJSON(t *testing.T) json.RawMessage {
	t.Helper()
	autoRepair := true
	spec := struct {
		EndpointAccess string `json:"endpointAccess"`
		ReleaseVersion string `json:"releaseVersion"`
		ChannelGroup   string `json:"channelGroup"`
		Nodepools      []struct {
			ID             string `json:"id"`
			Replicas       int    `json:"replicas"`
			InstanceType   string `json:"instanceType"`
			RootVolumeSize int    `json:"rootVolumeSize"`
			RootVolumeType string `json:"rootVolumeType"`
			AutoRepair     *bool  `json:"autoRepair"`
			UpgradeType    string `json:"upgradeType"`
		} `json:"nodepools"`
	}{
		EndpointAccess: "PublicAndPrivate",
		ReleaseVersion: "4.22.0",
		ChannelGroup:   "stable",
		Nodepools: []struct {
			ID             string `json:"id"`
			Replicas       int    `json:"replicas"`
			InstanceType   string `json:"instanceType"`
			RootVolumeSize int    `json:"rootVolumeSize"`
			RootVolumeType string `json:"rootVolumeType"`
			AutoRepair     *bool  `json:"autoRepair"`
			UpgradeType    string `json:"upgradeType"`
		}{
			{
				ID:             "np1",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     &autoRepair,
				UpgradeType:    "Replace",
			},
		},
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return raw
}

func deliverTrustBundle(t *testing.T, agent *gcphcp.Agent, reporter *recordingReporter, entry domain.TrustBundleEntry) {
	t.Helper()

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		domain.DeliveryID("trust-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("async state = %q, want %q", result.State, domain.DeliveryStateDelivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for trust bundle delivery completion")
	}
}

func trustBundleManifest(t *testing.T, entry domain.TrustBundleEntry) domain.Manifest {
	t.Helper()

	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return domain.Manifest{
		ManifestType: domain.TrustBundleManifestType,
		Raw:          json.RawMessage(raw),
	}
}

// --- Recovery tests ---

// recoveryReporter extends recordingReporter with configurable ListActiveDeliveries.
type recoveryReporter struct {
	recordingReporter
	active    []domain.ActiveDelivery
	activeErr error
}

func newRecoveryReporter(active []domain.ActiveDelivery) *recoveryReporter {
	return &recoveryReporter{
		recordingReporter: recordingReporter{
			results: make(map[domain.DeliveryID]domain.DeliveryResult),
			done:    make(chan domain.DeliveryResult, 10),
		},
		active: active,
	}
}

func (r *recoveryReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return r.active, r.activeErr
}

func makeActiveDelivery(id string, clusterName string, gen domain.Generation, token string) domain.ActiveDelivery {
	spec := validClusterSpecJSON2()
	return domain.ActiveDelivery{
		Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:         domain.DeliveryID(id),
			Generation: gen,
			State:      domain.DeliveryStateProgressing,
			Operation:  domain.DeliveryOperationDeliver,
			Manifests: []domain.Manifest{{
				ManifestType: gcphcp.ClusterManifestType,
				ManifestID:   domain.ManifestID(clusterName),
				Raw:          spec,
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
		Auth: domain.DeliveryAuth{Token: domain.RawToken(token)},
	}
}

func validClusterSpecJSON2() json.RawMessage {
	return json.RawMessage(`{
		"endpointAccess":"PublicAndPrivate","releaseVersion":"4.22.0","channelGroup":"stable",
		"nodepools":[{"id":"np1","replicas":2,"instanceType":"n1-standard-4",
		"rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":true,"upgradeType":"Replace"}]
	}`)
}

func TestAgent_RecoverActiveDeliveries_NoActiveDeliveries(t *testing.T) {
	reporter := newRecoveryReporter(nil)
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}
}

func TestAgent_RecoverActiveDeliveries_ListError(t *testing.T) {
	reporter := newRecoveryReporter(nil)
	reporter.activeErr = fmt.Errorf("database unavailable")
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err == nil {
		t.Fatal("expected error when ListActiveDeliveries fails")
	}
	if !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("error = %q, want wrapped cause", err.Error())
	}
}

func TestAgent_RecoverActiveDeliveries_ResumesActiveDelivery(t *testing.T) {
	ad := makeActiveDelivery("recovery-1", "test-cls", 1, "caller-token")
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	// The delivery will fail because there's no real GCP backend,
	// but it proves the goroutine was launched and reported a result.
	select {
	case result := <-reporter.done:
		if result.State == "" {
			t.Fatal("expected non-empty delivery state from recovered delivery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered delivery result")
	}
}

func TestAgent_RecoverActiveDeliveries_SkipsEmptyAuthToken(t *testing.T) {
	ad := makeActiveDelivery("recovery-no-auth", "test-cls", 1, "")
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	// No goroutine should be launched — no result expected.
	select {
	case result := <-reporter.done:
		t.Fatalf("expected no delivery result for empty auth, got state=%q", result.State)
	case <-time.After(200 * time.Millisecond):
		// expected: empty auth token is skipped
	}
}

func TestAgent_RecoverActiveDeliveries_SkipsNonClusterManifests(t *testing.T) {
	ad := domain.ActiveDelivery{
		Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:         "recovery-no-cluster",
			Generation: 1,
			State:      domain.DeliveryStateProgressing,
			Manifests: []domain.Manifest{{
				ManifestType: "some.other.type",
				ManifestID:   "something",
				Raw:          json.RawMessage(`{}`),
			}},
		}),
		Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "target-1"}),
		Auth:   domain.DeliveryAuth{Token: "token"},
	}
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		t.Fatalf("expected no result for non-cluster manifest, got state=%q", result.State)
	case <-time.After(200 * time.Millisecond):
		// expected: no cluster manifest means no recovery goroutine
	}
}

func TestAgent_RecoverActiveDeliveries_SkipsStaleGeneration(t *testing.T) {
	reporter := newRecordingReporter()
	agent := newTestAgent(reporter)

	// Accept generation 10 via a normal Deliver call.
	spec := validClusterSpecJSON(t)
	_ = agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("seed"),
		[]domain.Manifest{{
			ManifestType: gcphcp.ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "token"},
		nil,
		10,
	)
	select {
	case <-reporter.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for seed delivery")
	}

	// Now try to recover a stale delivery (generation 5) for the same cluster.
	ad := makeActiveDelivery("recovery-stale", "test-cls", 5, "token")
	recovReporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	// Need a new agent that shares generation state — use a fresh one
	// with the recovery reporter, then seed its generation.
	agent2 := newTestAgent(recovReporter)
	// Seed generation 10
	_ = agent2.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("seed2"),
		[]domain.Manifest{{
			ManifestType: gcphcp.ClusterManifestType,
			ManifestID:   "test-cls",
			Raw:          spec,
		}},
		domain.DeliveryAuth{Token: "token"},
		nil,
		10,
	)
	select {
	case <-recovReporter.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for seed delivery")
	}

	err := agent2.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	// Stale generation should be skipped — no result.
	select {
	case result := <-recovReporter.done:
		t.Fatalf("expected stale delivery to be skipped, got state=%q", result.State)
	case <-time.After(200 * time.Millisecond):
		// expected
	}
}

func TestAgent_RecoverActiveDeliveries_SkipsInvalidClusterSpec(t *testing.T) {
	ad := domain.ActiveDelivery{
		Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:         "recovery-bad-spec",
			Generation: 1,
			State:      domain.DeliveryStateProgressing,
			Manifests: []domain.Manifest{{
				ManifestType: gcphcp.ClusterManifestType,
				ManifestID:   "test-cls",
				Raw:          json.RawMessage(`{{{not json`),
			}},
		}),
		Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "target-1"}),
		Auth:   domain.DeliveryAuth{Token: "token"},
	}
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v, want nil (bad specs are skipped)", err)
	}

	select {
	case result := <-reporter.done:
		t.Fatalf("expected no result for invalid spec, got state=%q", result.State)
	case <-time.After(200 * time.Millisecond):
		// expected: invalid spec is logged and skipped
	}
}

func TestAgent_RecoverActiveDeliveries_ResumesDeleteDelivery(t *testing.T) {
	ad := makeActiveDelivery("recovery-del", "test-cls", 2, "caller-token")
	ad.Delivery = domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:         domain.DeliveryID("recovery-del"),
		Generation: 2,
		State:      domain.DeliveryStateProgressing,
		Operation:  domain.DeliveryOperationRemove,
		Manifests:  ad.Delivery.Manifests(),
	})
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State == "" {
			t.Fatal("expected non-empty delivery state from recovered delete delivery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered delete delivery result")
	}
}

func TestAgent_RecoverActiveDeliveries_MultipleDeliveries(t *testing.T) {
	ad1 := makeActiveDelivery("recovery-a", "cls-a", 1, "token-a")
	ad2 := makeActiveDelivery("recovery-b", "cls-b", 1, "token-b")
	reporter := newRecoveryReporter([]domain.ActiveDelivery{ad1, ad2})
	agent := newTestAgent(reporter)

	err := agent.RecoverActiveDeliveries(context.Background(), []domain.TargetID{"target-1"})
	if err != nil {
		t.Fatalf("RecoverActiveDeliveries() error = %v", err)
	}

	// Both should produce results (failures since no real backend).
	for i := 0; i < 2; i++ {
		select {
		case result := <-reporter.done:
			if result.State == "" {
				t.Fatalf("delivery %d: expected non-empty state", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for delivery %d result", i)
		}
	}
}
