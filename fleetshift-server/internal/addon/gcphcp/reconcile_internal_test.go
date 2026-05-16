package gcphcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeNodepoolClient struct {
	listedNodepools []map[string]any

	createdSpecs []map[string]any
	updatedSpecs map[string]map[string]any
	deletedIDs   []string
}

type createAttemptResult struct {
	result map[string]any
	err    error
}

type fakeCleanupInfra struct {
	ops                []string
	createIAMErr       error
	createInfraErr     error
	destroyIAMErr      error
	destroyInfraErr    error
	createIAMResults   []createAttemptResult
	createInfraResults []createAttemptResult
	createIAMCalls     int
	createInfraCalls   int
}

func (f *fakeCleanupInfra) CreateIAM(_ context.Context, infraID, projectID, jwksPath string, _ []string) (map[string]any, error) {
	f.ops = append(f.ops, "create-iam:"+infraID+":"+projectID+":"+jwksPath)
	attempt := f.createIAMCalls
	f.createIAMCalls++
	if len(f.createIAMResults) > 0 {
		if attempt >= len(f.createIAMResults) {
			attempt = len(f.createIAMResults) - 1
		}
		result := f.createIAMResults[attempt]
		if result.err != nil {
			return nil, result.err
		}
		if result.result != nil {
			return result.result, nil
		}
	}
	if f.createIAMErr != nil {
		return nil, f.createIAMErr
	}
	return map[string]any{
		"workloadIdentityPool": map[string]any{
			"audience": "test-audience",
		},
	}, nil
}

func (f *fakeCleanupInfra) CreateInfra(_ context.Context, infraID, projectID, region string, _ []string) (map[string]any, error) {
	f.ops = append(f.ops, "create-infra:"+infraID+":"+projectID+":"+region)
	attempt := f.createInfraCalls
	f.createInfraCalls++
	if len(f.createInfraResults) > 0 {
		if attempt >= len(f.createInfraResults) {
			attempt = len(f.createInfraResults) - 1
		}
		result := f.createInfraResults[attempt]
		if result.err != nil {
			return nil, result.err
		}
		if result.result != nil {
			return result.result, nil
		}
	}
	if f.createInfraErr != nil {
		return nil, f.createInfraErr
	}
	return map[string]any{
		"infraId":     infraID,
		"networkName": infraID + "-network",
		"subnetName":  infraID + "-subnet",
	}, nil
}

func (f *fakeCleanupInfra) DestroyInfra(_ context.Context, infraID, projectID, region string, _ []string) error {
	f.ops = append(f.ops, "infra:"+infraID+":"+projectID+":"+region)
	return f.destroyInfraErr
}

func (f *fakeCleanupInfra) DestroyIAM(_ context.Context, infraID, projectID string, _ []string) error {
	f.ops = append(f.ops, "iam:"+infraID+":"+projectID)
	return f.destroyIAMErr
}

type fakeClusterDeleteClient struct {
	clusterID  string
	resolveErr error
	deleteIDs  []string
}

type resolveResult struct {
	clusterID string
	err       error
}

type fakeClusterResolveClient struct {
	results         []resolveResult
	calls           int
	observedCluster map[string]any
	getClusterErr   error
	updateErr       error
	updatedIDs      []string
	updatedSpecs    []map[string]any
}

func (f *fakeClusterDeleteClient) ResolveClusterID(_ context.Context, _ string) (string, error) {
	return f.clusterID, f.resolveErr
}

func (f *fakeClusterDeleteClient) DeleteCluster(_ context.Context, clusterID string) error {
	f.deleteIDs = append(f.deleteIDs, clusterID)
	return nil
}

func (f *fakeClusterResolveClient) ResolveClusterID(_ context.Context, _ string) (string, error) {
	idx := f.calls
	if idx >= len(f.results) {
		idx = len(f.results) - 1
	}
	f.calls++
	return f.results[idx].clusterID, f.results[idx].err
}

func (f *fakeClusterResolveClient) GetCluster(_ context.Context, _ string) (map[string]any, error) {
	if f.getClusterErr != nil {
		return nil, f.getClusterErr
	}
	return f.observedCluster, nil
}

func (f *fakeClusterResolveClient) UpdateCluster(_ context.Context, clusterID string, spec map[string]any) (map[string]any, error) {
	f.updatedIDs = append(f.updatedIDs, clusterID)
	f.updatedSpecs = append(f.updatedSpecs, spec)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return map[string]any{"id": clusterID}, nil
}

func (f *fakeNodepoolClient) ListNodepools(_ context.Context, _ string) ([]map[string]any, error) {
	return f.listedNodepools, nil
}

func (f *fakeNodepoolClient) CreateNodepool(_ context.Context, spec map[string]any) (map[string]any, error) {
	f.createdSpecs = append(f.createdSpecs, spec)
	return map[string]any{"id": "created-nodepool"}, nil
}

func (f *fakeNodepoolClient) UpdateNodepool(_ context.Context, nodepoolID string, spec map[string]any) (map[string]any, error) {
	if f.updatedSpecs == nil {
		f.updatedSpecs = make(map[string]map[string]any)
	}
	f.updatedSpecs[nodepoolID] = spec
	return map[string]any{"id": nodepoolID}, nil
}

func (f *fakeNodepoolClient) DeleteNodepool(_ context.Context, nodepoolID string) error {
	f.deletedIDs = append(f.deletedIDs, nodepoolID)
	return nil
}

func TestReconcileNodepools_CreatesUpdatesAndDeletesByName(t *testing.T) {
	client := &fakeNodepoolClient{
		listedNodepools: []map[string]any{
			{"id": "np-existing", "name": "worker-a"},
			{"id": "np-removed", "name": "worker-old"},
		},
	}

	desired := []NodepoolSpec{
		{
			Name:           "worker-a",
			Replicas:       3,
			InstanceType:   "n1-standard-8",
			RootVolumeSize: 256,
			RootVolumeType: "pd-ssd",
			AutoRepair:     true,
			UpgradeType:    "Replace",
		},
		{
			Name:           "worker-b",
			Replicas:       2,
			InstanceType:   "n1-standard-4",
			RootVolumeSize: 128,
			RootVolumeType: "pd-standard",
			AutoRepair:     true,
			UpgradeType:    "Replace",
		},
	}

	err := reconcileNodepools(context.Background(), client, "cluster-123", desired, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("reconcileNodepools failed: %v", err)
	}

	if len(client.createdSpecs) != 1 {
		t.Fatalf("created count = %d, want 1", len(client.createdSpecs))
	}
	if name := client.createdSpecs[0]["name"]; name != "worker-b" {
		t.Errorf("created nodepool name = %v, want worker-b", name)
	}

	updated, ok := client.updatedSpecs["np-existing"]
	if !ok {
		t.Fatal("expected existing nodepool to be updated")
	}
	if name := updated["name"]; name != "worker-a" {
		t.Errorf("updated nodepool name = %v, want worker-a", name)
	}
	specMap, ok := updated["spec"].(map[string]any)
	if !ok {
		t.Fatal("updated nodepool spec is not a map")
	}
	if replicas := specMap["replicas"]; replicas != 3 {
		t.Errorf("updated replicas = %v, want 3", replicas)
	}

	if len(client.deletedIDs) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(client.deletedIDs))
	}
	if client.deletedIDs[0] != "np-removed" {
		t.Errorf("deleted nodepool id = %q, want np-removed", client.deletedIDs[0])
	}
}

func TestReconcileNodepools_DuplicateDesiredNames(t *testing.T) {
	client := &fakeNodepoolClient{}
	desired := []NodepoolSpec{
		{Name: "worker-a", Replicas: 2},
		{Name: "worker-a", Replicas: 3},
	}

	err := reconcileNodepools(context.Background(), client, "cluster-123", desired, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected duplicate desired name error")
	}
	if !strings.Contains(err.Error(), "duplicate desired nodepool name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCleanupCreateResources_DestroysCreatedInfraAndIAM(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		true,
		true,
	)
	if err != nil {
		t.Fatalf("cleanupCreateResources() error = %v", err)
	}

	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
}

func TestCleanupDeleteResources_ReturnsIAMFailure(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyIAMErr: errors.New("iam destroy failed"),
	}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected IAM destroy failure")
	}
	if !strings.Contains(err.Error(), "destroy IAM") || !strings.Contains(err.Error(), "iam destroy failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
}

func TestDeleteClusterIfPresent_SkipsMissingCluster(t *testing.T) {
	client := &fakeClusterDeleteClient{
		resolveErr: ErrClusterNotFound,
	}

	clusterID, deleted, err := deleteClusterIfPresent(
		context.Background(),
		client,
		"test-cluster",
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("deleteClusterIfPresent() error = %v", err)
	}
	if clusterID != "" {
		t.Fatalf("expected empty cluster ID, got %q", clusterID)
	}
	if deleted {
		t.Fatal("expected missing cluster to skip delete")
	}
	if len(client.deleteIDs) != 0 {
		t.Fatalf("expected no delete calls, got %v", client.deleteIDs)
	}
}

func TestEnsureIAMWithRecovery_RetriesAmbiguousFailure(t *testing.T) {
	origInterval := ambiguousPrereqRetryInterval
	origAttempts := ambiguousPrereqMaxAttempts
	ambiguousPrereqRetryInterval = 0
	ambiguousPrereqMaxAttempts = 2
	defer func() {
		ambiguousPrereqRetryInterval = origInterval
		ambiguousPrereqMaxAttempts = origAttempts
	}()

	infra := &fakeCleanupInfra{
		createIAMResults: []createAttemptResult{
			{err: errors.New("iam partially created before cli failure")},
			{
				result: map[string]any{
					"workloadIdentityPool": map[string]any{
						"audience": "recovered-audience",
					},
				},
			},
		},
	}

	iamConfig, err := ensureIAMWithRecovery(
		context.Background(),
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("ensureIAMWithRecovery() error = %v", err)
	}
	if infra.createIAMCalls != 2 {
		t.Fatalf("expected 2 IAM attempts, got %d", infra.createIAMCalls)
	}
	if got := strings.Join(infra.ops, ","); got != "create-iam:test-cluster:project-123:/tmp/jwks.json,create-iam:test-cluster:project-123:/tmp/jwks.json" {
		t.Fatalf("unexpected IAM recovery operations: %s", got)
	}
	wip, ok := iamConfig["workloadIdentityPool"].(map[string]any)
	if !ok {
		t.Fatal("expected workloadIdentityPool in IAM config")
	}
	if audience := wip["audience"]; audience != "recovered-audience" {
		t.Fatalf("audience = %v, want recovered-audience", audience)
	}
}

func TestEnsureInfraWithRecovery_ReturnsErrorAfterAmbiguousRetries(t *testing.T) {
	origInterval := ambiguousPrereqRetryInterval
	origAttempts := ambiguousPrereqMaxAttempts
	ambiguousPrereqRetryInterval = 0
	ambiguousPrereqMaxAttempts = 3
	defer func() {
		ambiguousPrereqRetryInterval = origInterval
		ambiguousPrereqMaxAttempts = origAttempts
	}()

	infra := &fakeCleanupInfra{
		createInfraResults: []createAttemptResult{
			{err: errors.New("infra partially created before cli failure")},
		},
	}

	infraConfig, err := ensureInfraWithRecovery(
		context.Background(),
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected ambiguous infra error after retries")
	}
	if infraConfig != nil {
		t.Fatalf("expected nil infra config, got %#v", infraConfig)
	}
	if infra.createInfraCalls != 3 {
		t.Fatalf("expected 3 infra attempts, got %d", infra.createInfraCalls)
	}
	if strings.Contains(strings.Join(infra.ops, ","), "iam:test-cluster") {
		t.Fatalf("expected no cleanup operations during infra retry path, got %v", infra.ops)
	}
	if !strings.Contains(err.Error(), "infrastructure creation remained ambiguous after 3 attempts") {
		t.Fatalf("expected ambiguous retry error, got %v", err)
	}
}

func TestRecoverFromAmbiguousCreateFailure_AdoptsClusterWhenItAppears(t *testing.T) {
	origInterval := ambiguousCreateProbeInterval
	origTimeout := ambiguousCreateProbeTimeout
	ambiguousCreateProbeInterval = 5 * time.Millisecond
	ambiguousCreateProbeTimeout = 25 * time.Millisecond
	defer func() {
		ambiguousCreateProbeInterval = origInterval
		ambiguousCreateProbeTimeout = origTimeout
	}()

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{err: fmt.Errorf("%w: %q", ErrClusterNotFound, "test-cluster")},
			{clusterID: "cluster-123"},
		},
		observedCluster: map[string]any{
			"target_project_id": "project-123",
			"spec": map[string]any{
				"infraID":                  "test-cluster",
				"issuerURL":                "https://hypershift-test-cluster-oidc",
				"serviceAccountSigningKey": "signing-key",
				"platform": map[string]any{
					"gcp": map[string]any{
						"projectID":        "project-123",
						"region":           "us-central1",
						"network":          "test-cluster-network",
						"subnet":           "test-cluster-subnet",
						"endpointAccess":   "PublicAndPrivate",
						"workloadIdentity": map[string]any{"audience": "test-audience"},
					},
				},
			},
		},
	}
	infra := &fakeCleanupInfra{}

	clusterID, err := recoverFromAmbiguousCreateFailure(
		context.Background(),
		client,
		infra,
		ClusterSpec{Name: "test-cluster", EndpointAccess: "Private"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("recoverFromAmbiguousCreateFailure() error = %v", err)
	}
	if clusterID != "cluster-123" {
		t.Fatalf("expected adopted cluster ID cluster-123, got %q", clusterID)
	}
	if got := strings.Join(infra.ops, ","); got != "create-iam:test-cluster:project-123:/tmp/jwks.json,create-infra:test-cluster:project-123:us-central1" {
		t.Fatalf("unexpected repair operations: %s", got)
	}
	if len(client.updatedIDs) != 1 || client.updatedIDs[0] != "cluster-123" {
		t.Fatalf("expected one update for adopted cluster, got ids %v", client.updatedIDs)
	}
	if len(client.updatedSpecs) != 1 {
		t.Fatalf("expected one updated spec, got %d", len(client.updatedSpecs))
	}
	updatedSpec, ok := client.updatedSpecs[0]["spec"].(map[string]any)
	if !ok {
		t.Fatal("updated cluster spec is not a map")
	}
	platform, ok := updatedSpec["platform"].(map[string]any)
	if !ok {
		t.Fatal("updated cluster spec missing platform map")
	}
	gcp, ok := platform["gcp"].(map[string]any)
	if !ok {
		t.Fatal("updated cluster platform missing gcp map")
	}
	if endpointAccess := gcp["endpointAccess"]; endpointAccess != "Private" {
		t.Fatalf("updated endpointAccess = %v, want Private", endpointAccess)
	}
}

func TestRecoverFromAmbiguousCreateFailure_ReturnsErrorWithoutCleanupWhenReensureFails(t *testing.T) {
	origInterval := ambiguousCreateProbeInterval
	origTimeout := ambiguousCreateProbeTimeout
	origPrereqInterval := ambiguousPrereqRetryInterval
	origPrereqAttempts := ambiguousPrereqMaxAttempts
	ambiguousCreateProbeInterval = 5 * time.Millisecond
	ambiguousCreateProbeTimeout = 20 * time.Millisecond
	ambiguousPrereqRetryInterval = 0
	ambiguousPrereqMaxAttempts = 2
	defer func() {
		ambiguousCreateProbeInterval = origInterval
		ambiguousCreateProbeTimeout = origTimeout
		ambiguousPrereqRetryInterval = origPrereqInterval
		ambiguousPrereqMaxAttempts = origPrereqAttempts
	}()

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{clusterID: "cluster-123"},
		},
	}
	infra := &fakeCleanupInfra{
		createInfraErr: errors.New("infra rerun failed"),
	}

	clusterID, err := recoverFromAmbiguousCreateFailure(
		context.Background(),
		client,
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected re-ensure failure")
	}
	if clusterID != "" {
		t.Fatalf("expected no adopted cluster ID on failed re-ensure, got %q", clusterID)
	}
	if got := strings.Join(infra.ops, ","); got != "create-iam:test-cluster:project-123:/tmp/jwks.json,create-infra:test-cluster:project-123:us-central1,create-infra:test-cluster:project-123:us-central1" {
		t.Fatalf("unexpected repair operations: %s", got)
	}
	if len(client.updatedIDs) != 0 {
		t.Fatalf("expected no cluster update after failed re-ensure, got %v", client.updatedIDs)
	}
	if !strings.Contains(err.Error(), "request timeout") || !strings.Contains(err.Error(), "re-ensure infrastructure for adopted cluster") {
		t.Fatalf("expected combined create/re-ensure error, got %v", err)
	}
}

func TestRecoverFromAmbiguousCreateFailure_CleansUpAfterTimeout(t *testing.T) {
	origInterval := ambiguousCreateProbeInterval
	origTimeout := ambiguousCreateProbeTimeout
	ambiguousCreateProbeInterval = 5 * time.Millisecond
	ambiguousCreateProbeTimeout = 20 * time.Millisecond
	defer func() {
		ambiguousCreateProbeInterval = origInterval
		ambiguousCreateProbeTimeout = origTimeout
	}()

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{err: fmt.Errorf("%w: %q", ErrClusterNotFound, "test-cluster")},
		},
	}
	infra := &fakeCleanupInfra{}

	clusterID, err := recoverFromAmbiguousCreateFailure(
		context.Background(),
		client,
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("cluster creation response missing id field"),
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected create failure after timeout")
	}
	if clusterID != "" {
		t.Fatalf("expected no adopted cluster ID, got %q", clusterID)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
	if !strings.Contains(err.Error(), "cluster creation response missing id field") {
		t.Fatalf("expected original create failure, got %v", err)
	}
}

func TestRecoverFromAmbiguousCreateFailure_SkipsCleanupWhenProbeFails(t *testing.T) {
	origInterval := ambiguousCreateProbeInterval
	origTimeout := ambiguousCreateProbeTimeout
	ambiguousCreateProbeInterval = 5 * time.Millisecond
	ambiguousCreateProbeTimeout = 20 * time.Millisecond
	defer func() {
		ambiguousCreateProbeInterval = origInterval
		ambiguousCreateProbeTimeout = origTimeout
	}()

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{err: errors.New("list clusters: temporary backend error")},
		},
	}
	infra := &fakeCleanupInfra{}

	clusterID, err := recoverFromAmbiguousCreateFailure(
		context.Background(),
		client,
		infra,
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected combined probe error")
	}
	if clusterID != "" {
		t.Fatalf("expected no adopted cluster ID, got %q", clusterID)
	}
	if len(infra.ops) != 0 {
		t.Fatalf("expected no cleanup when probe is inconclusive, got %v", infra.ops)
	}
	if !strings.Contains(err.Error(), "request timeout") || !strings.Contains(err.Error(), "probe for cluster after ambiguous create") {
		t.Fatalf("expected combined create/probe error, got %v", err)
	}
}

func TestCompleteGuestRegistration_ReturnsPostProvisionRegistrationErrorAfterBootstrapRetries(t *testing.T) {
	origAttempts := guestBootstrapMaxAttempts
	origDelay := guestBootstrapRetryDelay
	origBootstrap := bootstrapGuestCluster
	guestBootstrapMaxAttempts = 3
	guestBootstrapRetryDelay = 0
	defer func() {
		guestBootstrapMaxAttempts = origAttempts
		guestBootstrapRetryDelay = origDelay
		bootstrapGuestCluster = origBootstrap
	}()

	bootstrapCalls := 0
	bootstrapGuestCluster = func(context.Context, string, string, domain.TargetID) (BootstrapResult, error) {
		bootstrapCalls++
		return BootstrapResult{}, errors.New("rbac setup job not ready")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123/status" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		fmt.Fprint(w, `{"controller_status":[{"conditions":[{"type":"APIServer","message":"https://guest.example:6443"}]}]}`)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "broker-token", "broker@example.com", nil)

	_, _, err := completeGuestRegistration(
		context.Background(),
		client,
		"c-123",
		"broker-token",
		domain.TargetID("guest-target"),
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected guest registration error")
	}
	if !isPostProvisionRegistrationError(err) {
		t.Fatalf("expected post-provision registration error, got %T: %v", err, err)
	}
	if bootstrapCalls != 3 {
		t.Fatalf("bootstrap calls = %d, want 3", bootstrapCalls)
	}
	if !strings.Contains(err.Error(), "bootstrap guest cluster after 3 attempts") {
		t.Fatalf("error = %v, want retry exhaustion context", err)
	}
}
