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

func boolPtr(b bool) *bool { return &b }

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
	ops                   []string
	createIAMErr          error
	createInfraErr        error
	waitPSCErr            error
	waitPSCWorkforceToken string
	destroyIAMErr         error
	destroyInfraErr       error
	createIAMResults      []createAttemptResult
	createInfraResults    []createAttemptResult
	destroyInfraResults   []error
	createIAMCalls        int
	createInfraCalls      int
	waitPSCCalls          int
	destroyInfraCalls     int
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
	attempt := f.destroyInfraCalls
	f.destroyInfraCalls++
	if len(f.destroyInfraResults) > 0 {
		if attempt >= len(f.destroyInfraResults) {
			attempt = len(f.destroyInfraResults) - 1
		}
		if err := f.destroyInfraResults[attempt]; err != nil {
			return err
		}
		return nil
	}
	return f.destroyInfraErr
}

func (f *fakeCleanupInfra) DestroyIAM(_ context.Context, infraID, projectID string, _ []string) error {
	f.ops = append(f.ops, "iam:"+infraID+":"+projectID)
	return f.destroyIAMErr
}

func (f *fakeCleanupInfra) WaitForPSCCleanup(
	_ context.Context,
	clusterID, projectID, region, workforceToken string,
	_ *domain.DeliverySignaler,
) error {
	f.ops = append(f.ops, "psc:"+clusterID+":"+projectID+":"+region)
	f.waitPSCCalls++
	f.waitPSCWorkforceToken = workforceToken
	return f.waitPSCErr
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
			AutoRepair:     boolPtr(true),
			UpgradeType:    "Replace",
		},
		{
			Name:           "worker-b",
			Replicas:       2,
			InstanceType:   "n1-standard-4",
			RootVolumeSize: 128,
			RootVolumeType: "pd-standard",
			AutoRepair:     boolPtr(true),
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

func TestCleanupDeleteResources_DestroysInfraOnceAfterPSCWait(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyInfraResults: []error{nil},
	}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		"cluster-123",
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("cleanupDeleteResources() error = %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "psc:cluster-123:project-123:us-central1,infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
	if infra.waitPSCWorkforceToken != "workforce-token" {
		t.Fatalf("PSC wait token = %q, want workforce-token", infra.waitPSCWorkforceToken)
	}
}

func TestCleanupDeleteResources_ReturnsIAMFailureWithDeleteSuccessContext(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyIAMErr: errors.New("iam destroy failed"),
	}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		"cluster-123",
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected IAM destroy failure")
	}
	if !strings.Contains(err.Error(), "cluster deletion succeeded") ||
		!strings.Contains(err.Error(), "infrastructure cleanup completed") ||
		!strings.Contains(err.Error(), "iam destroy failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "psc:cluster-123:project-123:us-central1,infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
}

func TestCleanupDeleteResources_ReturnsInfraFailureWithoutRetry(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyInfraResults: []error{errors.New("infra not ready")},
	}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		"cluster-123",
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		[]string{"EXAMPLE=1"},
		&domain.DeliverySignaler{},
	)
	if err == nil {
		t.Fatal("expected infra destroy failure")
	}
	if !strings.Contains(err.Error(), "destroy infra") || !strings.Contains(err.Error(), "infra not ready") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "psc:cluster-123:project-123:us-central1,infra:test-cluster:project-123:us-central1" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
	if infra.destroyInfraCalls != 1 {
		t.Fatalf("destroy infra calls = %d, want 1", infra.destroyInfraCalls)
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

func TestCompleteGuestRegistration_RetriesUntilGuestAPIEndpointAppears(t *testing.T) {
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
	bootstrapGuestCluster = func(_ context.Context, endpoint, _ string, _ domain.TargetID) (BootstrapResult, error) {
		bootstrapCalls++
		if endpoint != "https://guest.example:6443" {
			t.Fatalf("bootstrap endpoint = %q, want https://guest.example:6443", endpoint)
		}
		return BootstrapResult{SATokenRef: "secret-ref"}, nil
	}

	statusCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123/status" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		statusCalls++
		if statusCalls == 1 {
			fmt.Fprint(w, `{"controller_status":[{"conditions":[{"type":"SomeOtherCondition","message":"still provisioning"}]}]}`)
			return
		}
		fmt.Fprint(w, `{"controller_status":[{"conditions":[{"type":"APIServer","message":"https://guest.example:6443"}]}]}`)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "broker-token", "broker@example.com", nil)

	endpoint, result, err := completeGuestRegistration(
		context.Background(),
		client,
		"c-123",
		"broker-token",
		domain.TargetID("guest-target"),
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("completeGuestRegistration() error = %v", err)
	}
	if endpoint != "https://guest.example:6443" {
		t.Fatalf("endpoint = %q, want https://guest.example:6443", endpoint)
	}
	if result.SATokenRef != "secret-ref" {
		t.Fatalf("SATokenRef = %q, want secret-ref", result.SATokenRef)
	}
	if statusCalls != 2 {
		t.Fatalf("status calls = %d, want 2", statusCalls)
	}
	if bootstrapCalls != 1 {
		t.Fatalf("bootstrap calls = %d, want 1", bootstrapCalls)
	}
}

func TestCompleteGuestRegistration_ReturnsRetryExhaustedWhenGuestAPIEndpointNeverAppears(t *testing.T) {
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
		return BootstrapResult{}, nil
	}

	statusCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123/status" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		statusCalls++
		fmt.Fprint(w, `{"controller_status":[{"conditions":[{"type":"SomeOtherCondition","message":"still provisioning"}]}]}`)
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
	if !strings.Contains(err.Error(), "resolve guest API endpoint after 3 attempts") {
		t.Fatalf("error = %v, want endpoint retry exhaustion context", err)
	}
	if statusCalls != 3 {
		t.Fatalf("status calls = %d, want 3", statusCalls)
	}
	if bootstrapCalls != 0 {
		t.Fatalf("bootstrap calls = %d, want 0", bootstrapCalls)
	}
}

type fakeBrokerAuth struct {
	result      BrokerAuthResult
	err         error
	callerToken string
}

func (f *fakeBrokerAuth) Exchange(_ context.Context, callerToken string) (BrokerAuthResult, error) {
	f.callerToken = callerToken
	if f.err != nil {
		return BrokerAuthResult{}, f.err
	}
	return f.result, nil
}

func TestReconcilerDelete_UsesCallerTokenForHypershiftEnvAndWorkforceTokenForPSCCleanup(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origBuildHypershiftEnv := buildHypershiftEnv
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		buildHypershiftEnv = origBuildHypershiftEnv
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:    "broker-token",
			BrokerEmail:    "broker@example.com",
			WorkforceToken: "workforce-token",
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	var gotHypershiftToken string
	buildHypershiftEnv = func(token string, _ TargetConfig, _ string) ([]string, error) {
		gotHypershiftToken = token
		return []string{"PATH=/usr/bin"}, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer broker-token" {
			t.Errorf("authorization header = %q, want Bearer broker-token", got)
		}
		if got := r.Header.Get("X-User-Email"); got != "broker@example.com" {
			t.Errorf("X-User-Email = %q, want broker@example.com", got)
		}

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-123","name":"test-cluster"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-123":
			if got := r.URL.Query().Get("force"); got != "true" {
				t.Errorf("force query param = %q, want true", got)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-123":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	infra := &fakeCleanupInfra{}
	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	err := reconciler.Delete(
		context.Background(),
		ClusterSpec{Name: "test-cluster"},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if fakeAuth.callerToken != "caller-token" {
		t.Fatalf("broker auth caller token = %q, want caller-token", fakeAuth.callerToken)
	}
	if gotHypershiftToken != "caller-token" {
		t.Fatalf("hypershift env token = %q, want caller-token", gotHypershiftToken)
	}
	if infra.waitPSCWorkforceToken != "workforce-token" {
		t.Fatalf("PSC cleanup workforce token = %q, want workforce-token", infra.waitPSCWorkforceToken)
	}
}
