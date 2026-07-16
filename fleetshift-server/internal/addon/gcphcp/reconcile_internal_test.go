package gcphcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
	ops                      []string
	createIAMErr             error
	createInfraErr           error
	waitPSCErr               error
	waitPSCWorkforceToken    string
	destroyIAMErr            error
	destroyInfraErr          error
	createIAMTokenURL        string
	createInfraTokenURL      string
	createIAMToken           string
	createInfraToken         string
	createIAMSubjectToken    string
	createInfraSubjectToken  string
	createIAMQuotaProject    string
	createInfraQuotaProject  string
	destroyInfraTokenURL     string
	destroyIAMTokenURL       string
	destroyInfraToken        string
	destroyIAMToken          string
	destroyInfraSubjectToken string
	destroyIAMSubjectToken   string
	destroyInfraQuotaProject string
	destroyIAMQuotaProject   string
	createIAMResults         []createAttemptResult
	createInfraResults       []createAttemptResult
	destroyInfraResults      []error
	createIAMDelay           time.Duration
	waitPSCDelay             time.Duration
	createIAMCalls           int
	createInfraCalls         int
	waitPSCCalls             int
	destroyInfraCalls        int
}

func (f *fakeCleanupInfra) CreateIAM(
	_ context.Context,
	infraID, projectID, jwksPath string,
	env []string,
) (map[string]any, error) {
	f.ops = append(f.ops, "create-iam:"+infraID+":"+projectID+":"+jwksPath)
	f.createIAMTokenURL, f.createIAMToken, f.createIAMSubjectToken, f.createIAMQuotaProject = readTokenURLAndToken(env)
	attempt := f.createIAMCalls
	f.createIAMCalls++
	if f.createIAMDelay > 0 {
		time.Sleep(f.createIAMDelay)
	}
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

func (f *fakeCleanupInfra) CreateInfra(
	_ context.Context,
	infraID, projectID, region string,
	env []string,
) (map[string]any, error) {
	f.ops = append(f.ops, "create-infra:"+infraID+":"+projectID+":"+region)
	f.createInfraTokenURL, f.createInfraToken, f.createInfraSubjectToken, f.createInfraQuotaProject = readTokenURLAndToken(env)
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

func (f *fakeCleanupInfra) DestroyInfra(
	_ context.Context,
	infraID, projectID, region string,
	env []string,
) error {
	f.ops = append(f.ops, "infra:"+infraID+":"+projectID+":"+region)
	f.destroyInfraTokenURL, f.destroyInfraToken, f.destroyInfraSubjectToken, f.destroyInfraQuotaProject = readTokenURLAndToken(env)
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

func (f *fakeCleanupInfra) DestroyIAM(
	_ context.Context,
	infraID, projectID string,
	env []string,
) error {
	f.ops = append(f.ops, "iam:"+infraID+":"+projectID)
	f.destroyIAMTokenURL, f.destroyIAMToken, f.destroyIAMSubjectToken, f.destroyIAMQuotaProject = readTokenURLAndToken(env)
	return f.destroyIAMErr
}

func (f *fakeCleanupInfra) WaitForPSCCleanup(
	_ context.Context,
	clusterID, projectID, region, workforceToken string,
	_ *deliveryProgress,
) error {
	f.ops = append(f.ops, "psc:"+clusterID+":"+projectID+":"+region)
	f.waitPSCCalls++
	f.waitPSCWorkforceToken = workforceToken
	if f.waitPSCDelay > 0 {
		time.Sleep(f.waitPSCDelay)
	}
	return f.waitPSCErr
}

func readTokenURLAndToken(env []string) (string, string, string, string) {
	adcPath := lookupHypershiftEnvVar(env, "GOOGLE_APPLICATION_CREDENTIALS")
	if adcPath == "" {
		return "", "", "", ""
	}

	credConfigData, err := os.ReadFile(adcPath)
	if err != nil {
		return "", "", "", ""
	}

	var credConfig map[string]any
	if err := json.Unmarshal(credConfigData, &credConfig); err != nil {
		return "", "", "", ""
	}

	tokenURL, _ := credConfig["token_url"].(string)
	quotaProject, _ := credConfig["quota_project_id"].(string)
	credSource, _ := credConfig["credential_source"].(map[string]any)
	subjectTokenPath, _ := credSource["file"].(string)
	var subjectToken string
	if subjectTokenPath != "" {
		subjectTokenData, err := os.ReadFile(subjectTokenPath)
		if err != nil {
			return tokenURL, "", "", quotaProject
		}
		subjectToken = string(subjectTokenData)
	}
	if tokenURL == "" {
		return "", "", subjectToken, quotaProject
	}
	if !strings.HasPrefix(tokenURL, "http://127.0.0.1:") {
		return tokenURL, "", subjectToken, quotaProject
	}
	audience, _ := credConfig["audience"].(string)

	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(
		"grant_type=urn:ietf:params:oauth:grant-type:token-exchange&audience="+url.QueryEscape(audience)+"&requested_token_type=urn:ietf:params:oauth:token-type:access_token&subject_token_type=urn:ietf:params:oauth:token-type:jwt&subject_token="+url.QueryEscape(subjectToken)+"&scope="+url.QueryEscape("https://www.googleapis.com/auth/cloud-platform"),
	))
	if err != nil {
		return tokenURL, "", subjectToken, quotaProject
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return tokenURL, "", subjectToken, quotaProject
	}

	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return tokenURL, "", subjectToken, quotaProject
	}
	return tokenURL, body.AccessToken, subjectToken, quotaProject
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
			{"id": "np-existing", "name": "test-cluster-workers"},
			{"id": "np-removed", "name": "test-cluster-old"},
		},
	}

	desired := []NodepoolSpec{
		{
			ID:             "workers",
			Replicas:       3,
			InstanceType:   "n1-standard-8",
			RootVolumeSize: 256,
			RootVolumeType: "pd-ssd",
			AutoRepair:     boolPtr(true),
			UpgradeType:    "Replace",
		},
		{
			ID:             "np1",
			Replicas:       2,
			InstanceType:   "n1-standard-4",
			RootVolumeSize: 128,
			RootVolumeType: "pd-standard",
			AutoRepair:     boolPtr(true),
			UpgradeType:    "Replace",
		},
	}

	err := reconcileNodepools(context.Background(), client, "cluster-123", "test-cluster", desired, noopProgress())
	if err != nil {
		t.Fatalf("reconcileNodepools failed: %v", err)
	}

	if len(client.createdSpecs) != 1 {
		t.Fatalf("created count = %d, want 1", len(client.createdSpecs))
	}
	if name := client.createdSpecs[0]["name"]; name != "test-cluster-np1" {
		t.Errorf("created nodepool name = %v, want test-cluster-np1", name)
	}

	updated, ok := client.updatedSpecs["np-existing"]
	if !ok {
		t.Fatal("expected existing nodepool to be updated")
	}
	if name := updated["name"]; name != "test-cluster-workers" {
		t.Errorf("updated nodepool name = %v, want test-cluster-workers", name)
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
		{ID: "workers", Replicas: 2},
		{ID: "workers", Replicas: 3},
	}

	err := reconcileNodepools(context.Background(), client, "cluster-123", "test-cluster", desired, noopProgress())
	if err == nil {
		t.Fatal("expected duplicate desired name error")
	}
	if !strings.Contains(err.Error(), "duplicate desired nodepool id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCleanupCreateResources_DestroysCreatedInfraAndIAM(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
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

func TestWaitForDeleteCleanupPrereqs_WaitsForPSC(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyInfraResults: []error{nil},
	}

	err := waitForDeleteCleanupPrereqs(
		context.Background(),
		infra,
		"cluster-123",
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("waitForDeleteCleanupPrereqs() error = %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "psc:cluster-123:project-123:us-central1" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
	if infra.waitPSCWorkforceToken != "workforce-token" {
		t.Fatalf("PSC wait token = %q, want workforce-token", infra.waitPSCWorkforceToken)
	}
}

func TestCleanupDeleteResources_DestroysInfraAndIAM(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("cleanupDeleteResources() error = %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
		t.Fatalf("unexpected cleanup operations: %s", got)
	}
}

func TestCleanupDeleteResources_ReturnsIAMFailureWithDeleteSuccessContext(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyIAMErr: errors.New("iam destroy failed"),
	}

	err := cleanupDeleteResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected IAM destroy failure")
	}
	if !strings.Contains(err.Error(), "cluster deletion succeeded") ||
		!strings.Contains(err.Error(), "infrastructure cleanup completed") ||
		!strings.Contains(err.Error(), "iam destroy failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1,iam:test-cluster:project-123" {
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
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected infra destroy failure")
	}
	if !strings.Contains(err.Error(), "destroy infra") || !strings.Contains(err.Error(), "infra not ready") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1" {
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
		noopProgress(),
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

// withFastRecoveryTimers saves and restores unconfirmedPrereqRetryInterval
// and unconfirmedPrereqMaxAttempts, setting fast defaults (0 interval,
// 2 max attempts) for testing.
func withFastRecoveryTimers(t *testing.T) {
	t.Helper()
	origInterval := unconfirmedPrereqRetryInterval
	origAttempts := unconfirmedPrereqMaxAttempts
	unconfirmedPrereqRetryInterval = 0
	unconfirmedPrereqMaxAttempts = 2
	t.Cleanup(func() {
		unconfirmedPrereqRetryInterval = origInterval
		unconfirmedPrereqMaxAttempts = origAttempts
	})
}

// withFastProbeTimers saves and restores unconfirmedCreateProbeInterval
// and unconfirmedCreateProbeTimeout, setting fast defaults (5ms interval,
// 20ms timeout) for testing.
func withFastProbeTimers(t *testing.T) {
	t.Helper()
	origInterval := unconfirmedCreateProbeInterval
	origTimeout := unconfirmedCreateProbeTimeout
	unconfirmedCreateProbeInterval = 5 * time.Millisecond
	unconfirmedCreateProbeTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		unconfirmedCreateProbeInterval = origInterval
		unconfirmedCreateProbeTimeout = origTimeout
	})
}

func TestEnsureIAMWithRecovery_RetriesUnconfirmedFailure(t *testing.T) {
	withFastRecoveryTimers(t)

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
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		noopProgress(),
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

func TestEnsureInfraWithRecovery_ReturnsErrorAfterUnconfirmedRetries(t *testing.T) {
	withFastRecoveryTimers(t)
	unconfirmedPrereqMaxAttempts = 3

	infra := &fakeCleanupInfra{
		createInfraResults: []createAttemptResult{
			{err: errors.New("infra partially created before cli failure")},
		},
	}

	infraConfig, err := ensureInfraWithRecovery(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected unconfirmed infra error after retries")
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
	if !strings.Contains(err.Error(), "infrastructure creation remained unconfirmed after 3 attempts") {
		t.Fatalf("expected unconfirmed retry error, got %v", err)
	}
}

func TestRecoverFromUnconfirmedCreate_AdoptsClusterWhenItAppears(t *testing.T) {
	withFastProbeTimers(t)
	unconfirmedCreateProbeTimeout = 25 * time.Millisecond

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

	clusterID, err := recoverFromUnconfirmedCreate(
		context.Background(),
		client,
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster", EndpointAccess: "Private"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("recoverFromUnconfirmedCreate error = %v", err)
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

func TestRecoverFromUnconfirmedCreate_ReturnsErrorWithoutCleanupWhenReensureFails(t *testing.T) {
	withFastProbeTimers(t)
	withFastRecoveryTimers(t)

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{clusterID: "cluster-123"},
		},
	}
	infra := &fakeCleanupInfra{
		createInfraErr: errors.New("infra rerun failed"),
	}

	clusterID, err := recoverFromUnconfirmedCreate(
		context.Background(),
		client,
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		noopProgress(),
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

func TestRecoverFromUnconfirmedCreate_CleansUpAfterTimeout(t *testing.T) {
	withFastProbeTimers(t)

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{err: fmt.Errorf("%w: %q", ErrClusterNotFound, "test-cluster")},
		},
	}
	infra := &fakeCleanupInfra{}

	clusterID, err := recoverFromUnconfirmedCreate(
		context.Background(),
		client,
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("cluster creation response missing id field"),
		noopProgress(),
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

func TestRecoverFromUnconfirmedCreate_SkipsCleanupWhenProbeFails(t *testing.T) {
	withFastProbeTimers(t)

	client := &fakeClusterResolveClient{
		results: []resolveResult{
			{err: errors.New("list clusters: temporary backend error")},
		},
	}
	infra := &fakeCleanupInfra{}

	clusterID, err := recoverFromUnconfirmedCreate(
		context.Background(),
		client,
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"/tmp/jwks.json",
		[]string{"EXAMPLE=1"},
		true,
		true,
		fmt.Errorf("create cluster: request timeout"),
		noopProgress(),
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
	if !strings.Contains(err.Error(), "request timeout") || !strings.Contains(err.Error(), "probe for cluster after unconfirmed create") {
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
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
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
		noopProgress(),
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
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
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
		noopProgress(),
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
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
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
		noopProgress(),
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

func validIAMConfigForReconcileTest() map[string]any {
	return map[string]any{
		"workloadIdentityPool": map[string]any{
			"poolId":     "pool-123",
			"providerId": "provider-123",
		},
		"projectNumber": "123456789012",
		"serviceAccounts": map[string]any{
			"ctrlplane-op":     "controlplane@test-project.iam.gserviceaccount.com",
			"nodepool-mgmt":    "nodepool@test-project.iam.gserviceaccount.com",
			"cloud-controller": "controller@test-project.iam.gserviceaccount.com",
			"gcp-pd-csi":       "storage@test-project.iam.gserviceaccount.com",
			"image-registry":   "registry@test-project.iam.gserviceaccount.com",
			"cloud-network":    "network@test-project.iam.gserviceaccount.com",
		},
	}
}

func TestReconcilerReconcile_CleansHypershiftWorkspaceBeforeNodepoolReconcile(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origBuildCreateWorkspace := buildCreateWorkspaceWithTokenURL
	origReconcileNodepools := reconcileNodepoolsFn
	origPollClusterReady := pollClusterReadyFn
	origCompleteGuestRegistration := completeGuestRegistrationFn
	origPollDesiredNodepoolsHealthy := pollDesiredNodepoolsHealthyFn
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		buildCreateWorkspaceWithTokenURL = origBuildCreateWorkspace
		reconcileNodepoolsFn = origReconcileNodepools
		pollClusterReadyFn = origPollClusterReady
		completeGuestRegistrationFn = origCompleteGuestRegistration
		pollDesiredNodepoolsHealthyFn = origPollDesiredNodepoolsHealthy
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	workspaceDir, err := os.MkdirTemp("", "gcphcp-workspace-reconcile-*")
	if err != nil {
		t.Fatalf("os.MkdirTemp() error = %v", err)
	}
	buildCreateWorkspaceWithTokenURL = func(token string, _ TargetConfig, jwksJSON []byte, tokenURL string, cleanupCallbacks ...func() error) (*HypershiftWorkspace, error) {
		if token == "" {
			t.Fatal("workspace token is empty, want nonce")
		}
		if token == "caller-token" {
			t.Fatalf("workspace token = %q, do not pass raw caller token to forwarder-mode workspace", token)
		}
		if !strings.HasPrefix(tokenURL, "http://127.0.0.1:") {
			t.Fatalf("token_url = %q, want localhost forwarder", tokenURL)
		}
		if len(jwksJSON) == 0 {
			t.Fatal("expected generated JWKS payload")
		}
		return &HypershiftWorkspace{
			Env:              []string{"PATH=/usr/bin"},
			JWKSPath:         workspaceDir + "/jwks.json",
			tempDir:          workspaceDir,
			cleanupCallbacks: cleanupCallbacks,
		}, nil
	}

	reconcileNodepoolsFn = func(_ context.Context, _ nodepoolReconcileClient, _ string, _ string, _ []NodepoolSpec, _ *deliveryProgress) error {
		if _, err := os.Stat(workspaceDir); err == nil {
			t.Fatal("expected hypershift workspace to be cleaned before nodepool reconcile")
		}
		return nil
	}
	pollClusterReadyFn = func(context.Context, *CLSClient, string, *deliveryProgress) error { return nil }
	completeGuestRegistrationFn = func(context.Context, *CLSClient, string, string, domain.TargetID, *deliveryProgress) (string, BootstrapResult, error) {
		return "https://guest.example:6443", BootstrapResult{
			SATokenRef: "sa-token-ref",
			SAToken:    []byte("sa-token"),
		}, nil
	}
	pollDesiredNodepoolsHealthyFn = func(context.Context, nodepoolStatusClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"id":"c-123"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	infra := &fakeCleanupInfra{
		createIAMResults: []createAttemptResult{{result: validIAMConfigForReconcileTest()}},
	}
	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	output, err := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "PublicAndPrivate",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "stable",
			Nodepools: []NodepoolSpec{{
				ID:             "workers",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     boolPtr(true),
				UpgradeType:    "Replace",
			}},
		},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if output == nil {
		t.Fatal("expected reconcile output")
	}
}

func TestReconcilerReconcile_UsesLocalSTSForwarderForCreatePrereqs(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origReconcileNodepools := reconcileNodepoolsFn
	origPollClusterReady := pollClusterReadyFn
	origCompleteGuestRegistration := completeGuestRegistrationFn
	origPollDesiredNodepoolsHealthy := pollDesiredNodepoolsHealthyFn
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		reconcileNodepoolsFn = origReconcileNodepools
		pollClusterReadyFn = origPollClusterReady
		completeGuestRegistrationFn = origCompleteGuestRegistration
		pollDesiredNodepoolsHealthyFn = origPollDesiredNodepoolsHealthy
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	reconcileNodepoolsFn = func(context.Context, nodepoolReconcileClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}
	pollClusterReadyFn = func(context.Context, *CLSClient, string, *deliveryProgress) error { return nil }
	completeGuestRegistrationFn = func(context.Context, *CLSClient, string, string, domain.TargetID, *deliveryProgress) (string, BootstrapResult, error) {
		return "https://guest.example:6443", BootstrapResult{
			SATokenRef: "sa-token-ref",
			SAToken:    []byte("sa-token"),
		}, nil
	}
	pollDesiredNodepoolsHealthyFn = func(context.Context, nodepoolStatusClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"id":"c-123"}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	infra := &fakeCleanupInfra{
		createIAMDelay:   15 * time.Millisecond,
		createIAMResults: []createAttemptResult{{result: validIAMConfigForReconcileTest()}},
	}
	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	output, err := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "PublicAndPrivate",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "stable",
			Nodepools: []NodepoolSpec{{
				ID:             "workers",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     boolPtr(true),
				UpgradeType:    "Replace",
			}},
		},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if output == nil {
		t.Fatal("expected reconcile output")
	}

	if fakeAuth.callerToken != "caller-token" {
		t.Fatalf("broker auth caller token = %q, want caller-token", fakeAuth.callerToken)
	}
	if !strings.HasPrefix(infra.createIAMTokenURL, "http://127.0.0.1:") {
		t.Fatalf("create IAM token_url = %q, want localhost forwarder", infra.createIAMTokenURL)
	}
	if !strings.HasPrefix(infra.createInfraTokenURL, "http://127.0.0.1:") {
		t.Fatalf("create infra token_url = %q, want localhost forwarder", infra.createInfraTokenURL)
	}
	if infra.createIAMToken != "workforce-token" {
		t.Fatalf("create IAM forwarded token = %q, want workforce-token", infra.createIAMToken)
	}
	if infra.createInfraToken != "workforce-token" {
		t.Fatalf("create infra forwarded token = %q, want workforce-token", infra.createInfraToken)
	}
	if infra.createIAMSubjectToken == "" {
		t.Fatal("create IAM subject token is empty, want nonce")
	}
	if infra.createIAMSubjectToken == "caller-token" {
		t.Fatalf("create IAM subject token = %q, do not persist raw caller token in forwarder mode", infra.createIAMSubjectToken)
	}
	if infra.createInfraSubjectToken == "" {
		t.Fatal("create infra subject token is empty, want nonce")
	}
	if infra.createInfraSubjectToken == "caller-token" {
		t.Fatalf("create infra subject token = %q, do not persist raw caller token in forwarder mode", infra.createInfraSubjectToken)
	}
	if infra.createIAMQuotaProject != "test-project" {
		t.Fatalf("create IAM quota_project_id = %q, want test-project", infra.createIAMQuotaProject)
	}
	if infra.createInfraQuotaProject != "test-project" {
		t.Fatalf("create infra quota_project_id = %q, want test-project", infra.createInfraQuotaProject)
	}
}

func TestReconcilerReconcile_CreateClusterCleanupUsesLocalSTSForwarder(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	defer func() {
		newBrokerAuth = origNewBrokerAuth
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/clusters":
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	infra := &fakeCleanupInfra{
		createIAMResults: []createAttemptResult{{result: validIAMConfigForReconcileTest()}},
	}
	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	_, err := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "PublicAndPrivate",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "stable",
			Nodepools: []NodepoolSpec{{
				ID:             "workers",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     boolPtr(true),
				UpgradeType:    "Replace",
			}},
		},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected reconcile error")
	}
	if !strings.Contains(err.Error(), "create cluster") {
		t.Fatalf("error = %q, want create cluster context", err.Error())
	}

	if !strings.HasPrefix(infra.destroyInfraTokenURL, "http://127.0.0.1:") {
		t.Fatalf("destroy infra token_url = %q, want localhost forwarder", infra.destroyInfraTokenURL)
	}
	if !strings.HasPrefix(infra.destroyIAMTokenURL, "http://127.0.0.1:") {
		t.Fatalf("destroy IAM token_url = %q, want localhost forwarder", infra.destroyIAMTokenURL)
	}
	if infra.destroyInfraToken != "workforce-token" {
		t.Fatalf("destroy infra forwarded token = %q, want workforce-token", infra.destroyInfraToken)
	}
	if infra.destroyIAMToken != "workforce-token" {
		t.Fatalf("destroy IAM forwarded token = %q, want workforce-token", infra.destroyIAMToken)
	}
	if infra.destroyInfraSubjectToken == "" {
		t.Fatal("destroy infra subject token is empty, want nonce")
	}
	if infra.destroyInfraSubjectToken == "caller-token" {
		t.Fatalf("destroy infra subject token = %q, do not persist raw caller token in forwarder mode", infra.destroyInfraSubjectToken)
	}
	if infra.destroyIAMSubjectToken == "" {
		t.Fatal("destroy IAM subject token is empty, want nonce")
	}
	if infra.destroyIAMSubjectToken == "caller-token" {
		t.Fatalf("destroy IAM subject token = %q, do not persist raw caller token in forwarder mode", infra.destroyIAMSubjectToken)
	}
	if infra.destroyInfraQuotaProject != "test-project" {
		t.Fatalf("destroy infra quota_project_id = %q, want test-project", infra.destroyInfraQuotaProject)
	}
	if infra.destroyIAMQuotaProject != "test-project" {
		t.Fatalf("destroy IAM quota_project_id = %q, want test-project", infra.destroyIAMQuotaProject)
	}
}

func TestReconcilerDelete_WaitsForPSCCleanupBeforeBuildingHypershiftWorkspace(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origBuildDestroyWorkspace := buildDestroyWorkspaceWithTokenURL
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		buildDestroyWorkspaceWithTokenURL = origBuildDestroyWorkspace
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	infra := &fakeCleanupInfra{}
	buildDestroyWorkspaceWithTokenURL = func(token string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		if token == "" {
			t.Fatal("workspace token is empty, want nonce")
		}
		if token == "caller-token" {
			t.Fatalf("workspace token = %q, do not pass raw caller token to forwarder-mode workspace", token)
		}
		if infra.waitPSCCalls == 0 {
			t.Fatal("expected PSC cleanup to complete before destroy workspace materialization")
		}

		workspaceDir, err := os.MkdirTemp("", "gcphcp-workspace-delete-*")
		if err != nil {
			t.Fatalf("os.MkdirTemp() error = %v", err)
		}
		return &HypershiftWorkspace{
			Env:     []string{"PATH=/usr/bin"},
			tempDir: workspaceDir,
		}, nil
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

	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	err := reconciler.Delete(
		context.Background(),
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
}

func TestReconcilerDelete_UsesNonceForHypershiftEnvAndWorkforceTokenForPSCCleanup(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origBuildDestroyWorkspace := buildDestroyWorkspaceWithTokenURL
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		buildDestroyWorkspaceWithTokenURL = origBuildDestroyWorkspace
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	var gotHypershiftToken string
	buildDestroyWorkspaceWithTokenURL = func(token string, _ TargetConfig, _ string, _ ...func() error) (*HypershiftWorkspace, error) {
		gotHypershiftToken = token
		workspaceDir, err := os.MkdirTemp("", "gcphcp-workspace-delete-*")
		if err != nil {
			t.Fatalf("os.MkdirTemp() error = %v", err)
		}
		return &HypershiftWorkspace{
			Env:     []string{"PATH=/usr/bin"},
			tempDir: workspaceDir,
		}, nil
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
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if fakeAuth.callerToken != "caller-token" {
		t.Fatalf("broker auth caller token = %q, want caller-token", fakeAuth.callerToken)
	}
	if gotHypershiftToken == "" {
		t.Fatal("hypershift env token is empty, want nonce")
	}
	if gotHypershiftToken == "caller-token" {
		t.Fatalf("hypershift env token = %q, do not persist raw caller token in forwarder mode", gotHypershiftToken)
	}
	if infra.waitPSCWorkforceToken != "workforce-token" {
		t.Fatalf("PSC cleanup workforce token = %q, want workforce-token", infra.waitPSCWorkforceToken)
	}
}

func TestReconcilerDelete_UsesLocalSTSForwarderForHypershiftCleanup(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	defer func() {
		newBrokerAuth = origNewBrokerAuth
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-123","name":"test-cluster"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-123":
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
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if !strings.HasPrefix(infra.destroyInfraTokenURL, "http://127.0.0.1:") {
		t.Fatalf("destroy infra token_url = %q, want localhost forwarder", infra.destroyInfraTokenURL)
	}
	if infra.destroyInfraToken != "workforce-token" {
		t.Fatalf("destroy infra forwarded token = %q, want workforce-token", infra.destroyInfraToken)
	}
	if infra.destroyIAMToken != "workforce-token" {
		t.Fatalf("destroy IAM forwarded token = %q, want workforce-token", infra.destroyIAMToken)
	}
	if infra.destroyInfraSubjectToken == "caller-token" {
		t.Fatalf("destroy infra subject token = %q, do not persist raw caller token in forwarder mode", infra.destroyInfraSubjectToken)
	}
	if infra.destroyIAMSubjectToken == "caller-token" {
		t.Fatalf("destroy IAM subject token = %q, do not persist raw caller token in forwarder mode", infra.destroyIAMSubjectToken)
	}
	if infra.destroyInfraQuotaProject != "test-project" {
		t.Fatalf("destroy infra quota_project_id = %q, want test-project", infra.destroyInfraQuotaProject)
	}
	if infra.destroyIAMQuotaProject != "test-project" {
		t.Fatalf("destroy IAM quota_project_id = %q, want test-project", infra.destroyIAMQuotaProject)
	}
}

func TestReconcilerDelete_ContinuesCleanupAfterLongPSCCleanupWait(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	defer func() {
		newBrokerAuth = origNewBrokerAuth
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "near-expiry-workforce-token",
			WorkforceTokenExpiry: time.Now().Add(3 * time.Second),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-123","name":"test-cluster"}]}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/c-123":
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

	infra := &fakeCleanupInfra{waitPSCDelay: 1100 * time.Millisecond}
	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      server.URL,
			Audience: "test-audience",
		},
		infra: infra,
	}

	start := time.Now()
	err := reconciler.Delete(
		context.Background(),
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < infra.waitPSCDelay {
		t.Fatalf("Delete() elapsed = %v, want at least %v PSC wait", elapsed, infra.waitPSCDelay)
	}

	if infra.waitPSCWorkforceToken != "near-expiry-workforce-token" {
		t.Fatalf("PSC cleanup workforce token = %q, want near-expiry-workforce-token", infra.waitPSCWorkforceToken)
	}
	if infra.destroyInfraToken != "near-expiry-workforce-token" {
		t.Fatalf("destroy infra forwarded token = %q, want near-expiry-workforce-token", infra.destroyInfraToken)
	}
	if infra.destroyIAMToken != "near-expiry-workforce-token" {
		t.Fatalf("destroy IAM forwarded token = %q, want near-expiry-workforce-token", infra.destroyIAMToken)
	}
	if infra.destroyInfraQuotaProject != "test-project" {
		t.Fatalf("destroy infra quota_project_id = %q, want test-project", infra.destroyInfraQuotaProject)
	}
	if infra.destroyIAMQuotaProject != "test-project" {
		t.Fatalf("destroy IAM quota_project_id = %q, want test-project", infra.destroyIAMQuotaProject)
	}
}

func TestReconcilerReconcile_UpdatePathSkipsCreateInfra(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origReconcileNodepools := reconcileNodepoolsFn
	origPollClusterReady := pollClusterReadyFn
	origCompleteGuestRegistration := completeGuestRegistrationFn
	origPollDesiredNodepoolsHealthy := pollDesiredNodepoolsHealthyFn
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		reconcileNodepoolsFn = origReconcileNodepools
		pollClusterReadyFn = origPollClusterReady
		completeGuestRegistrationFn = origCompleteGuestRegistration
		pollDesiredNodepoolsHealthyFn = origPollDesiredNodepoolsHealthy
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

	var reconcileNodepoolsCalled bool
	var nodepoolClusterID string
	reconcileNodepoolsFn = func(_ context.Context, _ nodepoolReconcileClient, clusterID string, _ string, _ []NodepoolSpec, _ *deliveryProgress) error {
		reconcileNodepoolsCalled = true
		nodepoolClusterID = clusterID
		return nil
	}
	pollClusterReadyFn = func(context.Context, *CLSClient, string, *deliveryProgress) error { return nil }
	completeGuestRegistrationFn = func(context.Context, *CLSClient, string, string, domain.TargetID, *deliveryProgress) (string, BootstrapResult, error) {
		return "https://guest.example:6443", BootstrapResult{
			SATokenRef: "sa-token-ref",
			SAToken:    []byte("sa-token"),
		}, nil
	}
	pollDesiredNodepoolsHealthyFn = func(context.Context, nodepoolStatusClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}

	var updatedClusterID string
	var updatedSpec map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[{"id":"c-existing","name":"test-cluster"}]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-existing":
			fmt.Fprint(w, `{
				"id":"c-existing",
				"name":"test-cluster",
				"target_project_id":"test-project",
				"spec":{
					"infraID":"test-cluster",
					"issuerURL":"https://hypershift-test-cluster-oidc",
					"serviceAccountSigningKey":"existing-key",
					"releaseVersion":"4.20.0",
					"channelGroup":"stable",
					"platform":{
						"type":"GCP",
						"gcp":{
							"projectID":"test-project",
							"region":"us-central1",
							"network":"test-cluster-network",
							"subnet":"test-cluster-subnet",
							"endpointAccess":"PublicAndPrivate",
							"workloadIdentity":{"projectNumber":"123456789"}
						}
					}
				}
			}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/v1/clusters/"):
			updatedClusterID = strings.TrimPrefix(r.URL.Path, "/api/v1/clusters/")
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			updatedSpec = body
			fmt.Fprintf(w, `{"id":"%s"}`, updatedClusterID)
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

	output, err := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "Private",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "fast",
			Nodepools: []NodepoolSpec{{
				ID:             "workers",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     boolPtr(true),
				UpgradeType:    "Replace",
			}},
		},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if output == nil {
		t.Fatal("expected reconcile output")
	}

	if updatedClusterID != "c-existing" {
		t.Fatalf("updated cluster ID = %q, want c-existing", updatedClusterID)
	}

	specMap, ok := updatedSpec["spec"].(map[string]any)
	if !ok {
		t.Fatal("updated spec missing 'spec' map")
	}
	if specMap["releaseVersion"] != "4.22.0" {
		t.Fatalf("updated releaseVersion = %v, want 4.22.0", specMap["releaseVersion"])
	}
	if specMap["channelGroup"] != "fast" {
		t.Fatalf("updated channelGroup = %v, want fast", specMap["channelGroup"])
	}
	platform, _ := specMap["platform"].(map[string]any)
	gcp, _ := platform["gcp"].(map[string]any)
	if gcp["endpointAccess"] != "Private" {
		t.Fatalf("updated endpointAccess = %v, want Private", gcp["endpointAccess"])
	}
	if specMap["serviceAccountSigningKey"] != "existing-key" {
		t.Fatalf("updated signingKey = %v, want existing-key (preserved)", specMap["serviceAccountSigningKey"])
	}

	if !reconcileNodepoolsCalled {
		t.Fatal("expected reconcileNodepools to be called on update path")
	}
	if nodepoolClusterID != "c-existing" {
		t.Fatalf("nodepool reconcile cluster ID = %q, want c-existing", nodepoolClusterID)
	}

	if len(infra.ops) != 0 {
		t.Fatalf("expected no infra operations on update path, got %v", infra.ops)
	}
}

func TestReconcilerReconcile_AuthExpiredReturnsAuthExpiredError(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	defer func() {
		newBrokerAuth = origNewBrokerAuth
	}()

	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return &fakeBrokerAuth{
			err: newAuthExpiredError(fmt.Errorf("STS returned status 400: invalid_grant")),
		}
	}

	reconciler := &Reconciler{
		gateway: GatewayConfig{
			URL:      "https://unused.invalid",
			Audience: "test-audience",
		},
		infra: &fakeCleanupInfra{},
	}

	_, err := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "PublicAndPrivate",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "stable",
			Nodepools: []NodepoolSpec{{
				ID:             "workers",
				Replicas:       2,
				InstanceType:   "n1-standard-4",
				RootVolumeSize: 128,
				RootVolumeType: "pd-standard",
				AutoRepair:     boolPtr(true),
				UpgradeType:    "Replace",
			}},
		},
		TargetConfig{
			GCPProject:        "test-project",
			Region:            "us-central1",
			WorkforcePool:     "test-pool",
			WorkforceProvider: "test-provider",
			BrokerSAEmail:     "broker@example.com",
		},
		"expired-token",
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected auth exchange error")
	}
	if !IsAuthExpiredError(err) {
		t.Fatalf("expected IsAuthExpiredError = true, got false; error was: %v", err)
	}

	result := deliveryResultForReconcileError(err)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("delivery state = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "credentials expired") {
		t.Fatalf("delivery message = %q, want 'credentials expired' context", result.Message)
	}
}

func TestBuildCLSClusterUpdateSpec_MalformedObservedCluster(t *testing.T) {
	spec := ClusterSpec{
		ResourceName:   "clusters/test-cluster",
		EndpointAccess: "Private",
		ReleaseVersion: "4.22.0",
		ChannelGroup:   "stable",
	}

	tests := []struct {
		name     string
		observed map[string]any
		want     string
	}{
		{
			name:     "missing target_project_id",
			observed: map[string]any{"spec": map[string]any{}},
			want:     "target_project_id",
		},
		{
			name:     "missing spec object",
			observed: map[string]any{"target_project_id": "proj-123"},
			want:     "missing spec object",
		},
		{
			name: "missing platform object",
			observed: map[string]any{
				"target_project_id": "proj-123",
				"spec":              map[string]any{"infraID": "infra-123"},
			},
			want: "missing platform object",
		},
		{
			name: "missing gcp object",
			observed: map[string]any{
				"target_project_id": "proj-123",
				"spec": map[string]any{
					"infraID":  "infra-123",
					"platform": map[string]any{"type": "GCP"},
				},
			},
			want: "missing gcp object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildCLSClusterUpdateSpec(spec, tt.observed)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestDeleteClusterIfPresent_ResolveError(t *testing.T) {
	client := &fakeClusterDeleteClient{
		resolveErr: errors.New("list clusters: connection refused"),
	}

	_, _, err := deleteClusterIfPresent(context.Background(), client, "test-cluster", noopProgress())
	if err == nil {
		t.Fatal("expected resolve error")
	}
	if !strings.Contains(err.Error(), "resolve cluster ID") {
		t.Fatalf("error = %q, want 'resolve cluster ID' context", err.Error())
	}
	if len(client.deleteIDs) != 0 {
		t.Fatalf("expected no delete calls, got %v", client.deleteIDs)
	}
}

func TestDeleteClusterIfPresent_DeleteError(t *testing.T) {
	client := &fakeClusterDeleteClientWithDeleteErr{
		clusterID: "c-123",
		deleteErr: errors.New("backend rejected delete"),
	}

	_, _, err := deleteClusterIfPresent(context.Background(), client, "test-cluster", noopProgress())
	if err == nil {
		t.Fatal("expected delete error")
	}
	if !strings.Contains(err.Error(), "delete cluster") {
		t.Fatalf("error = %q, want 'delete cluster' context", err.Error())
	}
}

type fakeClusterDeleteClientWithDeleteErr struct {
	clusterID string
	deleteErr error
}

func (f *fakeClusterDeleteClientWithDeleteErr) ResolveClusterID(_ context.Context, _ string) (string, error) {
	return f.clusterID, nil
}

func (f *fakeClusterDeleteClientWithDeleteErr) DeleteCluster(_ context.Context, _ string) error {
	return f.deleteErr
}

func TestWaitForDeleteCleanupPrereqs_SkipsWhenClusterIDEmpty(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := waitForDeleteCleanupPrereqs(
		context.Background(),
		infra,
		"",
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		noopProgress(),
	)
	if err != nil {
		t.Fatalf("waitForDeleteCleanupPrereqs() error = %v", err)
	}
	if infra.waitPSCCalls != 0 {
		t.Fatalf("expected no PSC wait calls, got %d", infra.waitPSCCalls)
	}
}

func TestWaitForDeleteCleanupPrereqs_ReturnsPSCError(t *testing.T) {
	infra := &fakeCleanupInfra{
		waitPSCErr: errors.New("compute API unavailable"),
	}

	err := waitForDeleteCleanupPrereqs(
		context.Background(),
		infra,
		"cluster-123",
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		"workforce-token",
		noopProgress(),
	)
	if err == nil {
		t.Fatal("expected PSC cleanup error")
	}
	if !strings.Contains(err.Error(), "wait for PSC cleanup") {
		t.Fatalf("error = %q, want 'wait for PSC cleanup' context", err.Error())
	}
}

func TestCleanupCreateResources_IAMOnlyNoInfra(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		false,
		true,
	)
	if err != nil {
		t.Fatalf("cleanupCreateResources() error = %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "iam:test-cluster:project-123" {
		t.Fatalf("cleanup operations = %q, want IAM only", got)
	}
}

func TestCleanupCreateResources_InfraOnlyNoIAM(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		true,
		false,
	)
	if err != nil {
		t.Fatalf("cleanupCreateResources() error = %v", err)
	}
	if got := strings.Join(infra.ops, ","); got != "infra:test-cluster:project-123:us-central1" {
		t.Fatalf("cleanup operations = %q, want infra only", got)
	}
}

func TestCleanupCreateResources_DestroyIAMErrorJoined(t *testing.T) {
	infra := &fakeCleanupInfra{
		destroyIAMErr: errors.New("iam permission denied"),
	}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		true,
		true,
	)
	if err == nil {
		t.Fatal("expected IAM cleanup error")
	}
	if !strings.Contains(err.Error(), "destroy IAM") {
		t.Fatalf("error = %q, want 'destroy IAM' context", err.Error())
	}
}

func TestCleanupCreateResources_NeitherCreatedReturnsNil(t *testing.T) {
	infra := &fakeCleanupInfra{}

	err := cleanupCreateResources(
		context.Background(),
		infra,
		ClusterSpec{ResourceName: "clusters/test-cluster"},
		TargetConfig{GCPProject: "project-123", Region: "us-central1"},
		[]string{"EXAMPLE=1"},
		false,
		false,
	)
	if err != nil {
		t.Fatalf("cleanupCreateResources() error = %v", err)
	}
	if len(infra.ops) != 0 {
		t.Fatalf("expected no ops, got %v", infra.ops)
	}
}

func TestReconcilerReconcile_FailureSnapshotEmittedOnError(t *testing.T) {
	origNewBrokerAuth := newBrokerAuth
	origBuildCreateWorkspace := buildCreateWorkspaceWithTokenURL
	origPollClusterReady := pollClusterReadyFn
	defer func() {
		newBrokerAuth = origNewBrokerAuth
		buildCreateWorkspaceWithTokenURL = origBuildCreateWorkspace
		pollClusterReadyFn = origPollClusterReady
	}()

	fakeAuth := &fakeBrokerAuth{
		result: BrokerAuthResult{
			BrokerToken:          "broker-token",
			BrokerEmail:          "broker@example.com",
			WorkforceToken:       "workforce-token",
			WorkforceTokenExpiry: time.Now().Add(time.Hour),
		},
	}
	newBrokerAuth = func(BrokerAuthConfig) brokerAuthExchanger {
		return fakeAuth
	}

	origReconcileNodepools := reconcileNodepoolsFn
	origCompleteGuestRegistration := completeGuestRegistrationFn
	origPollDesiredNodepoolsHealthy := pollDesiredNodepoolsHealthyFn
	defer func() {
		reconcileNodepoolsFn = origReconcileNodepools
		completeGuestRegistrationFn = origCompleteGuestRegistration
		pollDesiredNodepoolsHealthyFn = origPollDesiredNodepoolsHealthy
	}()

	reconcileNodepoolsFn = func(context.Context, nodepoolReconcileClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}
	completeGuestRegistrationFn = func(context.Context, *CLSClient, string, string, domain.TargetID, *deliveryProgress) (string, BootstrapResult, error) {
		return "", BootstrapResult{}, nil
	}
	pollDesiredNodepoolsHealthyFn = func(context.Context, nodepoolStatusClient, string, string, []NodepoolSpec, *deliveryProgress) error {
		return nil
	}

	workspaceDir, err := os.MkdirTemp("", "gcphcp-snapshot-test-*")
	if err != nil {
		t.Fatalf("os.MkdirTemp() error = %v", err)
	}
	buildCreateWorkspaceWithTokenURL = func(token string, _ TargetConfig, jwksJSON []byte, tokenURL string, cleanupCallbacks ...func() error) (*HypershiftWorkspace, error) {
		if token == "" {
			t.Fatal("workspace token is empty, want nonce")
		}
		if token == "caller-token" {
			t.Fatalf("workspace token = %q, do not pass raw caller token to forwarder-mode workspace", token)
		}
		if !strings.HasPrefix(tokenURL, "http://127.0.0.1:") {
			t.Fatalf("token_url = %q, want localhost forwarder", tokenURL)
		}
		return &HypershiftWorkspace{
			Env:              []string{"PATH=/usr/bin"},
			JWKSPath:         workspaceDir + "/jwks.json",
			tempDir:          workspaceDir,
			cleanupCallbacks: cleanupCallbacks,
		}, nil
	}

	pollClusterReadyFn = func(context.Context, *CLSClient, string, *deliveryProgress) error {
		return fmt.Errorf("timeout waiting for cluster to become ready")
	}

	var snapshotRequested bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"clusters":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/clusters":
			fmt.Fprint(w, `{"id":"c-123"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-123":
			snapshotRequested = true
			fmt.Fprint(w, `{"id":"c-123","name":"test-cluster","spec":{"releaseVersion":"4.22.0"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/c-123/status":
			fmt.Fprint(w, `{"status":{"phase":"Failed","reason":"Timeout","message":"timed out"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodepools":
			fmt.Fprint(w, `{"nodepools":[]}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	infra := &fakeCleanupInfra{
		createIAMResults: []createAttemptResult{{result: validIAMConfigForReconcileTest()}},
	}
	reconciler := &Reconciler{
		gateway: GatewayConfig{URL: server.URL, Audience: "test-audience"},
		infra:   infra,
	}

	_, reconcileErr := reconciler.Ensure(
		context.Background(),
		ClusterSpec{
			ResourceName:   "clusters/test-cluster",
			EndpointAccess: "PublicAndPrivate",
			ReleaseVersion: "4.22.0",
			ChannelGroup:   "stable",
			Nodepools: []NodepoolSpec{{
				ID: "workers", Replicas: 2, InstanceType: "n1-standard-4",
				RootVolumeSize: 128, RootVolumeType: "pd-standard",
				AutoRepair: boolPtr(true), UpgradeType: "Replace",
			}},
		},
		TargetConfig{
			GCPProject: "test-project", Region: "us-central1",
			WorkforcePool: "test-pool", WorkforceProvider: "test-provider",
			BrokerSAEmail: "broker@example.com",
		},
		"caller-token",
		noopProgress(),
	)
	if reconcileErr == nil {
		t.Fatal("expected reconcile error from pollClusterReady timeout")
	}
	if !snapshotRequested {
		t.Fatal("expected failure snapshot to request cluster status via GetCluster")
	}
}

func TestRetryUnconfirmedPrereqCreate_ContextCancelledMidRetry(t *testing.T) {
	withFastRecoveryTimers(t)
	unconfirmedPrereqMaxAttempts = 5
	unconfirmedPrereqRetryInterval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	_, err := retryUnconfirmedPrereqCreate(ctx, noopProgress(), "test resource", func() (map[string]any, error) {
		attempts++
		if attempts == 2 {
			cancel()
		}
		return nil, fmt.Errorf("attempt %d failed", attempts)
	})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %q, want context canceled context", err.Error())
	}
	if !strings.Contains(err.Error(), "attempt 1") || !strings.Contains(err.Error(), "attempt 2") {
		t.Fatalf("error = %q, want both attempt errors preserved", err.Error())
	}
}
