package gcphcp

import (
	"context"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeNodepoolClient struct {
	listedNodepools []map[string]any

	createdSpecs []map[string]any
	updatedSpecs map[string]map[string]any
	deletedIDs   []string
}

type fakeCleanupInfra struct {
	ops []string
}

func (f *fakeCleanupInfra) DestroyInfra(_ context.Context, infraID, projectID, region string, _ []string) error {
	f.ops = append(f.ops, "infra:"+infraID+":"+projectID+":"+region)
	return nil
}

func (f *fakeCleanupInfra) DestroyIAM(_ context.Context, infraID, projectID string, _ []string) error {
	f.ops = append(f.ops, "iam:"+infraID+":"+projectID)
	return nil
}

type fakeClusterDeleteClient struct {
	clusterID  string
	resolveErr error
	deleteIDs  []string
}

func (f *fakeClusterDeleteClient) ResolveClusterID(_ context.Context, _ string) (string, error) {
	return f.clusterID, f.resolveErr
}

func (f *fakeClusterDeleteClient) DeleteCluster(_ context.Context, clusterID string) error {
	f.deleteIDs = append(f.deleteIDs, clusterID)
	return nil
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
