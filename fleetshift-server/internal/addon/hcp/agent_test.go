package hcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func makeManifest(t *testing.T, spec ClusterSpec) domain.Manifest {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Manifest{
		ResourceType: ClusterResourceType,
		Raw:          raw,
	}
}

func TestValidateManifests_ValidMinimalSpec(t *testing.T) {
	spec := ClusterSpec{
		Name:       "test-cluster",
		RoleARN:    "arn:aws:iam::123456789012:role/test",
		BaseDomain: "example.com",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	specs, err := validateManifests(manifests)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("got %d specs, want 1", len(specs))
	}

	got := specs[0]
	if got.Name != "test-cluster" {
		t.Errorf("Name = %q, want %q", got.Name, "test-cluster")
	}
	if got.RoleARN != "arn:aws:iam::123456789012:role/test" {
		t.Errorf("RoleARN = %q, want correct ARN", got.RoleARN)
	}
}

func TestValidateManifests_DefaultsApplied(t *testing.T) {
	spec := ClusterSpec{
		Name:       "test-cluster",
		RoleARN:    "arn:aws:iam::123456789012:role/test",
		BaseDomain: "example.com",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	specs, err := validateManifests(manifests)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := specs[0]
	if got.NodePools[0].InstanceType != "m6i.xlarge" {
		t.Errorf("InstanceType = %q, want %q", got.NodePools[0].InstanceType, "m6i.xlarge")
	}
	if got.NodePools[0].Arch != "amd64" {
		t.Errorf("Arch = %q, want %q", got.NodePools[0].Arch, "amd64")
	}
	if got.ControlPlaneAvailability != "HighlyAvailable" {
		t.Errorf("ControlPlaneAvailability = %q, want %q", got.ControlPlaneAvailability, "HighlyAvailable")
	}
}

func TestValidateManifests_MissingName(t *testing.T) {
	spec := ClusterSpec{
		RoleARN: "arn:aws:iam::123456789012:role/test",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	_, err := validateManifests(manifests)
	if err == nil {
		t.Fatal("expected error for missing name")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

func TestValidateManifests_MissingRoleARN(t *testing.T) {
	spec := ClusterSpec{
		Name: "test-cluster",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	_, err := validateManifests(manifests)
	if err == nil {
		t.Fatal("expected error for missing roleArn")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

func TestValidateManifests_MissingBaseDomain(t *testing.T) {
	spec := ClusterSpec{
		Name:    "test-cluster",
		RoleARN: "arn:aws:iam::123456789012:role/test",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	_, err := validateManifests(manifests)
	if err == nil {
		t.Fatal("expected error for missing baseDomain")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

func TestValidateManifests_EmptyNodePools(t *testing.T) {
	spec := ClusterSpec{
		Name:       "test-cluster",
		RoleARN:    "arn:aws:iam::123456789012:role/test",
		BaseDomain: "example.com",
	}
	manifests := []domain.Manifest{makeManifest(t, spec)}

	_, err := validateManifests(manifests)
	if err == nil {
		t.Fatal("expected error for empty nodePools")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

// --- Agent-level tests ---

// fakeMgmt implements mgmtCluster for unit tests.
type fakeMgmt struct {
	applyErr     error
	waitErr      error
	kubeconfigErr error
	kubeconfig   []byte
	deleteErr    error

	mu             sync.Mutex
	applyCalled    bool
	deleteClusters []string
}

func (f *fakeMgmt) applyResources(_ context.Context, _ hyperv1.HostedCluster, _ []hyperv1.NodePool, _ []corev1.Secret) error {
	f.mu.Lock()
	f.applyCalled = true
	f.mu.Unlock()
	return f.applyErr
}

func (f *fakeMgmt) waitForAvailable(_ context.Context, _ string) error {
	return f.waitErr
}

func (f *fakeMgmt) getAdminKubeconfig(_ context.Context, _ string) ([]byte, error) {
	if f.kubeconfigErr != nil {
		return nil, f.kubeconfigErr
	}
	return f.kubeconfig, nil
}

func (f *fakeMgmt) deleteNodePools(_ context.Context, spec ClusterSpec) error {
	return f.deleteErr
}

func (f *fakeMgmt) deleteHostedCluster(_ context.Context, name string) error {
	f.mu.Lock()
	f.deleteClusters = append(f.deleteClusters, name)
	f.mu.Unlock()
	return f.deleteErr
}

func (f *fakeMgmt) waitForDeletion(_ context.Context, _ string) error {
	return f.deleteErr
}

func validSpec() ClusterSpec {
	return ClusterSpec{
		Name:       "test-cluster",
		InfraID:    "test-infra",
		RoleARN:    "arn:aws:iam::123456789012:role/test",
		Region:     "us-east-1",
		BaseDomain: "example.com",
		NodePools: []NodePoolSpec{
			{Name: "default", Replicas: 2},
		},
	}
}

// stubInfraCreator wraps CreateInfra calls so tests with nil AWS
// clients don't panic in the async goroutine. The fakeMgmt already
// intercepts K8s calls; we also need the AWS calls to either succeed
// or fail gracefully. We use a cancelled context so the goroutine
// exits quickly when we only care about the synchronous return.

func TestDeliver_ReturnsAccepted(t *testing.T) {
	// Use a cancelled context so deliverAsync exits early without
	// touching nil AWS clients.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		nil, nil, nil,
		withMgmtCluster(&fakeMgmt{}),
	)

	manifests := []domain.Manifest{makeManifest(t, validSpec())}
	result, err := agent.Deliver(ctx, domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, nil, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("state = %v, want Accepted", result.State)
	}
}

func TestDeliver_InvalidManifest_ReturnsFailed(t *testing.T) {
	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		nil, nil, nil,
	)

	// Missing name and roleArn
	badSpec := ClusterSpec{
		NodePools: []NodePoolSpec{{Name: "x", Replicas: 1}},
	}
	manifests := []domain.Manifest{makeManifest(t, badSpec)}
	result, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, nil, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected error for invalid manifest")
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("state = %v, want Failed", result.State)
	}
}

func TestDeliver_AttestationSkippedWhenNoVerifier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		nil, nil, nil,
		withMgmtCluster(&fakeMgmt{}),
	)

	manifests := []domain.Manifest{makeManifest(t, validSpec())}
	att := &domain.Attestation{} // non-nil attestation but no verifier

	result, err := agent.Deliver(ctx, domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No verifier configured, so attestation is skipped — should still accept.
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("state = %v, want Accepted", result.State)
	}
}

func TestRemove_CallsDeleteOnMgmtCluster(t *testing.T) {
	mgmt := &fakeMgmt{}
	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		&mockEC2{}, &mockIAM{}, &mockRoute53{},
		withMgmtCluster(mgmt),
	)

	manifests := []domain.Manifest{makeManifest(t, validSpec())}
	err := agent.Remove(context.Background(), domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mgmt.deleteClusters) != 1 || mgmt.deleteClusters[0] != "test-cluster" {
		t.Errorf("deleteClusters = %v, want [test-cluster]", mgmt.deleteClusters)
	}
}

func TestRemove_InvalidManifest_ReturnsError(t *testing.T) {
	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		nil, nil, nil,
	)

	badSpec := ClusterSpec{} // missing name
	manifests := []domain.Manifest{makeManifest(t, badSpec)}
	err := agent.Remove(context.Background(), domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid manifest")
	}
}

func TestRemove_DeleteError_Propagated(t *testing.T) {
	mgmt := &fakeMgmt{deleteErr: fmt.Errorf("k8s unavailable")}
	agent := NewAgent(
		AgentConfig{AWSRegion: "us-east-1"},
		&mockEC2{}, &mockIAM{}, &mockRoute53{},
		withMgmtCluster(mgmt),
	)

	manifests := []domain.Manifest{makeManifest(t, validSpec())}
	err := agent.Remove(context.Background(), domain.TargetInfo{}, "", manifests, domain.DeliveryAuth{}, nil, nil)
	if err == nil {
		t.Fatal("expected error from delete failure")
	}
}
