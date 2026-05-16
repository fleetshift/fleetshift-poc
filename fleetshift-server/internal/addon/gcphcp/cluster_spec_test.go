package gcphcp_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestParseClusterSpec_ValidMinimalSpec(t *testing.T) {
	raw := json.RawMessage(`{"name": "test-cluster"}`)

	spec, err := gcphcp.ParseClusterSpec(raw)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.Name != "test-cluster" {
		t.Errorf("expected Name=test-cluster, got %s", spec.Name)
	}
}

func TestParseClusterSpec_MissingName(t *testing.T) {
	raw := json.RawMessage(`{}`)

	_, err := gcphcp.ParseClusterSpec(raw)
	if err == nil {
		t.Fatal("expected error for missing name, got nil")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected error to wrap ErrInvalidArgument, got %v", err)
	}
}

func TestParseClusterSpec_NameTooLong(t *testing.T) {
	raw := json.RawMessage(`{"name": "this-cluster-name-is-way-too-long"}`)

	_, err := gcphcp.ParseClusterSpec(raw)
	if err == nil {
		t.Fatal("expected error for name too long, got nil")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected error to wrap ErrInvalidArgument, got %v", err)
	}
}

func TestParseClusterSpec_InvalidNamePattern(t *testing.T) {
	tests := []struct {
		name    string
		rawName string
	}{
		{"uppercase", "TestCluster"},
		{"starts with number", "1cluster"},
		{"has underscore", "test_cluster"},
		{"has dots", "test.cluster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := json.RawMessage(`{"name": "` + tt.rawName + `"}`)

			_, err := gcphcp.ParseClusterSpec(raw)
			if err == nil {
				t.Fatal("expected error for invalid name pattern, got nil")
			}
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("expected error to wrap ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestParseClusterSpec_ValidNamePatterns(t *testing.T) {
	tests := []string{
		"a",
		"z",
		"test",
		"test-cluster",
		"test-123",
		"a-b-c-d-e-f-g",
		"test-cluster-99",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			raw := json.RawMessage(`{"name": "` + name + `"}`)

			spec, err := gcphcp.ParseClusterSpec(raw)
			if err != nil {
				t.Fatalf("ParseClusterSpec failed for valid name %q: %v", name, err)
			}
			if spec.Name != name {
				t.Errorf("expected Name=%s, got %s", name, spec.Name)
			}
		})
	}
}

func TestApplyDefaults_EmptySpec(t *testing.T) {
	spec := gcphcp.ClusterSpec{Name: "test"}
	spec.ApplyDefaults()

	if spec.EndpointAccess != "PublicAndPrivate" {
		t.Errorf("expected EndpointAccess=PublicAndPrivate, got %s", spec.EndpointAccess)
	}

	if len(spec.Nodepools) != 1 {
		t.Fatalf("expected 1 nodepool, got %d", len(spec.Nodepools))
	}

	np := spec.Nodepools[0]
	if np.Name != "test-nodepool-1" {
		t.Errorf("expected nodepool Name=test-nodepool-1, got %s", np.Name)
	}
	if np.Replicas != 2 {
		t.Errorf("expected nodepool Replicas=2, got %d", np.Replicas)
	}
	if np.InstanceType != "n1-standard-4" {
		t.Errorf("expected nodepool InstanceType=n1-standard-4, got %s", np.InstanceType)
	}
	if np.RootVolumeSize != 128 {
		t.Errorf("expected nodepool RootVolumeSize=128, got %d", np.RootVolumeSize)
	}
	if np.RootVolumeType != "pd-standard" {
		t.Errorf("expected nodepool RootVolumeType=pd-standard, got %s", np.RootVolumeType)
	}
	if !np.AutoRepair {
		t.Errorf("expected nodepool AutoRepair=true, got false")
	}
	if np.UpgradeType != "Replace" {
		t.Errorf("expected nodepool UpgradeType=Replace, got %s", np.UpgradeType)
	}
}

func TestApplyDefaults_PreservesUserOverrides(t *testing.T) {
	spec := gcphcp.ClusterSpec{
		Name:           "test",
		EndpointAccess: "Private",
		ReleaseVersion: "4.14.0",
		ChannelGroup:   "stable",
		Nodepools: []gcphcp.NodepoolSpec{
			{
				Name:           "custom-pool",
				Replicas:       5,
				InstanceType:   "n1-standard-8",
				RootVolumeSize: 256,
				RootVolumeType: "pd-ssd",
				AutoRepair:     false,
				UpgradeType:    "InPlace",
			},
		},
	}

	spec.ApplyDefaults()

	if spec.EndpointAccess != "Private" {
		t.Errorf("expected EndpointAccess=Private, got %s", spec.EndpointAccess)
	}
	if spec.ReleaseVersion != "4.14.0" {
		t.Errorf("expected ReleaseVersion=4.14.0, got %s", spec.ReleaseVersion)
	}
	if spec.ChannelGroup != "stable" {
		t.Errorf("expected ChannelGroup=stable, got %s", spec.ChannelGroup)
	}

	if len(spec.Nodepools) != 1 {
		t.Fatalf("expected 1 nodepool, got %d", len(spec.Nodepools))
	}

	np := spec.Nodepools[0]
	if np.Name != "custom-pool" {
		t.Errorf("expected nodepool Name=custom-pool, got %s", np.Name)
	}
	if np.Replicas != 5 {
		t.Errorf("expected nodepool Replicas=5, got %d", np.Replicas)
	}
	if np.InstanceType != "n1-standard-8" {
		t.Errorf("expected nodepool InstanceType=n1-standard-8, got %s", np.InstanceType)
	}
	if np.RootVolumeSize != 256 {
		t.Errorf("expected nodepool RootVolumeSize=256, got %d", np.RootVolumeSize)
	}
	if np.RootVolumeType != "pd-ssd" {
		t.Errorf("expected nodepool RootVolumeType=pd-ssd, got %s", np.RootVolumeType)
	}
	if np.AutoRepair {
		t.Errorf("expected nodepool AutoRepair=false, got true")
	}
	if np.UpgradeType != "InPlace" {
		t.Errorf("expected nodepool UpgradeType=InPlace, got %s", np.UpgradeType)
	}
}

func TestApplyDefaults_FillsPartialNodepool(t *testing.T) {
	spec := gcphcp.ClusterSpec{
		Name: "test",
		Nodepools: []gcphcp.NodepoolSpec{
			{
				Name: "partial-pool",
				// Only Name is set, rest should get defaults
			},
		},
	}

	spec.ApplyDefaults()

	np := spec.Nodepools[0]
	if np.Name != "partial-pool" {
		t.Errorf("expected nodepool Name=partial-pool, got %s", np.Name)
	}
	if np.Replicas != 2 {
		t.Errorf("expected nodepool Replicas=2, got %d", np.Replicas)
	}
	if np.InstanceType != "n1-standard-4" {
		t.Errorf("expected nodepool InstanceType=n1-standard-4, got %s", np.InstanceType)
	}
	if np.RootVolumeSize != 128 {
		t.Errorf("expected nodepool RootVolumeSize=128, got %d", np.RootVolumeSize)
	}
	if np.RootVolumeType != "pd-standard" {
		t.Errorf("expected nodepool RootVolumeType=pd-standard, got %s", np.RootVolumeType)
	}
	if !np.AutoRepair {
		t.Errorf("expected nodepool AutoRepair=true, got false")
	}
	if np.UpgradeType != "Replace" {
		t.Errorf("expected nodepool UpgradeType=Replace, got %s", np.UpgradeType)
	}
}

func TestApplyDefaults_MultipleUnnamedNodepoolsGetUniqueNames(t *testing.T) {
	spec := gcphcp.ClusterSpec{
		Name: "test",
		Nodepools: []gcphcp.NodepoolSpec{
			{},
			{},
			{Name: "custom"},
		},
	}

	spec.ApplyDefaults()

	if spec.Nodepools[0].Name != "test-nodepool-1" {
		t.Fatalf("expected first unnamed nodepool to be test-nodepool-1, got %s", spec.Nodepools[0].Name)
	}
	if spec.Nodepools[1].Name != "test-nodepool-2" {
		t.Fatalf("expected second unnamed nodepool to be test-nodepool-2, got %s", spec.Nodepools[1].Name)
	}
	if spec.Nodepools[2].Name != "custom" {
		t.Fatalf("expected named nodepool to be preserved, got %s", spec.Nodepools[2].Name)
	}
}

func TestParseClusterSpec_FullSpec(t *testing.T) {
	raw := json.RawMessage(`{
		"name": "prod-cluster",
		"endpointAccess": "Private",
		"releaseVersion": "4.14.0",
		"channelGroup": "stable",
		"nodepools": [
			{
				"name": "worker-pool",
				"replicas": 3,
				"instanceType": "n1-standard-8",
				"rootVolumeSize": 256,
				"rootVolumeType": "pd-ssd",
				"autoRepair": false,
				"upgradeType": "InPlace"
			}
		]
	}`)

	spec, err := gcphcp.ParseClusterSpec(raw)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.Name != "prod-cluster" {
		t.Errorf("expected Name=prod-cluster, got %s", spec.Name)
	}
	if spec.EndpointAccess != "Private" {
		t.Errorf("expected EndpointAccess=Private, got %s", spec.EndpointAccess)
	}
	if spec.ReleaseVersion != "4.14.0" {
		t.Errorf("expected ReleaseVersion=4.14.0, got %s", spec.ReleaseVersion)
	}
	if spec.ChannelGroup != "stable" {
		t.Errorf("expected ChannelGroup=stable, got %s", spec.ChannelGroup)
	}

	if len(spec.Nodepools) != 1 {
		t.Fatalf("expected 1 nodepool, got %d", len(spec.Nodepools))
	}

	np := spec.Nodepools[0]
	if np.Name != "worker-pool" {
		t.Errorf("expected nodepool Name=worker-pool, got %s", np.Name)
	}
	if np.Replicas != 3 {
		t.Errorf("expected nodepool Replicas=3, got %d", np.Replicas)
	}
	if np.InstanceType != "n1-standard-8" {
		t.Errorf("expected nodepool InstanceType=n1-standard-8, got %s", np.InstanceType)
	}
	if np.RootVolumeSize != 256 {
		t.Errorf("expected nodepool RootVolumeSize=256, got %d", np.RootVolumeSize)
	}
	if np.RootVolumeType != "pd-ssd" {
		t.Errorf("expected nodepool RootVolumeType=pd-ssd, got %s", np.RootVolumeType)
	}
	if np.AutoRepair {
		t.Errorf("expected nodepool AutoRepair=false, got true")
	}
	if np.UpgradeType != "InPlace" {
		t.Errorf("expected nodepool UpgradeType=InPlace, got %s", np.UpgradeType)
	}
}

func TestParseClusterSpec_NegativeNodepoolNumbers(t *testing.T) {
	tests := []struct {
		name    string
		rawJSON string
		want    string
	}{
		{
			name:    "negative replicas",
			rawJSON: `{"name":"test","nodepools":[{"replicas":-1}]}`,
			want:    "nodepools[0].replicas",
		},
		{
			name:    "negative root volume size",
			rawJSON: `{"name":"test","nodepools":[{"rootVolumeSize":-10}]}`,
			want:    "nodepools[0].rootVolumeSize",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gcphcp.ParseClusterSpec(json.RawMessage(tt.rawJSON))
			if err == nil {
				t.Fatal("expected invalid argument error")
			}
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
			if got := err.Error(); got == "" || !strings.Contains(got, tt.want) {
				t.Fatalf("expected error to mention %q, got %q", tt.want, got)
			}
		})
	}
}
