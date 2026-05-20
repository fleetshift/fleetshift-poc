package gcphcp_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func boolPtr(b bool) *bool { return &b }

func fullSpecJSON() string {
	return `{
		"endpointAccess": "PublicAndPrivate",
		"releaseVersion": "4.18.0",
		"channelGroup": "stable",
		"nodepools": [{
			"id": "workers",
			"replicas": 2,
			"instanceType": "n1-standard-4",
			"rootVolumeSize": 128,
			"rootVolumeType": "pd-standard",
			"autoRepair": true,
			"upgradeType": "Replace"
		}]
	}`
}

func TestParseClusterSpec_ValidFullSpec(t *testing.T) {
	raw := json.RawMessage(fullSpecJSON())

	spec, err := gcphcp.ParseClusterSpec(raw)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.Name != "" {
		t.Errorf("expected Name to be empty (json:\"-\"), got %s", spec.Name)
	}
	if spec.EndpointAccess != "PublicAndPrivate" {
		t.Errorf("expected EndpointAccess=PublicAndPrivate, got %s", spec.EndpointAccess)
	}
	if spec.ReleaseVersion != "4.18.0" {
		t.Errorf("expected ReleaseVersion=4.18.0, got %s", spec.ReleaseVersion)
	}
	if spec.ChannelGroup != "stable" {
		t.Errorf("expected ChannelGroup=stable, got %s", spec.ChannelGroup)
	}

	if len(spec.Nodepools) != 1 {
		t.Fatalf("expected 1 nodepool, got %d", len(spec.Nodepools))
	}

	np := spec.Nodepools[0]
	if np.ID != "workers" {
		t.Errorf("expected nodepool ID=workers, got %s", np.ID)
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
	if np.AutoRepair == nil || *np.AutoRepair != true {
		t.Errorf("expected nodepool AutoRepair=true, got %v", np.AutoRepair)
	}
	if np.UpgradeType != "Replace" {
		t.Errorf("expected nodepool UpgradeType=Replace, got %s", np.UpgradeType)
	}
}

func TestParseClusterSpec_AllFieldsExplicit(t *testing.T) {
	raw := json.RawMessage(`{
		"endpointAccess": "Private",
		"releaseVersion": "4.14.0",
		"channelGroup": "fast",
		"nodepools": [
			{
				"id": "infra",
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

	if spec.Name != "" {
		t.Errorf("expected Name to be empty (json:\"-\"), got %s", spec.Name)
	}
	if spec.EndpointAccess != "Private" {
		t.Errorf("expected EndpointAccess=Private, got %s", spec.EndpointAccess)
	}
	if spec.ReleaseVersion != "4.14.0" {
		t.Errorf("expected ReleaseVersion=4.14.0, got %s", spec.ReleaseVersion)
	}
	if spec.ChannelGroup != "fast" {
		t.Errorf("expected ChannelGroup=fast, got %s", spec.ChannelGroup)
	}

	np := spec.Nodepools[0]
	if np.ID != "infra" {
		t.Errorf("expected nodepool ID=infra, got %s", np.ID)
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
	if np.AutoRepair == nil || *np.AutoRepair != false {
		t.Errorf("expected nodepool AutoRepair=false, got %v", np.AutoRepair)
	}
	if np.UpgradeType != "InPlace" {
		t.Errorf("expected nodepool UpgradeType=InPlace, got %s", np.UpgradeType)
	}
}

func TestValidateClusterName(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		err := gcphcp.ValidateClusterName("")
		if err == nil {
			t.Fatal("expected error for empty name, got nil")
		}
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	t.Run("too long", func(t *testing.T) {
		err := gcphcp.ValidateClusterName("this-name-is-way-too-long")
		if err == nil {
			t.Fatal("expected error for name too long, got nil")
		}
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got %v", err)
		}
	})

	invalidNames := []struct {
		name    string
		input   string
	}{
		{"uppercase", "TestCluster"},
		{"starts with number", "1cluster"},
		{"has underscore", "test_cluster"},
		{"has dots", "test.cluster"},
	}
	for _, tt := range invalidNames {
		t.Run("invalid/"+tt.name, func(t *testing.T) {
			err := gcphcp.ValidateClusterName(tt.input)
			if err == nil {
				t.Fatalf("expected error for invalid name %q, got nil", tt.input)
			}
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("expected ErrInvalidArgument, got %v", err)
			}
		})
	}

	validNames := []string{
		"a",
		"z",
		"test",
		"test-cluster",
		"test-123",
		"a-b-c-d-e-f-g",
		"test-cluster-99",
	}
	for _, name := range validNames {
		t.Run("valid/"+name, func(t *testing.T) {
			if err := gcphcp.ValidateClusterName(name); err != nil {
				t.Fatalf("ValidateClusterName(%q) unexpected error: %v", name, err)
			}
		})
	}
}

func TestParseClusterSpec_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		rawJSON string
		want    string
	}{
		{
			name:    "missing endpointAccess",
			rawJSON: `{"releaseVersion":"4.18.0","channelGroup":"stable","nodepools":[{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}]}`,
			want:    "endpointAccess is required",
		},
		{
			name:    "missing releaseVersion",
			rawJSON: `{"endpointAccess":"Public","channelGroup":"stable","nodepools":[{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}]}`,
			want:    "releaseVersion is required",
		},
		{
			name:    "missing channelGroup",
			rawJSON: `{"endpointAccess":"Public","releaseVersion":"4.18.0","nodepools":[{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}]}`,
			want:    "channelGroup is required",
		},
		{
			name:    "missing nodepools",
			rawJSON: `{"endpointAccess":"Public","releaseVersion":"4.18.0","channelGroup":"stable"}`,
			want:    "at least one nodepool is required",
		},
		{
			name:    "empty nodepools",
			rawJSON: `{"endpointAccess":"Public","releaseVersion":"4.18.0","channelGroup":"stable","nodepools":[]}`,
			want:    "at least one nodepool is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gcphcp.ParseClusterSpec(json.RawMessage(tt.rawJSON))
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error to contain %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestParseClusterSpec_MissingNodepoolFields(t *testing.T) {
	base := `{"endpointAccess":"Public","releaseVersion":"4.18.0","channelGroup":"stable","nodepools":[`

	tests := []struct {
		name     string
		nodepool string
		want     string
	}{
		{
			name:     "missing id",
			nodepool: `{"replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].id is required",
		},
		{
			name:     "id too long",
			nodepool: `{"id":"abcdefghijk","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].id must be 10 characters or less",
		},
		{
			name:     "id invalid pattern uppercase",
			nodepool: `{"id":"Workers","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].id must match pattern",
		},
		{
			name:     "id invalid pattern starts with number",
			nodepool: `{"id":"1pool","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].id must match pattern",
		},
		{
			name:     "id invalid pattern underscore",
			nodepool: `{"id":"my_pool","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].id must match pattern",
		},
		{
			name:     "zero replicas",
			nodepool: `{"id":"w","replicas":0,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].replicas must be > 0",
		},
		{
			name:     "negative replicas",
			nodepool: `{"id":"w","replicas":-1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].replicas must be > 0",
		},
		{
			name:     "missing instanceType",
			nodepool: `{"id":"w","replicas":1,"rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].instanceType is required",
		},
		{
			name:     "zero rootVolumeSize",
			nodepool: `{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":0,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].rootVolumeSize must be > 0",
		},
		{
			name:     "negative rootVolumeSize",
			nodepool: `{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":-10,"rootVolumeType":"t","autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].rootVolumeSize must be > 0",
		},
		{
			name:     "missing rootVolumeType",
			nodepool: `{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"autoRepair":true,"upgradeType":"Replace"}`,
			want:     "nodepools[0].rootVolumeType is required",
		},
		{
			name:     "missing autoRepair",
			nodepool: `{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","upgradeType":"Replace"}`,
			want:     "nodepools[0].autoRepair is required",
		},
		{
			name:     "missing upgradeType",
			nodepool: `{"id":"w","replicas":1,"instanceType":"t","rootVolumeSize":1,"rootVolumeType":"t","autoRepair":true}`,
			want:     "nodepools[0].upgradeType is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawJSON := base + tt.nodepool + `]}`
			_, err := gcphcp.ParseClusterSpec(json.RawMessage(rawJSON))
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error to contain %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestParseClusterSpec_MultipleNodepools(t *testing.T) {
	raw := json.RawMessage(`{
		"endpointAccess": "Public",
		"releaseVersion": "4.18.0",
		"channelGroup": "stable",
		"nodepools": [
			{"id":"workers","replicas":3,"instanceType":"n1-standard-8","rootVolumeSize":256,"rootVolumeType":"pd-ssd","autoRepair":true,"upgradeType":"Replace"},
			{"id":"infra","replicas":1,"instanceType":"n1-standard-4","rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":false,"upgradeType":"InPlace"}
		]
	}`)

	spec, err := gcphcp.ParseClusterSpec(raw)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if len(spec.Nodepools) != 2 {
		t.Fatalf("expected 2 nodepools, got %d", len(spec.Nodepools))
	}
	if spec.Nodepools[0].ID != "workers" {
		t.Errorf("expected first nodepool ID=workers, got %s", spec.Nodepools[0].ID)
	}
	if spec.Nodepools[1].ID != "infra" {
		t.Errorf("expected second nodepool ID=infra, got %s", spec.Nodepools[1].ID)
	}
}

func TestNodepoolName(t *testing.T) {
	tests := []struct {
		cluster string
		poolID  string
		want    string
	}{
		{"mycluster", "workers", "mycluster-workers"},
		{"prod", "infra", "prod-infra"},
		{"a", "np1", "a-np1"},
	}

	for _, tt := range tests {
		t.Run(tt.cluster+"-"+tt.poolID, func(t *testing.T) {
			got := gcphcp.NodepoolName(tt.cluster, tt.poolID)
			if got != tt.want {
				t.Errorf("NodepoolName(%q, %q) = %q, want %q", tt.cluster, tt.poolID, got, tt.want)
			}
		})
	}
}

func TestParseClusterSpec_DuplicateNodepoolID(t *testing.T) {
	raw := json.RawMessage(`{
		"endpointAccess": "Public",
		"releaseVersion": "4.18.0",
		"channelGroup": "stable",
		"nodepools": [
			{"id":"workers","replicas":3,"instanceType":"n1-standard-8","rootVolumeSize":256,"rootVolumeType":"pd-ssd","autoRepair":true,"upgradeType":"Replace"},
			{"id":"workers","replicas":1,"instanceType":"n1-standard-4","rootVolumeSize":128,"rootVolumeType":"pd-standard","autoRepair":false,"upgradeType":"InPlace"}
		]
	}`)

	_, err := gcphcp.ParseClusterSpec(raw)
	if err == nil {
		t.Fatal("expected error for duplicate nodepool id, got nil")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected error to wrap ErrInvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), "duplicate nodepool id") {
		t.Errorf("expected error to mention duplicate nodepool id, got %q", err.Error())
	}
}
