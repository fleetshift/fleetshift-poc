package gcphcp_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestBuildCLSClusterSpec(t *testing.T) {
	spec := gcphcp.ClusterSpec{
		Name:           "test-cluster",
		EndpointAccess: "PublicAndPrivate",
		ReleaseVersion: "4.16.1",
		ChannelGroup:   "stable",
	}

	target := gcphcp.TargetConfig{
		ID:         "target-1",
		GCPProject: "my-project-123",
		Region:     "us-central1",
	}

	infraConfig := map[string]any{
		"infraId":     "test-infra",
		"networkName": "test-network",
		"subnetName":  "test-subnet",
	}

	iamConfig := map[string]any{
		"projectNumber": "123456789",
		"workloadIdentityPool": map[string]any{
			"poolId":     "test-pool",
			"providerId": "test-provider",
		},
		"serviceAccounts": map[string]any{
			"ctrlplane-op":     "ctrlplane@example.com",
			"nodepool-mgmt":    "nodepool@example.com",
			"cloud-controller": "cloudctrl@example.com",
			"gcp-pd-csi":       "storage@example.com",
			"image-registry":   "registry@example.com",
			"cloud-network":    "network@example.com",
		},
	}

	signingKeyBase64 := "dGVzdC1rZXk="

	result, err := gcphcp.BuildCLSClusterSpec(spec, target, infraConfig, iamConfig, signingKeyBase64)
	if err != nil {
		t.Fatalf("BuildCLSClusterSpec failed: %v", err)
	}

	// Verify top-level fields
	if name := result["name"]; name != "test-cluster" {
		t.Errorf("expected name=test-cluster, got %v", name)
	}

	if targetProject := result["target_project_id"]; targetProject != "my-project-123" {
		t.Errorf("expected target_project_id=my-project-123, got %v", targetProject)
	}

	// Verify nested spec fields
	specMap, ok := result["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec field is not a map")
	}

	if infraID := specMap["infraID"]; infraID != "test-infra" {
		t.Errorf("expected spec.infraID=test-infra, got %v", infraID)
	}

	if issuerURL := specMap["issuerURL"]; issuerURL != "https://hypershift-test-infra-oidc" {
		t.Errorf("expected spec.issuerURL=https://hypershift-test-infra-oidc, got %v", issuerURL)
	}

	if signingKey := specMap["serviceAccountSigningKey"]; signingKey != signingKeyBase64 {
		t.Errorf("expected spec.serviceAccountSigningKey=%s, got %v", signingKeyBase64, signingKey)
	}

	if releaseVersion := specMap["releaseVersion"]; releaseVersion != "4.16.1" {
		t.Errorf("expected spec.releaseVersion=4.16.1, got %v", releaseVersion)
	}

	if channelGroup := specMap["channelGroup"]; channelGroup != "stable" {
		t.Errorf("expected spec.channelGroup=stable, got %v", channelGroup)
	}

	// Verify platform fields
	platformMap, ok := specMap["platform"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform is not a map")
	}

	if platformType := platformMap["type"]; platformType != "GCP" {
		t.Errorf("expected spec.platform.type=GCP, got %v", platformType)
	}

	gcpMap, ok := platformMap["gcp"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform.gcp is not a map")
	}

	if projectID := gcpMap["projectID"]; projectID != "my-project-123" {
		t.Errorf("expected spec.platform.gcp.projectID=my-project-123, got %v", projectID)
	}

	if region := gcpMap["region"]; region != "us-central1" {
		t.Errorf("expected spec.platform.gcp.region=us-central1, got %v", region)
	}

	if network := gcpMap["network"]; network != "test-network" {
		t.Errorf("expected spec.platform.gcp.network=test-network, got %v", network)
	}

	if subnet := gcpMap["subnet"]; subnet != "test-subnet" {
		t.Errorf("expected spec.platform.gcp.subnet=test-subnet, got %v", subnet)
	}

	if endpointAccess := gcpMap["endpointAccess"]; endpointAccess != "PublicAndPrivate" {
		t.Errorf("expected spec.platform.gcp.endpointAccess=PublicAndPrivate, got %v", endpointAccess)
	}

	// Verify workloadIdentity was converted properly
	wifMap, ok := gcpMap["workloadIdentity"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform.gcp.workloadIdentity is not a map")
	}

	if projectNumber := wifMap["projectNumber"]; projectNumber != "123456789" {
		t.Errorf("expected workloadIdentity.projectNumber=123456789, got %v", projectNumber)
	}

	if poolID := wifMap["poolID"]; poolID != "test-pool" {
		t.Errorf("expected workloadIdentity.poolID=test-pool, got %v", poolID)
	}

	if providerID := wifMap["providerID"]; providerID != "test-provider" {
		t.Errorf("expected workloadIdentity.providerID=test-provider, got %v", providerID)
	}
}

func TestBuildCLSClusterUpdateSpec(t *testing.T) {
	observed := map[string]any{
		"id":                "c-123",
		"name":              "test-cluster",
		"target_project_id": "existing-project",
		"spec": map[string]any{
			"infraID":                  "existing-infra",
			"issuerURL":                "https://hypershift-existing-infra-oidc",
			"serviceAccountSigningKey": "existing-signing-key",
			"releaseVersion":           "4.15.0",
			"channelGroup":             "candidate",
			"platform": map[string]any{
				"type": "GCP",
				"gcp": map[string]any{
					"projectID":      "existing-project",
					"region":         "us-central1",
					"network":        "existing-network",
					"subnet":         "existing-subnet",
					"endpointAccess": "PublicAndPrivate",
					"workloadIdentity": map[string]any{
						"projectNumber": "123456789",
					},
				},
			},
		},
	}

	updated, err := gcphcp.BuildCLSClusterUpdateSpec(gcphcp.ClusterSpec{
		Name:           "test-cluster",
		EndpointAccess: "Private",
		ReleaseVersion: "4.16.1",
		ChannelGroup:   "stable",
	}, observed)
	if err != nil {
		t.Fatalf("BuildCLSClusterUpdateSpec failed: %v", err)
	}

	if targetProject := updated["target_project_id"]; targetProject != "existing-project" {
		t.Errorf("expected target_project_id=existing-project, got %v", targetProject)
	}

	specMap, ok := updated["spec"].(map[string]any)
	if !ok {
		t.Fatal("updated spec is not a map")
	}

	if infraID := specMap["infraID"]; infraID != "existing-infra" {
		t.Errorf("expected spec.infraID=existing-infra, got %v", infraID)
	}

	if issuerURL := specMap["issuerURL"]; issuerURL != "https://hypershift-existing-infra-oidc" {
		t.Errorf("expected existing issuerURL to be preserved, got %v", issuerURL)
	}

	if signingKey := specMap["serviceAccountSigningKey"]; signingKey != "existing-signing-key" {
		t.Errorf("expected existing signing key to be preserved, got %v", signingKey)
	}

	if releaseVersion := specMap["releaseVersion"]; releaseVersion != "4.16.1" {
		t.Errorf("expected spec.releaseVersion=4.16.1, got %v", releaseVersion)
	}

	if channelGroup := specMap["channelGroup"]; channelGroup != "stable" {
		t.Errorf("expected spec.channelGroup=stable, got %v", channelGroup)
	}

	platformMap, ok := specMap["platform"].(map[string]any)
	if !ok {
		t.Fatal("updated platform is not a map")
	}
	gcpMap, ok := platformMap["gcp"].(map[string]any)
	if !ok {
		t.Fatal("updated platform.gcp is not a map")
	}

	if endpointAccess := gcpMap["endpointAccess"]; endpointAccess != "Private" {
		t.Errorf("expected spec.platform.gcp.endpointAccess=Private, got %v", endpointAccess)
	}
	if network := gcpMap["network"]; network != "existing-network" {
		t.Errorf("expected spec.platform.gcp.network=existing-network, got %v", network)
	}
	if subnet := gcpMap["subnet"]; subnet != "existing-subnet" {
		t.Errorf("expected spec.platform.gcp.subnet=existing-subnet, got %v", subnet)
	}
}

func TestBuildCLSNodepoolSpec(t *testing.T) {
	np := gcphcp.NodepoolSpec{
		ID:             "workers",
		Replicas:       3,
		InstanceType:   "n1-standard-8",
		RootVolumeSize: 256,
		RootVolumeType: "pd-ssd",
		AutoRepair:     boolPtr(true),
		UpgradeType:    "Replace",
	}

	clusterID := "cluster-abc-123"

	result := gcphcp.BuildCLSNodepoolSpec(np, "test-cluster", clusterID)

	// Verify top-level fields
	if name := result["name"]; name != "test-cluster-workers" {
		t.Errorf("expected name=test-cluster-workers, got %v", name)
	}

	if cID := result["cluster_id"]; cID != clusterID {
		t.Errorf("expected cluster_id=%s, got %v", clusterID, cID)
	}

	// Verify nested spec fields
	specMap, ok := result["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec field is not a map")
	}

	if replicas := specMap["replicas"]; replicas != 3 {
		t.Errorf("expected spec.replicas=3, got %v", replicas)
	}

	// Verify platform fields
	platformMap, ok := specMap["platform"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform is not a map")
	}

	if platformType := platformMap["type"]; platformType != "GCP" {
		t.Errorf("expected spec.platform.type=GCP, got %v", platformType)
	}

	gcpMap, ok := platformMap["gcp"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform.gcp is not a map")
	}

	if instanceType := gcpMap["instanceType"]; instanceType != "n1-standard-8" {
		t.Errorf("expected spec.platform.gcp.instanceType=n1-standard-8, got %v", instanceType)
	}

	rootVolumeMap, ok := gcpMap["rootVolume"].(map[string]any)
	if !ok {
		t.Fatalf("spec.platform.gcp.rootVolume is not a map")
	}

	if size := rootVolumeMap["size"]; size != 256 {
		t.Errorf("expected rootVolume.size=256, got %v", size)
	}

	if volumeType := rootVolumeMap["type"]; volumeType != "pd-ssd" {
		t.Errorf("expected rootVolume.type=pd-ssd, got %v", volumeType)
	}

	// Verify management fields
	managementMap, ok := specMap["management"].(map[string]any)
	if !ok {
		t.Fatalf("spec.management is not a map")
	}

	if autoRepair := managementMap["autoRepair"]; autoRepair != true {
		t.Errorf("expected management.autoRepair=true, got %v", autoRepair)
	}

	if upgradeType := managementMap["upgradeType"]; upgradeType != "Replace" {
		t.Errorf("expected management.upgradeType=Replace, got %v", upgradeType)
	}
}
