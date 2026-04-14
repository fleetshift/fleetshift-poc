package artifacts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ArtifactResult holds validated artifact data from a successful install.
type ArtifactResult struct {
	InfraID       string
	ClusterID     string
	HasKubeconfig bool
}

// Validate checks that all required post-install artifacts exist and are valid.
func Validate(workDir string) (*ArtifactResult, error) {
	metadataPath := filepath.Join(workDir, "metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("metadata.json not found: %w", err)
	}

	var metadata struct {
		InfraID   string `json:"infraID"`
		ClusterID string `json:"clusterID"`
	}
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("metadata.json is invalid JSON: %w", err)
	}
	if metadata.InfraID == "" {
		return nil, fmt.Errorf("metadata.json missing infraID field")
	}

	kubeconfigPath := filepath.Join(workDir, "auth", "kubeconfig")
	info, err := os.Stat(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("auth/kubeconfig not found: %w", err)
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("auth/kubeconfig is empty")
	}

	passwordPath := filepath.Join(workDir, "auth", "kubeadmin-password")
	info, err = os.Stat(passwordPath)
	if err != nil {
		return nil, fmt.Errorf("auth/kubeadmin-password not found: %w", err)
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("auth/kubeadmin-password is empty")
	}

	return &ArtifactResult{
		InfraID:       metadata.InfraID,
		ClusterID:     metadata.ClusterID,
		HasKubeconfig: true,
	}, nil
}
