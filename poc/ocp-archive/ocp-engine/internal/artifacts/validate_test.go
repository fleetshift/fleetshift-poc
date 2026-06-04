package artifacts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestValidate_AllPresent(t *testing.T) {
	dir := t.TempDir()

	metadata := map[string]any{"infraID": "test-abc", "clusterID": "uuid-123"}
	metadataBytes, _ := json.Marshal(metadata)
	os.WriteFile(filepath.Join(dir, "metadata.json"), metadataBytes, 0644)
	os.MkdirAll(filepath.Join(dir, "auth"), 0755)
	os.WriteFile(filepath.Join(dir, "auth", "kubeconfig"), []byte("apiVersion: v1"), 0600)
	os.WriteFile(filepath.Join(dir, "auth", "kubeadmin-password"), []byte("secret"), 0600)

	result, err := Validate(dir)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result.InfraID != "test-abc" {
		t.Errorf("InfraID = %q, want %q", result.InfraID, "test-abc")
	}
	if result.ClusterID != "uuid-123" {
		t.Errorf("ClusterID = %q, want %q", result.ClusterID, "uuid-123")
	}
	if !result.HasKubeconfig {
		t.Error("HasKubeconfig = false, want true")
	}
}

func TestValidate_MissingMetadata(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "auth"), 0755)
	os.WriteFile(filepath.Join(dir, "auth", "kubeconfig"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(dir, "auth", "kubeadmin-password"), []byte("pw"), 0600)

	_, err := Validate(dir)
	if err == nil {
		t.Fatal("expected error for missing metadata.json")
	}
}

func TestValidate_MissingKubeconfig(t *testing.T) {
	dir := t.TempDir()
	metadata := map[string]any{"infraID": "test-abc", "clusterID": "uuid-123"}
	metadataBytes, _ := json.Marshal(metadata)
	os.WriteFile(filepath.Join(dir, "metadata.json"), metadataBytes, 0644)
	os.MkdirAll(filepath.Join(dir, "auth"), 0755)
	os.WriteFile(filepath.Join(dir, "auth", "kubeadmin-password"), []byte("pw"), 0600)

	_, err := Validate(dir)
	if err == nil {
		t.Fatal("expected error for missing kubeconfig")
	}
}

func TestValidate_InvalidMetadataJSON(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("not json"), 0644)
	os.MkdirAll(filepath.Join(dir, "auth"), 0755)
	os.WriteFile(filepath.Join(dir, "auth", "kubeconfig"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(dir, "auth", "kubeadmin-password"), []byte("pw"), 0600)

	_, err := Validate(dir)
	if err == nil {
		t.Fatal("expected error for invalid JSON in metadata.json")
	}
}
