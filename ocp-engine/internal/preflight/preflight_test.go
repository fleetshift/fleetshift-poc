package preflight

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ocp-engine/internal/config"
)

func TestCheckFiles_AllExist(t *testing.T) {
	dir := t.TempDir()
	pullSecret := filepath.Join(dir, "pull-secret.json")
	sshKey := filepath.Join(dir, "id_rsa.pub")
	os.WriteFile(pullSecret, []byte(`{"auths":{}}`), 0600)
	os.WriteFile(sshKey, []byte("ssh-rsa AAAA"), 0644)

	cfg := &config.EngineConfig{
		PullSecretFile:   pullSecret,
		SSHPublicKeyFile: sshKey,
	}

	err := CheckFiles(cfg)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestCheckFiles_PullSecretMissing(t *testing.T) {
	cfg := &config.EngineConfig{
		PullSecretFile: "/nonexistent/pull-secret.json",
	}

	err := CheckFiles(cfg)
	if err == nil {
		t.Fatal("expected error for missing pull secret")
	}
}

func TestCheckFiles_OptionalSSHKeyMissing(t *testing.T) {
	dir := t.TempDir()
	pullSecret := filepath.Join(dir, "pull-secret.json")
	os.WriteFile(pullSecret, []byte(`{"auths":{}}`), 0600)

	cfg := &config.EngineConfig{
		PullSecretFile:   pullSecret,
		SSHPublicKeyFile: "/nonexistent/key.pub",
	}

	err := CheckFiles(cfg)
	if err == nil {
		t.Fatal("expected error for missing SSH key when path is set")
	}
}

func TestCheckFiles_OptionalSSHKeyEmpty(t *testing.T) {
	dir := t.TempDir()
	pullSecret := filepath.Join(dir, "pull-secret.json")
	os.WriteFile(pullSecret, []byte(`{"auths":{}}`), 0600)

	cfg := &config.EngineConfig{
		PullSecretFile:   pullSecret,
		SSHPublicKeyFile: "",
	}

	err := CheckFiles(cfg)
	if err != nil {
		t.Fatalf("expected no error when optional SSH key is empty, got: %v", err)
	}
}

func TestCheckInstallConfig_ValidFields(t *testing.T) {
	ic := map[string]any{
		"baseDomain": "example.com",
		"metadata":   map[string]any{"name": "my-cluster"},
		"platform":   map[string]any{"aws": map[string]any{"region": "us-east-1"}},
	}
	err := CheckInstallConfig(ic)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestCheckInstallConfig_MissingBaseDomain(t *testing.T) {
	ic := map[string]any{
		"metadata": map[string]any{"name": "my-cluster"},
		"platform": map[string]any{"aws": map[string]any{"region": "us-east-1"}},
	}
	err := CheckInstallConfig(ic)
	if err == nil {
		t.Fatal("expected error for missing baseDomain")
	}
}

func TestCheckDNSCollision_NoCollision(t *testing.T) {
	warning := CheckDNSCollision("nonexistent-cluster-xyz", "invalid.example.test")
	if warning != "" {
		t.Fatalf("expected no warning for non-resolving domain, got: %s", warning)
	}
}
