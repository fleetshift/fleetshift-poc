package cmd

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func projectRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..")
}

func TestGenConfig_EndToEnd(t *testing.T) {
	binPath := buildBinary(t)

	// Create test config with real pull secret file
	tmpDir := t.TempDir()
	psPath := filepath.Join(tmpDir, "pull-secret.json")
	os.WriteFile(psPath, []byte(`{"auths":{}}`), 0644)

	// Config goes directly in the cluster directory (which is the work dir)
	configPath := filepath.Join(tmpDir, "cluster.yaml")
	configYAML := `
ocp_engine:
  pull_secret_file: ` + psPath + `
  credentials:
    access_key_id: "AKIATEST"
    secret_access_key: "secrettest"
baseDomain: test.example.com
metadata:
  name: smoke-test
platform:
  aws:
    region: us-east-1
`
	os.WriteFile(configPath, []byte(configYAML), 0644)

	// Run gen-config -- work dir is derived from config's parent directory
	cmd := exec.Command(binPath, "gen-config", "--config", configPath)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("gen-config failed: %v\noutput: %s", err, out)
	}

	// Verify JSON output
	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, out)
	}
	if result["status"] != "complete" {
		t.Errorf("status = %v, want complete", result["status"])
	}

	// Verify install-config.yaml was created in the same directory
	icData, err := os.ReadFile(filepath.Join(tmpDir, "install-config.yaml"))
	if err != nil {
		t.Fatalf("install-config.yaml not created: %v", err)
	}
	if len(icData) == 0 {
		t.Error("install-config.yaml is empty")
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "ocp-engine")
	build := exec.Command("go", "build", "-o", binPath)
	build.Dir = projectRoot()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	return binPath
}

func TestStatus_EmptyWorkDir(t *testing.T) {
	binPath := buildBinary(t)

	workDir := t.TempDir()
	cmd := exec.Command(binPath, "status", "--work-dir", workDir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("status failed: %v\noutput: %s", err, out)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["state"] != "empty" {
		t.Errorf("state = %v, want empty", result["state"])
	}
}

func TestStatus_NonexistentWorkDir(t *testing.T) {
	binPath := buildBinary(t)

	cmd := exec.Command(binPath, "status", "--work-dir", "/tmp/nonexistent-ocp-test-dir")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("status should exit 0 for nonexistent dir: %v\noutput: %s", err, out)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["state"] != "empty" {
		t.Errorf("state = %v, want empty", result["state"])
	}
}

func TestStatus_Succeeded(t *testing.T) {
	binPath := buildBinary(t)

	workDir := t.TempDir()
	// Create all phase markers
	for _, phase := range []string{"extract", "install-config", "manifests", "ignition", "cluster"} {
		os.WriteFile(filepath.Join(workDir, "_phase_"+phase+"_complete"), []byte(""), 0644)
	}
	// Create kubeconfig
	os.MkdirAll(filepath.Join(workDir, "auth"), 0755)
	os.WriteFile(filepath.Join(workDir, "auth", "kubeconfig"), []byte("apiVersion: v1"), 0644)

	cmd := exec.Command(binPath, "status", "--work-dir", workDir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("status failed: %v\noutput: %s", err, out)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["state"] != "succeeded" {
		t.Errorf("state = %v, want succeeded", result["state"])
	}
	if result["has_kubeconfig"] != true {
		t.Error("has_kubeconfig should be true")
	}
}
