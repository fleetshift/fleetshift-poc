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
	// Build the binary
	binPath := filepath.Join(t.TempDir(), "ocp-engine")
	build := exec.Command("go", "build", "-o", binPath)
	build.Dir = projectRoot()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	// Create test config with real pull secret file
	tmpDir := t.TempDir()
	psPath := filepath.Join(tmpDir, "pull-secret.json")
	os.WriteFile(psPath, []byte(`{"auths":{}}`), 0644)

	configPath := filepath.Join(tmpDir, "cluster.yaml")
	configYAML := `
cluster:
  name: smoke-test
  base_domain: test.example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "AKIATEST"
      secret_access_key: "secrettest"
pull_secret_file: ` + psPath + `
`
	os.WriteFile(configPath, []byte(configYAML), 0644)
	workDir := filepath.Join(tmpDir, "work")

	// Run gen-config
	cmd := exec.Command(binPath, "gen-config", "--config", configPath, "--work-dir", workDir)
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

	// Verify install-config.yaml was created
	icData, err := os.ReadFile(filepath.Join(workDir, "install-config.yaml"))
	if err != nil {
		t.Fatalf("install-config.yaml not created: %v", err)
	}
	if len(icData) == 0 {
		t.Error("install-config.yaml is empty")
	}

	// Verify cluster.yaml was copied
	if _, err := os.Stat(filepath.Join(workDir, "cluster.yaml")); err != nil {
		t.Error("cluster.yaml not copied to work-dir")
	}
}

func TestStatus_EmptyWorkDir(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "ocp-engine")
	build := exec.Command("go", "build", "-o", binPath)
	build.Dir = projectRoot()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

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
