package workdir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInit_CreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new-cluster")
	w, err := Init(dir)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := os.Stat(w.Path); os.IsNotExist(err) {
		t.Error("work-dir was not created")
	}
}

func TestInit_ExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	w, err := Init(dir)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if w.Path != dir {
		t.Errorf("path = %q, want %q", w.Path, dir)
	}
}

func TestLock_WritesAndReadsPID(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	if err := w.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	pid, alive, err := w.ReadPID()
	if err != nil {
		t.Fatalf("ReadPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Errorf("pid = %d, want %d", pid, os.Getpid())
	}
	if !alive {
		t.Error("pid should be alive")
	}
	w.Unlock()
	_, _, err = w.ReadPID()
	if err == nil {
		t.Error("expected error after unlock, PID file should be removed")
	}
}

func TestLock_FailsIfAlreadyLocked(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	if err := w.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	defer w.Unlock()
	w2, _ := Init(dir)
	err := w2.Lock()
	if err == nil {
		t.Error("expected error locking already-locked work-dir")
	}
}

func TestHasMetadata(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	if w.HasMetadata() {
		t.Error("should not have metadata before creation")
	}
	os.WriteFile(filepath.Join(dir, "metadata.json"), []byte(`{"infraID":"test-abc"}`), 0644)
	if !w.HasMetadata() {
		t.Error("should have metadata after creation")
	}
}

func TestHasKubeconfig(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	if w.HasKubeconfig() {
		t.Error("should not have kubeconfig before creation")
	}
	os.MkdirAll(filepath.Join(dir, "auth"), 0755)
	os.WriteFile(filepath.Join(dir, "auth", "kubeconfig"), []byte("apiVersion: v1"), 0644)
	if !w.HasKubeconfig() {
		t.Error("should have kubeconfig after creation")
	}
}

func TestInfraID(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	_, err := w.InfraID()
	if err == nil {
		t.Error("expected error when no metadata.json")
	}
	os.WriteFile(filepath.Join(dir, "metadata.json"), []byte(`{"infraID":"my-cluster-a1b2c"}`), 0644)
	id, err := w.InfraID()
	if err != nil {
		t.Fatalf("InfraID: %v", err)
	}
	if id != "my-cluster-a1b2c" {
		t.Errorf("infraID = %q, want my-cluster-a1b2c", id)
	}
}

func TestHasInstaller(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	if w.HasInstaller() {
		t.Error("should not have installer before creation")
	}
	os.WriteFile(filepath.Join(dir, "openshift-install"), []byte("binary"), 0755)
	if !w.HasInstaller() {
		t.Error("should have installer after creation")
	}
}

func TestCompletedPhases(t *testing.T) {
	dir := t.TempDir()
	w, _ := Init(dir)
	phases := w.CompletedPhases()
	if len(phases) != 0 {
		t.Errorf("expected 0 completed phases, got %d", len(phases))
	}
	w.MarkPhaseComplete("extract")
	w.MarkPhaseComplete("install-config")
	phases = w.CompletedPhases()
	if len(phases) != 2 {
		t.Errorf("expected 2 completed phases, got %d", len(phases))
	}
	if phases[0] != "extract" || phases[1] != "install-config" {
		t.Errorf("phases = %v, want [extract install-config]", phases)
	}
}

func TestCopyConfig(t *testing.T) {
	dir := t.TempDir()
	wd, err := Init(dir)
	if err != nil {
		t.Fatal(err)
	}

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "cluster.yaml")
	content := []byte("baseDomain: example.com\n")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	if err := wd.CopyConfig(srcPath); err != nil {
		t.Fatalf("CopyConfig failed: %v", err)
	}

	got, err := os.ReadFile(wd.ClusterConfigPath())
	if err != nil {
		t.Fatalf("reading copied config: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("copied content = %q, want %q", got, content)
	}
}

func TestCopyConfig_SourceMissing(t *testing.T) {
	dir := t.TempDir()
	wd, err := Init(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = wd.CopyConfig("/nonexistent/cluster.yaml")
	if err == nil {
		t.Fatal("expected error for missing source, got nil")
	}
}
