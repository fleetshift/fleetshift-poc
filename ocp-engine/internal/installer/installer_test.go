package installer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildExtractArgs_WithPullSecret(t *testing.T) {
	i := &Installer{
		WorkDir:        "/tmp/test-cluster",
		ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64",
		PullSecretFile: "/tmp/pull-secret.json",
	}
	args := i.buildExtractArgs()

	found := false
	for _, arg := range args {
		if arg == "--registry-config=/tmp/pull-secret.json" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected --registry-config flag in args, got %v", args)
	}
}

func TestBuildExtractArgs_WithoutPullSecret(t *testing.T) {
	i := &Installer{
		WorkDir:      "/tmp/test-cluster",
		ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64",
	}
	args := i.buildExtractArgs()

	for _, arg := range args {
		if arg == "--registry-config=" {
			t.Error("should not include --registry-config when PullSecretFile is empty")
		}
	}
}

func TestBuildEnv(t *testing.T) {
	i := &Installer{
		WorkDir: "/tmp/test-cluster",
		AWSEnv: map[string]string{
			"AWS_ACCESS_KEY_ID":     "AKIATEST",
			"AWS_SECRET_ACCESS_KEY": "secrettest",
		},
	}
	env := i.buildEnv()
	found := map[string]bool{}
	for _, e := range env {
		if e == "AWS_ACCESS_KEY_ID=AKIATEST" {
			found["key"] = true
		}
		if e == "AWS_SECRET_ACCESS_KEY=secrettest" {
			found["secret"] = true
		}
	}
	if !found["key"] {
		t.Error("AWS_ACCESS_KEY_ID not found in env")
	}
	if !found["secret"] {
		t.Error("AWS_SECRET_ACCESS_KEY not found in env")
	}
}

func TestRunCommand_Success(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	err := RunCommand("echo", []string{"hello"}, nil, logFile)
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	data, _ := os.ReadFile(logFile)
	if string(data) == "" {
		t.Error("log file should have content")
	}
}

func TestRunCommand_Failure(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	err := RunCommand("false", nil, nil, logFile)
	if err == nil {
		t.Error("RunCommand should fail for 'false' command")
	}
}

func TestRunCommandWithContext_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	logPath := filepath.Join(t.TempDir(), "test.log")
	err := RunCommandWithContext(ctx, "sleep", []string{"10"}, nil, logPath)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "killed") && !strings.Contains(err.Error(), "signal") {
		t.Errorf("error should indicate timeout/kill, got: %v", err)
	}
}

func TestRunCommandWithContext_Success(t *testing.T) {
	ctx := context.Background()
	logPath := filepath.Join(t.TempDir(), "test.log")
	err := RunCommandWithContext(ctx, "echo", []string{"hello"}, nil, logPath)
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}
