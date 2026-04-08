package installer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildExtractArgs(t *testing.T) {
	i := &Installer{
		WorkDir:      "/tmp/test-cluster",
		ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64",
	}
	args := i.buildExtractArgs()
	expected := []string{
		"adm", "release", "extract",
		"--command=openshift-install",
		"--to=/tmp/test-cluster",
		"quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64",
	}
	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d", len(args), len(expected))
	}
	for i, arg := range args {
		if arg != expected[i] {
			t.Errorf("arg[%d] = %q, want %q", i, arg, expected[i])
		}
	}
}

func TestBuildInstallerArgs(t *testing.T) {
	i := &Installer{
		WorkDir:       "/tmp/test-cluster",
		InstallerPath: "/tmp/test-cluster/openshift-install",
	}
	args := i.buildInstallerArgs("create", "manifests")
	expected := []string{"create", "manifests", "--dir=/tmp/test-cluster"}
	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d", len(args), len(expected))
	}
	for idx, arg := range args {
		if arg != expected[idx] {
			t.Errorf("arg[%d] = %q, want %q", idx, arg, expected[idx])
		}
	}
}

func TestBuildDestroyArgs(t *testing.T) {
	i := &Installer{
		WorkDir:       "/tmp/test-cluster",
		InstallerPath: "/tmp/test-cluster/openshift-install",
	}
	args := i.buildInstallerArgs("destroy", "cluster")
	expected := []string{"destroy", "cluster", "--dir=/tmp/test-cluster"}
	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d", len(args), len(expected))
	}
	for idx, arg := range args {
		if arg != expected[idx] {
			t.Errorf("arg[%d] = %q, want %q", idx, arg, expected[idx])
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
