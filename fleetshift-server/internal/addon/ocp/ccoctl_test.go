package ocp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCCOctlCreateAllArgs(t *testing.T) {
	args := ccoctlCreateAllArgs(
		"test-cluster",
		"us-east-1",
		"/path/to/credrequests",
		"/path/to/output",
	)

	want := []string{
		"aws", "create-all",
		"--name", "test-cluster",
		"--region", "us-east-1",
		"--credentials-requests-dir", "/path/to/credrequests",
		"--output-dir", "/path/to/output",
	}

	if len(args) != len(want) {
		t.Fatalf("ccoctlCreateAllArgs returned %d args, want %d", len(args), len(want))
	}

	for i, wantArg := range want {
		if args[i] != wantArg {
			t.Errorf("args[%d] = %q, want %q", i, args[i], wantArg)
		}
	}
}

func TestCCOctlDeleteArgs(t *testing.T) {
	args := ccoctlDeleteArgs("test-cluster", "us-west-2")

	want := []string{
		"aws", "delete",
		"--name", "test-cluster",
		"--region", "us-west-2",
	}

	if len(args) != len(want) {
		t.Fatalf("ccoctlDeleteArgs returned %d args, want %d", len(args), len(want))
	}

	for i, wantArg := range want {
		if args[i] != wantArg {
			t.Errorf("args[%d] = %q, want %q", i, args[i], wantArg)
		}
	}
}

func TestCopyDir(t *testing.T) {
	// Create temporary source directory structure
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	dstDir := filepath.Join(tmpDir, "dst")

	// Create source directory with files and subdirectories
	if err := os.MkdirAll(filepath.Join(srcDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create source directory structure: %v", err)
	}

	// Create test files
	files := map[string]string{
		"file1.txt":        "content1",
		"file2.txt":        "content2",
		"subdir/file3.txt": "content3",
	}

	for path, content := range files {
		fullPath := filepath.Join(srcDir, path)
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", path, err)
		}
	}

	// Copy directory
	if err := copyDir(srcDir, dstDir); err != nil {
		t.Fatalf("copyDir failed: %v", err)
	}

	// Verify destination directory exists
	if _, err := os.Stat(dstDir); err != nil {
		t.Errorf("destination directory does not exist: %v", err)
	}

	// Verify all files were copied with correct content
	for path, wantContent := range files {
		dstPath := filepath.Join(dstDir, path)
		gotContent, err := os.ReadFile(dstPath)
		if err != nil {
			t.Errorf("failed to read copied file %s: %v", path, err)
			continue
		}

		if string(gotContent) != wantContent {
			t.Errorf("file %s content = %q, want %q", path, gotContent, wantContent)
		}
	}
}

func TestCopyDir_SourceNotDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "file.txt")
	dstDir := filepath.Join(tmpDir, "dst")

	// Create a file (not a directory)
	if err := os.WriteFile(srcFile, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Attempt to copy file as directory
	err := copyDir(srcFile, dstDir)
	if err == nil {
		t.Fatal("copyDir should fail when source is not a directory")
	}
}

func TestCopyDir_SourceNotExist(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "nonexistent")
	dstDir := filepath.Join(tmpDir, "dst")

	err := copyDir(srcDir, dstDir)
	if err == nil {
		t.Fatal("copyDir should fail when source does not exist")
	}
}
