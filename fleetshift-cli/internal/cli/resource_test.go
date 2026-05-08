package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/testserver"
)

func TestResource_TypesCommand(t *testing.T) {
	addr := testserver.Start(t)

	out := runCLI(t, "--server", addr, "resource", "types")

	if !strings.Contains(out, "kindclusters") {
		t.Fatalf("expected 'kindclusters' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "KindCluster") {
		t.Fatalf("expected 'KindCluster' in output, got:\n%s", out)
	}
}

func TestResource_CreateGetListDelete(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "test-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	// Create
	out := runCLI(t, "--server", addr, "resource", "create", "kindclusters",
		"--id", "test-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)
	assertJSONHasField(t, out, "name", "kindclusters/test-cluster")

	// Get
	out = runCLI(t, "--server", addr, "resource", "get", "kindclusters", "test-cluster", "--output", "json")
	assertJSONHasField(t, out, "name", "kindclusters/test-cluster")

	// List
	out = runCLI(t, "--server", addr, "resource", "list", "kindclusters", "--output", "json")
	if !strings.Contains(out, "kindclusters/test-cluster") {
		t.Fatalf("expected resource in list output, got:\n%s", out)
	}

	// Delete
	out = runCLI(t, "--server", addr, "resource", "delete", "kindclusters", "test-cluster", "--output", "json")
	if !strings.Contains(out, "kindclusters/test-cluster") {
		t.Fatalf("expected deleted resource in output, got:\n%s", out)
	}

	// Verify it's gone.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
}

func writeSpecFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write spec file: %v", err)
	}
	return path
}

func assertJSONHasField(t *testing.T, output, field, expected string) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(output), &m); err != nil {
		t.Fatalf("parse JSON output: %v\nOutput:\n%s", err, output)
	}
	got, ok := m[field]
	if !ok {
		t.Fatalf("field %q not found in JSON output: %s", field, output)
	}
	if got != expected {
		t.Fatalf("field %q = %v, want %v", field, got, expected)
	}
}
