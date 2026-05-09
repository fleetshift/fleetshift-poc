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

	if !strings.Contains(out, "KindClusters") {
		t.Fatalf("expected 'KindClusters' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "KindCluster") {
		t.Fatalf("expected 'KindCluster' in output, got:\n%s", out)
	}
}

func TestResource_DescribeCommand(t *testing.T) {
	addr := testserver.Start(t)

	out := runCLI(t, "--server", addr, "resource", "describe", "kindclusters")

	if !strings.Contains(out, "Service:  fleetshift.v1.KindClusterService") {
		t.Fatalf("expected service name in output, got:\n%s", out)
	}
	if !strings.Contains(out, "Spec (addons.kind.v1.KindClusterSpec):") {
		t.Fatalf("expected spec message header in output, got:\n%s", out)
	}
	if !strings.Contains(out, "string name = 1") {
		t.Fatalf("expected 'name' field in spec output, got:\n%s", out)
	}
	if !strings.Contains(out, "CreateKindCluster") {
		t.Fatalf("expected 'CreateKindCluster' method in output, got:\n%s", out)
	}
	// Verify nested messages are shown.
	if !strings.Contains(out, "Networking networking") {
		t.Fatalf("expected nested 'networking' field in output, got:\n%s", out)
	}
	if !strings.Contains(out, "api_server_port") {
		t.Fatalf("expected nested 'api_server_port' field in output, got:\n%s", out)
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
	assertJSONHasField(t, out, "name", "kindClusters/test-cluster")
	assertJSONHasField(t, out, "state", "CREATING")

	// Get
	out = runCLI(t, "--server", addr, "resource", "get", "kindclusters", "test-cluster", "--output", "json")
	assertJSONHasField(t, out, "name", "kindClusters/test-cluster")
	assertJSONHasField(t, out, "state", "CREATING")

	// List
	out = runCLI(t, "--server", addr, "resource", "list", "kindclusters", "--output", "json")
	if !strings.Contains(out, "kindClusters/test-cluster") {
		t.Fatalf("expected resource in list output, got:\n%s", out)
	}

	// Delete
	out = runCLI(t, "--server", addr, "resource", "delete", "kindclusters", "test-cluster", "--output", "json")
	if !strings.Contains(out, "kindClusters/test-cluster") {
		t.Fatalf("expected deleted resource in output, got:\n%s", out)
	}

	// Verify it's gone.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
}

func TestResource_GetTableOutput(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "tbl-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	// Create a resource first.
	runCLI(t, "--server", addr, "resource", "create", "kindclusters",
		"--id", "tbl-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)

	// Get with default (table) output.
	out := runCLI(t, "--server", addr, "resource", "get", "kindclusters", "tbl-cluster")

	if !strings.Contains(out, "NAME") {
		t.Fatalf("expected NAME header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "STATE") {
		t.Fatalf("expected STATE header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "kindClusters/tbl-cluster") {
		t.Fatalf("expected resource name in table output, got:\n%s", out)
	}
}

func TestResource_ListTableOutput(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "tbl-list-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	runCLI(t, "--server", addr, "resource", "create", "kindclusters",
		"--id", "tbl-list-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)

	// List with default (table) output.
	out := runCLI(t, "--server", addr, "resource", "list", "kindclusters")

	if !strings.Contains(out, "NAME") {
		t.Fatalf("expected NAME header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "STATE") {
		t.Fatalf("expected STATE header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "kindClusters/tbl-list-cluster") {
		t.Fatalf("expected resource name in list output, got:\n%s", out)
	}
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
