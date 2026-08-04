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

	// The TYPE column shows qualified names: {protoPackage}/{collectionID}.
	if !strings.Contains(out, "kind.fleetshift.v1/clusters") {
		t.Fatalf("expected 'kind.fleetshift.v1/clusters' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "gcphcp.fleetshift.v1/clusters") {
		t.Fatalf("expected 'gcphcp.fleetshift.v1/clusters' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "kind.fleetshift.v1.ClusterService") {
		t.Fatalf("expected 'kind.fleetshift.v1.ClusterService' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "gcphcp.fleetshift.v1.ClusterService") {
		t.Fatalf("expected 'gcphcp.fleetshift.v1.ClusterService' in output, got:\n%s", out)
	}
}

func TestResource_DescribeCommand(t *testing.T) {
	addr := testserver.Start(t)

	out := runCLI(t, "--server", addr, "resource", "--service", "kind.fleetshift.v1.ClusterService", "describe", "clusters")

	if !strings.Contains(out, "Service:  kind.fleetshift.v1.ClusterService") {
		t.Fatalf("expected service name in output, got:\n%s", out)
	}
	if !strings.Contains(out, "CreateCluster") {
		t.Fatalf("expected 'CreateCluster' method in output, got:\n%s", out)
	}
}

func TestResource_CreateGetListDelete(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "test-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	svcFlag := "--service"
	svcName := "kind.fleetshift.v1.ClusterService"

	// Create
	out := runCLI(t, "--server", addr, "resource", svcFlag, svcName, "create", "clusters",
		"--id", "test-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)
	assertJSONHasField(t, out, "name", "clusters/test-cluster")
	assertJSONHasField(t, out, "state", "CREATING")

	// Get
	out = runCLI(t, "--server", addr, "resource", svcFlag, svcName, "get", "clusters", "test-cluster", "--output", "json")
	assertJSONHasField(t, out, "name", "clusters/test-cluster")
	assertJSONHasField(t, out, "state", "CREATING")

	// List
	out = runCLI(t, "--server", addr, "resource", svcFlag, svcName, "list", "clusters", "--output", "json")
	if !strings.Contains(out, "clusters/test-cluster") {
		t.Fatalf("expected resource in list output, got:\n%s", out)
	}

	// Delete
	out = runCLI(t, "--server", addr, "resource", svcFlag, svcName, "delete", "clusters", "test-cluster", "--output", "json")
	if !strings.Contains(out, "clusters/test-cluster") {
		t.Fatalf("expected deleted resource in output, got:\n%s", out)
	}

	// Verify it's gone.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
}

func TestResource_QualifiedTypeCRUD(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "qualified-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	// Use the qualified type form directly — no --service needed.
	qualifiedType := "kind.fleetshift.v1/clusters"

	// Create
	out := runCLI(t, "--server", addr, "resource", "create", qualifiedType,
		"--id", "qualified-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)
	assertJSONHasField(t, out, "name", "clusters/qualified-cluster")
	assertJSONHasField(t, out, "state", "CREATING")

	// Get
	out = runCLI(t, "--server", addr, "resource", "get", qualifiedType, "qualified-cluster", "--output", "json")
	assertJSONHasField(t, out, "name", "clusters/qualified-cluster")

	// List
	out = runCLI(t, "--server", addr, "resource", "list", qualifiedType, "--output", "json")
	if !strings.Contains(out, "clusters/qualified-cluster") {
		t.Fatalf("expected resource in list output, got:\n%s", out)
	}

	// Delete
	out = runCLI(t, "--server", addr, "resource", "delete", qualifiedType, "qualified-cluster", "--output", "json")
	if !strings.Contains(out, "clusters/qualified-cluster") {
		t.Fatalf("expected deleted resource in output, got:\n%s", out)
	}
}

func TestResource_GetTableOutput(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "tbl-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	svcFlag := "--service"
	svcName := "kind.fleetshift.v1.ClusterService"

	// Create a resource first.
	runCLI(t, "--server", addr, "resource", svcFlag, svcName, "create", "clusters",
		"--id", "tbl-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)

	// Get with default (table) output.
	out := runCLI(t, "--server", addr, "resource", svcFlag, svcName, "get", "clusters", "tbl-cluster")

	if !strings.Contains(out, "NAME") {
		t.Fatalf("expected NAME header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "STATE") {
		t.Fatalf("expected STATE header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "clusters/tbl-cluster") {
		t.Fatalf("expected resource name in table output, got:\n%s", out)
	}
}

func TestResource_ListTableOutput(t *testing.T) {
	addr := testserver.Start(t)

	specJSON := `{"name": "tbl-list-cluster"}`
	specFile := writeSpecFile(t, specJSON)

	svcFlag := "--service"
	svcName := "kind.fleetshift.v1.ClusterService"

	runCLI(t, "--server", addr, "resource", svcFlag, svcName, "create", "clusters",
		"--id", "tbl-list-cluster",
		"--spec-file", specFile,
		"--output", "json",
	)

	// List with default (table) output.
	out := runCLI(t, "--server", addr, "resource", svcFlag, svcName, "list", "clusters")

	if !strings.Contains(out, "NAME") {
		t.Fatalf("expected NAME header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "STATE") {
		t.Fatalf("expected STATE header in table output, got:\n%s", out)
	}
	if !strings.Contains(out, "clusters/tbl-list-cluster") {
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
