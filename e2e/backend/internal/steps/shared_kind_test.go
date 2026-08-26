package steps

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

func TestIsSharedKindCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "pool a", id: sharedKindIDA, want: true},
		{name: "pool b", id: sharedKindIDB, want: true},
		{name: "lifecycle random", id: "kind-e2e-abcd1234", want: false},
		{name: "empty", id: "", want: false},
		{name: "prefix only", id: "kind-e2e-", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isSharedKindCluster(tt.id); got != tt.want {
				t.Fatalf("isSharedKindCluster(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestSharedKindIDsUseSuitePrefix(t *testing.T) {
	t.Parallel()
	for _, id := range []string{sharedKindIDA, sharedKindIDB} {
		if !strings.HasPrefix(id, harness.KindClusterIDPrefix) {
			t.Fatalf("%q missing prefix %q", id, harness.KindClusterIDPrefix)
		}
	}
}

func TestCleanupKindCluster_SharedSkipped(t *testing.T) {
	t.Parallel()
	CleanupKindCluster(t, nil, sharedKindIDA)
	CleanupKindCluster(t, nil, sharedKindIDB)
}

func TestDeleteKindCluster_RefusesShared(t *testing.T) {
	if os.Getenv("TEST_DELETE_SHARED_INNER") == "1" {
		DeleteKindCluster(t, nil, sharedKindIDA)
		return
	}

	t.Parallel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDeleteKindCluster_RefusesShared$", "-test.v=true")
	cmd.Env = append(os.Environ(), "TEST_DELETE_SHARED_INNER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("inner test should fail:\n%s", out)
	}
	if !strings.Contains(string(out), "refusing to delete suite shared kind cluster") {
		t.Fatalf("got:\n%s", out)
	}
}

func TestReleaseSharedKindClusters_Nil(t *testing.T) {
	t.Parallel()
	ReleaseSharedKindClusters(nil)
}

func TestKindClusterPresent_Nil(t *testing.T) {
	t.Parallel()
	if kindClusterPresent(nil, sharedKindIDA) {
		t.Fatal("nil fixture must not report present")
	}
	if kindClusterPresent(nil, "") {
		t.Fatal("empty name must not report present")
	}
}
