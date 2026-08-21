package steps

import (
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

func TestUniqueKindClusterID(t *testing.T) {
	t.Parallel()
	got := UniqueKindClusterID(t)
	if !strings.HasPrefix(got, harness.KindClusterIDPrefix) {
		t.Fatalf("UniqueKindClusterID() = %q, want prefix %q", got, harness.KindClusterIDPrefix)
	}
	suffix := strings.TrimPrefix(got, harness.KindClusterIDPrefix)
	if len(suffix) != 8 {
		t.Fatalf("UniqueKindClusterID() suffix %q, want 8 hex chars", suffix)
	}
}

func TestConfigMapDeploymentID(t *testing.T) {
	t.Parallel()
	if got := ConfigMapDeploymentID("kind-e2e-abcd"); got != "cm-kind-e2e-abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestKubernetesTargetID(t *testing.T) {
	t.Parallel()
	if got := kubernetesTargetID("kind-e2e-abcd"); got != "k8s-kind-e2e-abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestJSONClusterName(t *testing.T) {
	t.Parallel()
	if got := jsonClusterName("kind-e2e-abcd"); got != "clusters/kind-e2e-abcd" {
		t.Fatalf("got %q", got)
	}
	if got := jsonClusterName("clusters/kind-e2e-abcd"); got != "clusters/kind-e2e-abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestParseCluster(t *testing.T) {
	t.Parallel()
	const in = `{"name":"clusters/kind-e2e-abcd","state":"ACTIVE","pauseReason":"token expired","conditions":{"Ready":{"status":"True"}}}`
	cl, err := parseCluster(in)
	if err != nil {
		t.Fatal(err)
	}
	if cl.Name != "clusters/kind-e2e-abcd" {
		t.Fatalf("Name = %q, want clusters/kind-e2e-abcd", cl.Name)
	}
	if cl.State != "ACTIVE" {
		t.Fatalf("State = %q, want ACTIVE", cl.State)
	}
	if cl.PauseReason != "token expired" {
		t.Fatalf("PauseReason = %q, want token expired", cl.PauseReason)
	}
	if got := cl.Conditions["Ready"].Status; got != "True" {
		t.Fatalf("conditions Ready status = %q, want True", got)
	}
	if clusterReady(cl) {
		t.Fatal("paused cluster with Ready=True must not be ready")
	}
}

func TestParseCluster_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseCluster("{"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseClusterList(t *testing.T) {
	t.Parallel()
	const in = `[{"name":"clusters/a","state":"CREATING","pauseReason":"waiting","conditions":{"Ready":{"status":"False"}}}]`
	clusters, err := parseClusterList(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(clusters) != 1 {
		t.Fatalf("len = %d, want 1", len(clusters))
	}
	cl := clusters[0]
	if cl.Name != "clusters/a" {
		t.Fatalf("Name = %q, want clusters/a", cl.Name)
	}
	if cl.State != "CREATING" {
		t.Fatalf("State = %q, want CREATING", cl.State)
	}
	if cl.PauseReason != "waiting" {
		t.Fatalf("PauseReason = %q, want waiting", cl.PauseReason)
	}
	if got := cl.Conditions["Ready"].Status; got != "False" {
		t.Fatalf("conditions Ready status = %q, want False", got)
	}
}

func TestParseClusterList_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseClusterList("{"); err == nil {
		t.Fatal("expected error")
	}
}

func TestClusterReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cl   clusterView
		want bool
	}{
		{name: "active", cl: clusterView{State: "ACTIVE"}, want: true},
		{name: "creating", cl: clusterView{State: "CREATING"}, want: false},
		{name: "running is ui-only", cl: clusterView{State: "RUNNING"}, want: false},
		{name: "state prefix is deployments json", cl: clusterView{State: "STATE_ACTIVE"}, want: false},
		{name: "paused active", cl: clusterView{State: "ACTIVE", PauseReason: "token expired"}, want: false},
		{
			name: "creating with ready condition",
			cl: clusterView{
				State:      "CREATING",
				Conditions: map[string]clusterCondition{"Ready": {Status: "True"}},
			},
			want: false,
		},
		{
			name: "active with ready false",
			cl: clusterView{
				State:      "ACTIVE",
				Conditions: map[string]clusterCondition{"Ready": {Status: "False"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := clusterReady(tt.cl); got != tt.want {
				t.Fatalf("clusterReady = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseConfigMapData(t *testing.T) {
	t.Parallel()
	data, err := parseConfigMapData([]byte(`{"data":{"from":"fleetshift-e2e-backend"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if data["from"] != "fleetshift-e2e-backend" {
		t.Fatalf("got %v", data)
	}
}

func TestParseConfigMapData_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseConfigMapData([]byte("{")); err == nil {
		t.Fatal("expected error")
	}
}
