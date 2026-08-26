package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestIsFraming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{name: "run", s: "=== RUN   TestKindClusterLifecycle/wait_until_the_cluster_is_ready\n", want: true},
		{name: "pass", s: "--- PASS: TestKindClusterLifecycle/create_the_cluster (1.21s)\n", want: true},
		{name: "fail", s: "--- FAIL: TestOpsLoginAndCredentialIsolation (0.02s)\n", want: true},
		{name: "pause", s: "=== PAUSE TestFoo\n", want: true},
		{name: "log line", s: "    cluster.go:110: cluster x state=CREATING\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isFraming(tt.s); got != tt.want {
				t.Errorf("isFraming(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

func TestLogMessage(t *testing.T) {
	t.Parallel()
	got := logMessage("    cluster.go:110: cluster x state=CREATING\n")
	if got != "cluster x state=CREATING" {
		t.Fatalf("got %q", got)
	}
	got = logMessage("e2e/backend: fleetctl exit=0\n")
	if got != "e2e/backend: fleetctl exit=0" {
		t.Fatalf("got %q", got)
	}
}

func TestRender_HidesParentRun(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"output","Package":"p","Output":"e2e/backend: building AIO image\n"}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle"}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster"}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster","Elapsed":1.29}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle","Elapsed":34.6}`,
	)
	if strings.Contains(got, "⏳ Kind cluster lifecycle\n") {
		t.Fatalf("parent run leaked: %s", got)
	}
	if !strings.Contains(got, "e2e/backend: building AIO image") {
		t.Fatalf("missing setup line: %s", got)
	}
	if !strings.Contains(got, "✅ Create the cluster (1.29s)") {
		t.Fatalf("missing step pass: %s", got)
	}
	if !strings.Contains(got, "❯ Kind cluster lifecycle\n") {
		t.Fatalf("missing suite header: %s", got)
	}
}

func TestDisplayName(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"TestKindClusterLifecycle":                                         "Kind cluster lifecycle",
		"TestKindClusterLifecycle/wait_ready":                              "Wait ready",
		"TestKindClusterLifecycle/create_the_cluster":                      "Create the cluster",
		"TestKindClusterLifecycle/wait_until_the_cluster_is_ready":         "Wait until the cluster is ready",
		"TestKindClusterLifecycle/wait_until_OIDC_authentication_works":    "Wait until OIDC authentication works",
		"TestBootstrapDeployment/is listed":                                "Is listed",
		"TestBootstrapDeployment/is_listed":                                "Is listed",
		"TestFanOutToTwoClusters/see_the_namespace_on_the_first_cluster":   "See the namespace on the first cluster",
		"TestHTTPGateway/serves_the_livez_probe_without_a_token":           "Serves the livez probe without a token",
		"TestOpsLoginAndCredentialIsolation/credentials exist":             "Credentials exist",
		"TestOpsLoginAndCredentialIsolation/token_belongs_to_the_operator": "Token belongs to the operator",
		"TestDeveloperTargetIsolationAndResume/log_in_as_the_developer":    "Log in as the developer",
		"TestOIDCWriteToKindCluster":                                       "OIDC write to kind cluster",
	}
	for in, want := range cases {
		if got := displayName(in); got != want {
			t.Errorf("displayName(%q)=%q want %q", in, got, want)
		}
	}
}

func TestRender_LeafRootStillPrints(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestSomethingSimple"}`,
		`{"Action":"pass","Package":"p","Test":"TestSomethingSimple","Elapsed":0.04}`,
	)
	if !strings.Contains(got, "✅ Something simple (0.04s)") {
		t.Fatalf("leaf root pass missing: %s", got)
	}
}

func TestRender_LiveSteps(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle"}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/wait_until_the_cluster_is_ready"}`,
		`{"Action":"output","Package":"p","Test":"TestKindClusterLifecycle/wait_until_the_cluster_is_ready","Output":"=== RUN   TestKindClusterLifecycle/wait_until_the_cluster_is_ready\n"}`,
		`{"Action":"output","Package":"p","Test":"TestKindClusterLifecycle/wait_until_the_cluster_is_ready","Output":"    cluster.go:110: cluster x state=CREATING\n"}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle/wait_until_the_cluster_is_ready","Elapsed":12.77}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle","Elapsed":34.6}`,
		`DONE 16 tests in 59.513s`,
	)
	want := strings.Join([]string{
		"❯ Kind cluster lifecycle",
		"  ⏳ Wait until the cluster is ready",
		"    cluster x state=CREATING",
		"  ✅ Wait until the cluster is ready (12.77s)",
		"DONE 16 tests in 59.513s",
		"",
	}, "\n")
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRender_SkipAndFail(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/delete_the_cluster"}`,
		`{"Action":"skip","Package":"p","Test":"TestKindClusterLifecycle/delete_the_cluster","Elapsed":0}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster"}`,
		`{"Action":"fail","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster","Elapsed":1.2}`,
	)
	if !strings.Contains(got, "➖ Delete the cluster (0.00s)") {
		t.Fatalf("missing skip: %s", got)
	}
	if !strings.Contains(got, "❌ Create the cluster (1.20s)") {
		t.Fatalf("missing fail: %s", got)
	}
}

func TestRender_ParentFailStillPrints(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle"}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster"}`,
		`{"Action":"fail","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster","Elapsed":1.2}`,
		`{"Action":"fail","Package":"p","Test":"TestKindClusterLifecycle","Elapsed":1.2}`,
	)
	if !strings.Contains(got, "❌ Kind cluster lifecycle (1.20s)") {
		t.Fatalf("parent fail missing: %s", got)
	}
}

func TestClip(t *testing.T) {
	t.Parallel()
	if got := clip("ab", 4); got != "ab" {
		t.Errorf("clip(%q, 4) = %q, want %q", "ab", got, "ab")
	}
	if got := clip("abcd", 2); got != "ab…" {
		t.Errorf("clip(%q, 2) = %q, want %q", "abcd", got, "ab…")
	}
	if got := clip("héllo", 3); got != "hél…" {
		t.Errorf("clip(%q, 3) = %q, want %q", "héllo", got, "hél…")
	}
}

func TestRender_SentenceSteps(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestFanOutToTwoClusters"}`,
		`{"Action":"run","Package":"p","Test":"TestFanOutToTwoClusters/see_the_namespace_on_the_first_cluster"}`,
		`{"Action":"pass","Package":"p","Test":"TestFanOutToTwoClusters/see_the_namespace_on_the_first_cluster","Elapsed":0.25}`,
		`{"Action":"pass","Package":"p","Test":"TestFanOutToTwoClusters","Elapsed":18.5}`,
		`{"Action":"run","Package":"p","Test":"TestHTTPGateway"}`,
		`{"Action":"run","Package":"p","Test":"TestHTTPGateway/serves_the_livez_probe_without_a_token"}`,
		`{"Action":"pass","Package":"p","Test":"TestHTTPGateway/serves_the_livez_probe_without_a_token","Elapsed":0}`,
		`{"Action":"pass","Package":"p","Test":"TestHTTPGateway","Elapsed":0.01}`,
	)
	want := strings.Join([]string{
		"❯ Fan out to two clusters",
		"  ⏳ See the namespace on the first cluster",
		"  ✅ See the namespace on the first cluster (0.25s)",
		"",
		"❯ HTTP gateway",
		"  ⏳ Serves the livez probe without a token",
		"  ✅ Serves the livez probe without a token (0.00s)",
		"",
	}, "\n")
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRender_SuiteHeadersSeparate(t *testing.T) {
	t.Parallel()
	got := renderJSON(t,
		`{"Action":"run","Package":"p","Test":"TestBootstrapDeployment"}`,
		`{"Action":"run","Package":"p","Test":"TestBootstrapDeployment/is listed"}`,
		`{"Action":"pass","Package":"p","Test":"TestBootstrapDeployment/is listed","Elapsed":0.03}`,
		`{"Action":"pass","Package":"p","Test":"TestBootstrapDeployment","Elapsed":0.5}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle"}`,
		`{"Action":"run","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster"}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle/create_the_cluster","Elapsed":0.97}`,
		`{"Action":"pass","Package":"p","Test":"TestKindClusterLifecycle","Elapsed":1.2}`,
	)
	want := strings.Join([]string{
		"❯ Bootstrap deployment",
		"  ⏳ Is listed",
		"  ✅ Is listed (0.03s)",
		"",
		"❯ Kind cluster lifecycle",
		"  ⏳ Create the cluster",
		"  ✅ Create the cluster (0.97s)",
		"",
	}, "\n")
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func renderJSON(t *testing.T, lines ...string) string {
	t.Helper()
	in := strings.Join(lines, "\n") + "\n"
	var out bytes.Buffer
	if err := render(strings.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}
