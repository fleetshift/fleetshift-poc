package steps

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestParseQueryPage(t *testing.T) {
	t.Parallel()
	const in = `{
  "resources": [
    {
      "name": "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/node-uid",
      "resourceType": "kubernetes.fleetshift.io/Object",
      "resource": {
        "name": "clusters/kind-e2e-abcd/apiResources/nodes/objects/node-uid",
        "observation": {
          "kind": "Node",
          "gvr": {"group": "", "version": "v1", "resource": "nodes", "scope": "cluster"},
          "metadata": {"name": "fs--kind-e2e-abcd-control-plane", "uid": "node-uid"},
          "extracted": {"kubeletVersion": "v1.31.0"}
        }
      }
    }
  ],
  "nextPageToken": "tok"
}`
	page, err := parseQueryPage(in)
	if err != nil {
		t.Fatal(err)
	}
	if page.NextPageToken != "tok" {
		t.Fatalf("NextPageToken = %q, want tok", page.NextPageToken)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("len(resources) = %d, want 1", len(page.Resources))
	}
	hit := page.Resources[0]
	if hit.ResourceType != kubernetesObjectQueryType {
		t.Fatalf("ResourceType = %q, want %q", hit.ResourceType, kubernetesObjectQueryType)
	}
	if observationKind(hit) != "Node" {
		t.Fatalf("kind = %q, want Node", observationKind(hit))
	}
	if observationMetaName(hit) != "fs--kind-e2e-abcd-control-plane" {
		t.Fatalf("metadata.name = %q, want fs--kind-e2e-abcd-control-plane", observationMetaName(hit))
	}
	if got := extractedString(hit, "kubeletVersion"); got != "v1.31.0" {
		t.Fatalf("kubeletVersion = %q, want v1.31.0", got)
	}
	if !kubernetesObjectInCluster(hit, "kind-e2e-abcd") {
		t.Fatalf("in-cluster = false, name %s", hit.Name)
	}
}

func TestParseQueryPage_Edges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		wantErr bool
		wantLen int
	}{
		{name: "empty resources", in: `{"resources":[],"nextPageToken":""}`, wantLen: 0},
		{name: "null resources", in: `{}`, wantLen: 0},
		{name: "invalid json", in: `{`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			page, err := parseQueryPage(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatal("parseQueryPage() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseQueryPage() unexpected error: %v", err)
			}
			if page.Resources == nil {
				t.Fatal("Resources is nil, want empty slice")
			}
			if len(page.Resources) != tt.wantLen {
				t.Fatalf("len(Resources) = %d, want %d", len(page.Resources), tt.wantLen)
			}
			if page.NextPageToken != "" {
				t.Fatalf("NextPageToken = %q, want empty", page.NextPageToken)
			}
		})
	}
}

func TestResourceQueryRequestArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  resourceQueryRequest
		want []string
	}{
		{
			name: "all fields",
			req: resourceQueryRequest{
				filter:    `resourceType == "kind.fleetshift.io/Cluster"`,
				pageSize:  10,
				pageToken: "tok",
				orderBy:   "resource_type,name",
			},
			want: []string{
				"resource", "query",
				"--filter", `resourceType == "kind.fleetshift.io/Cluster"`,
				"--page-size", "10",
				"--page-token", "tok",
				"--order-by", "resource_type,name",
			},
		},
		{
			name: "empty omits flags",
			req:  resourceQueryRequest{},
			want: []string{"resource", "query"},
		},
		{
			name: "zero page size omitted",
			req:  resourceQueryRequest{filter: "x", pageSize: 0, pageToken: "tok"},
			want: []string{"resource", "query", "--filter", "x", "--page-token", "tok"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.req.args(); !slices.Equal(got, tt.want) {
				t.Fatalf("args() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQueryFilters(t *testing.T) {
	t.Parallel()
	const clusterID = "kind-e2e-abcd"
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "kubernetes objects in cluster",
			got:  kubernetesObjectsInClusterFilter(clusterID),
			want: `resourceType == "kubernetes.fleetshift.io/Object" && name.startsWith("//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/")`,
		},
		{
			name: "kubernetes object kind",
			got:  kubernetesObjectKindFilter(clusterID, "Node"),
			want: `resourceType == "kubernetes.fleetshift.io/Object" && name.startsWith("//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/") && resource.observation.kind == "Node"`,
		},
		{
			name: "configmap",
			got:  kubernetesObjectConfigMapFilter(clusterID, "default", "test-config"),
			want: `resourceType == "kubernetes.fleetshift.io/Object" && name.startsWith("//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/") && resource.observation.kind == "ConfigMap" && resource.observation.metadata.namespace == "default" && resource.observation.metadata.name == "test-config"`,
		},
		{
			name: "kind cluster ready",
			got:  kindClusterReadyFilter(clusterID),
			want: `resourceType == "kind.fleetshift.io/Cluster" && resource.name == "clusters/kind-e2e-abcd" && resource.state == "ACTIVE" && resource.conditions["Ready"].status == "True"`,
		},
		{
			name: "kind node in cluster",
			got:  kindNodeInClusterFilter(clusterID),
			want: `resourceType == "kind.fleetshift.io/Node" && resource.observation.cluster == "clusters/kind-e2e-abcd"`,
		},
		{
			name: "denied kubernetes GVRs",
			got:  deniedKubernetesGVRFilter(),
			want: `resourceType == "kubernetes.fleetshift.io/Object" && resource.observation.gvr.resource in ["events","leases","endpoints","endpointslices","componentstatuses"]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Fatalf("filter = %s, want %s", tt.got, tt.want)
			}
		})
	}
}

func TestKubernetesObjectNamePrefix_EscapesClusterID(t *testing.T) {
	t.Parallel()
	got := kubernetesObjectNamePrefix("a/b")
	want := "//kubernetes.fleetshift.io/clusters/a%2Fb/"
	if got != want {
		t.Fatalf("kubernetesObjectNamePrefix(a/b) = %q, want %q", got, want)
	}
}

func TestKubernetesObjectInCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		hit       queryHit
		clusterID string
		want      bool
	}{
		{
			name: "cluster-scoped node",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/node-uid",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
			want:      true,
		},
		{
			name: "namespaced configmap",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/namespaces/default/apiResources/configmaps/objects/cm-uid",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
			want:      true,
		},
		{
			name: "other cluster",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/other/apiResources/nodes/objects/node-uid",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
		{
			name: "kind cluster envelope",
			hit: queryHit{
				Name:         "//kind.fleetshift.io/clusters/kind-e2e-abcd",
				ResourceType: kindClusterQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
		{
			name: "missing objects leaf",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
		{
			name: "empty uid leaf",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
		{
			name: "missing apiResources",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/namespaces/default/objects/cm-uid",
				ResourceType: kubernetesObjectQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
		{
			name: "wrong resource type with object name",
			hit: queryHit{
				Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/node-uid",
				ResourceType: kindNodeQueryType,
			},
			clusterID: "kind-e2e-abcd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := kubernetesObjectInCluster(tt.hit, tt.clusterID); got != tt.want {
				t.Fatalf("kubernetesObjectInCluster() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamesOutsideCluster(t *testing.T) {
	t.Parallel()
	inCluster := queryHit{
		Name:         "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/n1",
		ResourceType: kubernetesObjectQueryType,
	}
	otherCluster := queryHit{
		Name:         "//kubernetes.fleetshift.io/clusters/other/apiResources/nodes/objects/n2",
		ResourceType: kubernetesObjectQueryType,
	}
	kindEnvelope := queryHit{
		Name:         "//kind.fleetshift.io/clusters/kind-e2e-abcd",
		ResourceType: kindClusterQueryType,
	}
	got := namesOutsideCluster([]queryHit{inCluster, otherCluster, kindEnvelope}, "kind-e2e-abcd")
	want := []string{otherCluster.Name, kindEnvelope.Name}
	if !slices.Equal(got, want) {
		t.Fatalf("namesOutsideCluster() = %v, want %v", got, want)
	}
	if got := namesOutsideCluster(nil, "kind-e2e-abcd"); got != nil {
		t.Fatalf("namesOutsideCluster(nil) = %v, want nil", got)
	}
}

func TestKindClusterEnvelopeName(t *testing.T) {
	t.Parallel()
	if got := kindClusterEnvelopeName("kind-e2e-abcd"); got != "//kind.fleetshift.io/clusters/kind-e2e-abcd" {
		t.Fatalf("kindClusterEnvelopeName() = %q, want //kind.fleetshift.io/clusters/kind-e2e-abcd", got)
	}
}

func TestObservationMetaNames(t *testing.T) {
	t.Parallel()
	hits := []queryHit{
		{Resource: []byte(`{"observation":{"metadata":{"name":"default"}}}`)},
		{Resource: []byte(`{`)},
		{Resource: []byte(`{"observation":{"metadata":{"name":""}}}`)},
		{Resource: []byte(`{"observation":{"metadata":{"name":"kube-system"}}}`)},
	}
	got := observationMetaNames(hits)
	want := []string{"default", "kube-system"}
	if !slices.Equal(got, want) {
		t.Fatalf("observationMetaNames() = %v, want %v", got, want)
	}
}

func TestKindNodeLeafNames(t *testing.T) {
	t.Parallel()
	hits := []queryHit{
		{Name: kindNodeEnvelopePrefix + "fs--kind-e2e-abcd-control-plane"},
		{Name: "//kind.fleetshift.io/clusters/kind-e2e-abcd"},
		{Name: kindNodeEnvelopePrefix},
		{Name: kindNodeEnvelopePrefix + "worker"},
	}
	got := kindNodeLeafNames(hits)
	want := []string{"fs--kind-e2e-abcd-control-plane", "worker"}
	if !slices.Equal(got, want) {
		t.Fatalf("kindNodeLeafNames() = %v, want %v", got, want)
	}
}

func TestQueryHitObservation_Invalid(t *testing.T) {
	t.Parallel()
	hit := queryHit{Name: "broken", Resource: []byte(`{`)}
	if _, err := hit.observation(); err == nil {
		t.Fatal("observation() error = nil, want error")
	}
	got := hit.observationOrZero()
	if got.Kind != "" || got.Metadata.Name != "" || got.Extracted != nil {
		t.Fatalf("observationOrZero() = %+v, want zero", got)
	}
}

func TestQueryHitObservation_KindNodeCluster(t *testing.T) {
	t.Parallel()
	hit := queryHit{Resource: []byte(`{"observation":{"cluster":"clusters/kind-e2e-abcd","kubeletVersion":"v1.31.0"}}`)}
	obs, err := hit.observation()
	if err != nil {
		t.Fatal(err)
	}
	if obs.Cluster != "clusters/kind-e2e-abcd" {
		t.Fatalf("Cluster = %q, want clusters/kind-e2e-abcd", obs.Cluster)
	}
}

func TestQueryHitNames(t *testing.T) {
	t.Parallel()
	got := queryHitNames([]queryHit{{Name: "a"}, {Name: "b"}})
	if got != "a,b" {
		t.Fatalf("queryHitNames() = %q, want a,b", got)
	}
}

func TestExtractedString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		hit  queryHit
		key  string
		want string
	}{
		{
			name: "missing key",
			hit:  queryHit{Resource: []byte(`{"observation":{"extracted":{}}}`)},
			key:  "kubeletVersion",
		},
		{
			name: "non-string value",
			hit:  queryHit{Resource: []byte(`{"observation":{"extracted":{"kubeletVersion":1}}}`)},
			key:  "kubeletVersion",
		},
		{
			name: "string value",
			hit:  queryHit{Resource: []byte(`{"observation":{"extracted":{"kubeletVersion":"v1.31.0"}}}`)},
			key:  "kubeletVersion",
			want: "v1.31.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractedString(tt.hit, tt.key); got != tt.want {
				t.Fatalf("extractedString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCollectQueryPages(t *testing.T) {
	t.Parallel()
	var tokens []string
	hits, firstNext := collectQueryPagesWith(t, resourceQueryRequest{filter: "f", pageSize: 2, orderBy: "name"}, func(req resourceQueryRequest) queryPage {
		if req.filter != "f" || req.pageSize != 2 || req.orderBy != "name" {
			t.Fatalf("fetch req = %+v, want filter=f pageSize=2 orderBy=name", req)
		}
		tokens = append(tokens, req.pageToken)
		switch req.pageToken {
		case "":
			return queryPage{
				Resources:     []queryHit{{Name: "a"}, {Name: "b"}},
				NextPageToken: "p2",
			}
		case "p2":
			return queryPage{Resources: []queryHit{{Name: "c"}}}
		default:
			t.Fatalf("unexpected page token %q", req.pageToken)
			return queryPage{}
		}
	})
	if firstNext != "p2" {
		t.Fatalf("firstNext = %q, want p2", firstNext)
	}
	if !slices.Equal(tokens, []string{"", "p2"}) {
		t.Fatalf("fetch tokens = %q, want [\"\" p2]", tokens)
	}
	got := queryHitNames(hits)
	if got != "a,b,c" {
		t.Fatalf("hits = %q, want a,b,c", got)
	}
}

func TestCollectQueryPages_Fatal(t *testing.T) {
	if mode := os.Getenv("TEST_COLLECT_PAGES_FATAL"); mode != "" {
		switch mode {
		case "duplicate":
			collectQueryPagesWith(t, resourceQueryRequest{}, func(resourceQueryRequest) queryPage {
				return queryPage{Resources: []queryHit{{Name: "a"}, {Name: "a"}}}
			})
		case "empty":
			collectQueryPagesWith(t, resourceQueryRequest{}, func(resourceQueryRequest) queryPage {
				return queryPage{}
			})
		case "max-pages":
			n := 0
			collectQueryPagesWith(t, resourceQueryRequest{}, func(resourceQueryRequest) queryPage {
				n++
				return queryPage{
					Resources:     []queryHit{{Name: fmt.Sprintf("h-%d", n)}},
					NextPageToken: "more",
				}
			})
		case "oversized":
			collectQueryPagesWith(t, resourceQueryRequest{pageSize: 2}, func(resourceQueryRequest) queryPage {
				return queryPage{Resources: []queryHit{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
			})
		case "short-token":
			collectQueryPagesWith(t, resourceQueryRequest{pageSize: 2}, func(resourceQueryRequest) queryPage {
				return queryPage{
					Resources:     []queryHit{{Name: "a"}},
					NextPageToken: "p2",
				}
			})
		default:
			t.Fatalf("unknown TEST_COLLECT_PAGES_FATAL %q", mode)
		}
		return
	}

	t.Parallel()
	tests := []struct {
		mode string
		want string
	}{
		{mode: "duplicate", want: `duplicate query name "a" on page 1`},
		{mode: "empty", want: "not to be empty"},
		{mode: "max-pages", want: fmt.Sprintf("query did not exhaust after %d pages", queryPaginationMaxPages)},
		{mode: "oversized", want: "want <= page-size 2"},
		{mode: "short-token", want: "want exactly page-size 2"},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCollectQueryPages_Fatal$", "-test.v=true")
			cmd.Env = append(os.Environ(), "TEST_COLLECT_PAGES_FATAL="+tt.mode)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("inner test should fail:\n%s", out)
			}
			if !strings.Contains(string(out), tt.want) {
				t.Fatalf("output missing %q:\n%s", tt.want, out)
			}
		})
	}
}
