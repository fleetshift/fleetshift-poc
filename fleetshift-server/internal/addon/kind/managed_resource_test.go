package kind

import (
	"encoding/json"
	"testing"
)

func TestParseClusterManifest(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    ClusterSpec
		wantErr bool
	}{
		{
			name: "name only",
			raw:  `{"name":"my-cluster"}`,
			want: ClusterSpec{Name: "my-cluster"},
		},
		{
			name: "with nodes",
			raw:  `{"name":"multi","nodes":[{"role":"control-plane"},{"role":"worker"}]}`,
			want: ClusterSpec{
				Name:  "multi",
				Nodes: []NodeSpec{{Role: "control-plane"}, {Role: "worker"}},
			},
		},
		{
			name: "with networking",
			raw:  `{"name":"netcluster","networking":{"apiServerPort":6443,"podSubnet":"10.244.0.0/16"}}`,
			want: ClusterSpec{
				Name:       "netcluster",
				Networking: &NetworkSpec{APIServerPort: 6443, PodSubnet: "10.244.0.0/16"},
			},
		},
		{
			name: "with node image",
			raw:  `{"name":"pinned","nodes":[{"role":"control-plane","image":"kindest/node:v1.31.0"}]}`,
			want: ClusterSpec{
				Name:  "pinned",
				Nodes: []NodeSpec{{Role: "control-plane", Image: "kindest/node:v1.31.0"}},
			},
		},
		{
			name: "with oidc",
			raw:  `{"name":"oidc-cluster","oidc":{"usernameClaim":"email","groupsClaim":"roles"}}`,
			want: ClusterSpec{
				Name: "oidc-cluster",
				OIDC: &OIDCSpec{UsernameClaim: "email", GroupsClaim: "roles"},
			},
		},
		{
			name:    "workers only is allowed",
			raw:     `{"name":"workers","nodes":[{"role":"worker"},{"role":"worker"}]}`,
			want:    ClusterSpec{Name: "workers", Nodes: []NodeSpec{{Role: "worker"}, {Role: "worker"}}},
		},
		{
			name:    "empty name",
			raw:     `{"nodes":[{"role":"control-plane"}]}`,
			wantErr: true,
		},
		{
			name:    "whitespace-only name",
			raw:     `{"name":"  "}`,
			wantErr: true,
		},
		{
			name:    "invalid node role",
			raw:     `{"name":"bad-role","nodes":[{"role":"master"}]}`,
			wantErr: true,
		},
		{
			name:    "invalid json",
			raw:     `{not valid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseClusterManifest(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseClusterManifest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.Name != tt.want.Name {
				t.Errorf("Name = %q, want %q", got.Name, tt.want.Name)
			}
			if len(got.Nodes) != len(tt.want.Nodes) {
				t.Fatalf("Nodes len = %d, want %d", len(got.Nodes), len(tt.want.Nodes))
			}
			for i, n := range got.Nodes {
				if n != tt.want.Nodes[i] {
					t.Errorf("Nodes[%d] = %+v, want %+v", i, n, tt.want.Nodes[i])
				}
			}
			if (got.Networking == nil) != (tt.want.Networking == nil) {
				t.Fatalf("Networking nil mismatch: got %v, want %v", got.Networking, tt.want.Networking)
			}
			if got.Networking != nil && *got.Networking != *tt.want.Networking {
				t.Errorf("Networking = %+v, want %+v", *got.Networking, *tt.want.Networking)
			}
			if (got.OIDC == nil) != (tt.want.OIDC == nil) {
				t.Fatalf("OIDC nil mismatch: got %v, want %v", got.OIDC, tt.want.OIDC)
			}
			if got.OIDC != nil && *got.OIDC != *tt.want.OIDC {
				t.Errorf("OIDC = %+v, want %+v", *got.OIDC, *tt.want.OIDC)
			}
		})
	}
}

func TestBuildKindConfig(t *testing.T) {
	spec := ClusterSpec{
		Name: "test",
		Nodes: []NodeSpec{
			{Role: "control-plane"},
			{Role: "worker", Image: "kindest/node:v1.31.0"},
		},
		Networking: &NetworkSpec{
			APIServerPort: 6443,
			PodSubnet:     "10.244.0.0/16",
		},
	}

	raw, err := buildKindConfig(spec)
	if err != nil {
		t.Fatalf("buildKindConfig: %v", err)
	}

	var got kindConfig
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if got.Kind != "Cluster" {
		t.Errorf("kind = %q, want %q", got.Kind, "Cluster")
	}
	if got.APIVersion != "kind.x-k8s.io/v1alpha4" {
		t.Errorf("apiVersion = %q, want %q", got.APIVersion, "kind.x-k8s.io/v1alpha4")
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("nodes len = %d, want 2", len(got.Nodes))
	}
	if got.Nodes[0].Role != "control-plane" {
		t.Errorf("nodes[0].role = %q, want %q", got.Nodes[0].Role, "control-plane")
	}
	if got.Nodes[1].Image != "kindest/node:v1.31.0" {
		t.Errorf("nodes[1].image = %q, want %q", got.Nodes[1].Image, "kindest/node:v1.31.0")
	}
	if got.Networking == nil {
		t.Fatal("networking is nil")
	}
	if got.Networking.APIServerPort != 6443 {
		t.Errorf("apiServerPort = %d, want 6443", got.Networking.APIServerPort)
	}
	if got.Networking.PodSubnet != "10.244.0.0/16" {
		t.Errorf("podSubnet = %q, want %q", got.Networking.PodSubnet, "10.244.0.0/16")
	}
}

func TestBuildKindConfig_NoNodesOrNetworking(t *testing.T) {
	spec := ClusterSpec{Name: "bare"}
	if spec.hasClusterConfig() {
		t.Error("hasClusterConfig() should be false for spec with no nodes/networking")
	}
}
