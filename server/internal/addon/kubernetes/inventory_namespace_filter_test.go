package kubernetes

import (
	"strings"
	"testing"
)

func mustNamespaceFilter(t *testing.T, cfg NamespaceFilterConfig) *NamespaceFilter {
	t.Helper()
	f, err := NewNamespaceFilter(cfg)
	if err != nil {
		t.Fatalf("NewNamespaceFilter: %v", err)
	}
	return f
}

func TestNamespaceFilter_AllowAll(t *testing.T) {
	f := mustNamespaceFilter(t, NamespaceFilterConfig{})

	if !f.IsNamespaceAllowed("default") {
		t.Error("expected default allowed")
	}
	if !f.IsNamespaceAllowed("kube-system") {
		t.Error("expected kube-system allowed")
	}
	if !f.IsNamespaceAllowed("") {
		t.Error("expected cluster-scoped allowed")
	}
}

func TestNamespaceFilter_IncludeOnly(t *testing.T) {
	f := mustNamespaceFilter(t, NamespaceFilterConfig{
		IncludePatterns: []string{"prod-*", "staging"},
	})

	if !f.IsNamespaceAllowed("prod-us") {
		t.Error("expected prod-us allowed")
	}
	if !f.IsNamespaceAllowed("staging") {
		t.Error("expected staging allowed")
	}
	if f.IsNamespaceAllowed("dev-1") {
		t.Error("expected dev-1 denied")
	}
	if !f.IsNamespaceAllowed("") {
		t.Error("expected cluster-scoped allowed")
	}
}

func TestNamespaceFilter_ExcludeOnly(t *testing.T) {
	f := mustNamespaceFilter(t, NamespaceFilterConfig{
		ExcludePatterns: []string{"kube-*", "openshift-*"},
	})

	if !f.IsNamespaceAllowed("default") {
		t.Error("expected default allowed")
	}
	if f.IsNamespaceAllowed("kube-system") {
		t.Error("expected kube-system denied")
	}
	if f.IsNamespaceAllowed("openshift-monitoring") {
		t.Error("expected openshift-monitoring denied")
	}
}

func TestNamespaceFilter_IncludeAndExclude(t *testing.T) {
	f := mustNamespaceFilter(t, NamespaceFilterConfig{
		IncludePatterns: []string{"prod-*"},
		ExcludePatterns: []string{"prod-canary"},
	})

	if !f.IsNamespaceAllowed("prod-us") {
		t.Error("expected prod-us allowed")
	}
	if f.IsNamespaceAllowed("prod-canary") {
		t.Error("expected prod-canary denied")
	}
	if f.IsNamespaceAllowed("staging") {
		t.Error("expected staging denied")
	}
}

func TestNamespaceFilter_InvalidPatternRejected(t *testing.T) {
	_, err := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns: []string{"["},
	})
	if err == nil {
		t.Fatal("expected invalid include pattern to be rejected")
	}
	if !strings.Contains(err.Error(), "invalid include pattern") {
		t.Fatalf("error = %v, want include pattern context", err)
	}

	_, err = NewNamespaceFilter(NamespaceFilterConfig{
		ExcludePatterns: []string{"["},
	})
	if err == nil {
		t.Fatal("expected invalid exclude pattern to be rejected")
	}
	if !strings.Contains(err.Error(), "invalid exclude pattern") {
		t.Fatalf("error = %v, want exclude pattern context", err)
	}
}

func TestNamespaceFilter_ClusterScopedAlwaysPasses(t *testing.T) {
	cases := []struct {
		name string
		cfg  NamespaceFilterConfig
	}{
		{"no patterns", NamespaceFilterConfig{}},
		{"include only", NamespaceFilterConfig{IncludePatterns: []string{"prod-*"}}},
		{"exclude only", NamespaceFilterConfig{ExcludePatterns: []string{"kube-*"}}},
		{"both", NamespaceFilterConfig{
			IncludePatterns: []string{"prod-*"},
			ExcludePatterns: []string{"prod-canary"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := mustNamespaceFilter(t, tc.cfg)
			if !f.IsNamespaceAllowed("") {
				t.Error("expected cluster-scoped resource to always pass")
			}
		})
	}
}
