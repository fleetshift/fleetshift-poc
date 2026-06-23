package kubernetes

import "testing"

func TestNamespaceFilter_AllowAll(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{})

	if !f.IsNamespaceAllowed("default") {
		t.Error("expected default namespace allowed")
	}
	if !f.IsNamespaceAllowed("kube-system") {
		t.Error("expected kube-system allowed")
	}
	if !f.IsNamespaceAllowed("") {
		t.Error("expected cluster-scoped allowed")
	}
}

func TestNamespaceFilter_IncludeOnly(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns: []string{"prod-*", "staging"},
	})

	if !f.IsNamespaceAllowed("prod-us") {
		t.Error("expected prod-us allowed by include pattern")
	}
	if !f.IsNamespaceAllowed("staging") {
		t.Error("expected staging allowed by include pattern")
	}
	if f.IsNamespaceAllowed("dev-1") {
		t.Error("expected dev-1 denied: no include match")
	}
	if !f.IsNamespaceAllowed("") {
		t.Error("expected cluster-scoped allowed")
	}
}

func TestNamespaceFilter_ExcludeOnly(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{
		ExcludePatterns: []string{"kube-*", "openshift-*"},
	})

	if !f.IsNamespaceAllowed("default") {
		t.Error("expected default allowed")
	}
	if f.IsNamespaceAllowed("kube-system") {
		t.Error("expected kube-system denied by exclude pattern")
	}
	if f.IsNamespaceAllowed("openshift-monitoring") {
		t.Error("expected openshift-monitoring denied by exclude pattern")
	}
}

func TestNamespaceFilter_IncludeAndExclude(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns: []string{"prod-*"},
		ExcludePatterns: []string{"prod-canary"},
	})

	if !f.IsNamespaceAllowed("prod-us") {
		t.Error("expected prod-us allowed")
	}
	if f.IsNamespaceAllowed("prod-canary") {
		t.Error("expected prod-canary denied by exclude")
	}
	if f.IsNamespaceAllowed("staging") {
		t.Error("expected staging denied: no include match")
	}
}

func TestNamespaceFilter_InvalidPatternIgnored(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns: []string{"["},
	})

	if f.IsNamespaceAllowed("anything") {
		t.Error("expected denied: invalid include pattern should not match")
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
			f := NewNamespaceFilter(tc.cfg)
			if !f.IsNamespaceAllowed("") {
				t.Error("expected cluster-scoped (empty namespace) to always pass")
			}
		})
	}
}
