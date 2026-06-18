package kubernetes

import "testing"

func TestNamespaceFilter_AllowAll(t *testing.T) {
	// Empty patterns: everything allowed.
	f := NewNamespaceFilter(NamespaceFilterConfig{IncludeClusterScoped: true})

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

func TestNamespaceFilter_ClusterScopedDisabled(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{IncludeClusterScoped: false})

	if f.IsNamespaceAllowed("") {
		t.Error("expected cluster-scoped denied when IncludeClusterScoped=false")
	}
	if !f.IsNamespaceAllowed("default") {
		t.Error("expected named namespace allowed")
	}
}

func TestNamespaceFilter_IncludeOnly(t *testing.T) {
	f := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns:      []string{"prod-*", "staging"},
		IncludeClusterScoped: true,
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
		ExcludePatterns:      []string{"kube-*", "openshift-*"},
		IncludeClusterScoped: true,
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
		IncludePatterns:      []string{"prod-*"},
		ExcludePatterns:      []string{"prod-canary"},
		IncludeClusterScoped: true,
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
	// filepath.Match returns an error for invalid patterns like "[".
	// These should be treated as non-matching.
	f := NewNamespaceFilter(NamespaceFilterConfig{
		IncludePatterns:      []string{"["},
		IncludeClusterScoped: true,
	})

	if f.IsNamespaceAllowed("anything") {
		t.Error("expected denied: invalid include pattern should not match")
	}
}
