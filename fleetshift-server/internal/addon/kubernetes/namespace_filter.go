package kubernetes

import "path/filepath"

// NamespaceFilterConfig controls which namespaces are included in indexing.
// Glob patterns use filepath.Match syntax.
type NamespaceFilterConfig struct {
	IncludePatterns []string
	ExcludePatterns []string
}

// NamespaceFilter evaluates whether a resource's namespace passes the
// configured include/exclude patterns.
type NamespaceFilter struct {
	config NamespaceFilterConfig
}

// NewNamespaceFilter creates a NamespaceFilter from the given config.
func NewNamespaceFilter(cfg NamespaceFilterConfig) *NamespaceFilter {
	return &NamespaceFilter{config: cfg}
}

// IsNamespaceAllowed returns true if the namespace passes the filter.
// Cluster-scoped resources (empty namespace) always pass — namespace
// patterns don't apply to them. For named namespaces: if IncludePatterns
// is non-empty, the namespace must match at least one include pattern;
// then if ExcludePatterns is non-empty, it must NOT match any exclude pattern.
func (f *NamespaceFilter) IsNamespaceAllowed(namespace string) bool {
	if namespace == "" {
		return true
	}

	// Include check: if patterns are specified, namespace must match at least one.
	if len(f.config.IncludePatterns) > 0 {
		matched := false
		for _, pattern := range f.config.IncludePatterns {
			if ok, err := filepath.Match(pattern, namespace); err == nil && ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Exclude check: namespace must NOT match any exclude pattern.
	for _, pattern := range f.config.ExcludePatterns {
		if ok, err := filepath.Match(pattern, namespace); err == nil && ok {
			return false
		}
	}

	return true
}
