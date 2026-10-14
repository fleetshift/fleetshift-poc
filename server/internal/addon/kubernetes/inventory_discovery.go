package kubernetes

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Resource describes a group of API resources for allow/deny filtering.
type Resource struct {
	ApiGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
}

// DiscoveredAPIResource is one watchable API resource selected from
// discovery, including the discovery-authoritative [ObjectScope] used
// for inventory naming. Scope is never inferred from object metadata.
// Construct via [NewDiscoveredAPIResource]; zero values are not usable.
type DiscoveredAPIResource struct {
	// GVR is the preferred group/version/resource to watch.
	GVR schema.GroupVersionResource
	// Scope is discovery's APIResource.Namespaced mapping
	// ([ObjectScopeNamespaced] or [ObjectScopeCluster]).
	Scope ObjectScope
}

// NewDiscoveredAPIResource constructs a [DiscoveredAPIResource] for a
// non-empty, non-subresource GVR and a concrete discovery [ObjectScope].
// It also requires a valid [GroupResourceKey] for the GVR's group/resource.
func NewDiscoveredAPIResource(gvr schema.GroupVersionResource, scope ObjectScope) (DiscoveredAPIResource, error) {
	if gvr.Empty() {
		return DiscoveredAPIResource{}, fmt.Errorf("%w: discovered API resource requires a non-empty GVR", domain.ErrInvalidArgument)
	}
	if strings.Contains(gvr.Resource, "/") {
		return DiscoveredAPIResource{}, fmt.Errorf("%w: discovered API resource rejects subresource %q", domain.ErrInvalidArgument, gvr.Resource)
	}
	scope, err := ParseObjectScope(string(scope))
	if err != nil {
		return DiscoveredAPIResource{}, err
	}
	if _, err := GroupResourceKey(gvr.GroupResource()); err != nil {
		return DiscoveredAPIResource{}, err
	}
	return DiscoveredAPIResource{GVR: gvr, Scope: scope}, nil
}

// IsResourceAllowed checks whether a resource passes the allow/deny filter.
//
// resource is the plural API resource name (e.g. "pods", "deployments"),
// not a Kubernetes Kind (e.g. "Pod").
//
// Watch-all mode (allowList empty): user deny → default deny → ALLOW.
// Watch-selected mode (allowList non-empty): user deny → user allow (overrides default deny) → DENY.
func IsResourceAllowed(group, resource string, allowList, userDenyList, defaultDenyList []Resource, logger *slog.Logger) bool {
	if g, r, denied := IsResourceMatchingList(userDenyList, group, resource); denied {
		logger.Debug("deny resource: matched user deny rule",
			"group", group, "resource", resource, "ruleGroup", g, "ruleResource", r)
		return false
	}

	if len(allowList) == 0 {
		if g, r, denied := IsResourceMatchingList(defaultDenyList, group, resource); denied {
			logger.Debug("deny resource: matched default deny rule",
				"group", group, "resource", resource, "ruleGroup", g, "ruleResource", r)
			return false
		}
		return true
	}

	if g, r, allowed := IsResourceMatchingList(allowList, group, resource); allowed {
		logger.Debug("allow resource: matched allow rule",
			"group", group, "resource", resource, "ruleGroup", g, "ruleResource", r)
		return true
	}

	logger.Debug("deny resource: not in allow list",
		"group", group, "resource", resource)
	return false
}

// IsResourceMatchingList checks whether the given group and plural API
// resource name match any entry in resourceList. Wildcard "*" matches
// any group or resource.
func IsResourceMatchingList(resourceList []Resource, group, resource string) (string, string, bool) {
	for _, r := range resourceList {
		for _, g := range r.ApiGroups {
			for _, res := range r.Resources {
				if (g == "*" || g == group) && (res == "*" || res == resource) {
					return g, res, true
				}
			}
		}
	}
	return "", "", false
}

// DefaultDenyList contains resource types that should never be watched by default.
// These are high-volume or low-value resources that would waste bandwidth and storage.
var DefaultDenyList = []Resource{
	{ApiGroups: []string{""}, Resources: []string{"events"}},
	{ApiGroups: []string{"events.k8s.io"}, Resources: []string{"events"}},
	{ApiGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}},
	{ApiGroups: []string{""}, Resources: []string{"endpoints"}},
	{ApiGroups: []string{"discovery.k8s.io"}, Resources: []string{"endpointslices"}},
	{ApiGroups: []string{""}, Resources: []string{"componentstatuses"}},
	{ApiGroups: []string{"oauth.openshift.io"}, Resources: []string{"oauthaccesstokens"}},
	{ApiGroups: []string{"oauth.openshift.io"}, Resources: []string{"oauthauthorizetokens"}},
	{ApiGroups: []string{"project.openshift.io"}, Resources: []string{"projects"}},
	{ApiGroups: []string{"packages.operators.coreos.com"}, Resources: []string{"packagemanifests"}},
}

// FilterSupportedResources filters [DiscoveredAPIResource] values through
// user deny, user allow, and the default deny list. User allow overrides
// default deny; user deny always wins. Values are assumed to come from
// [NewDiscoveredAPIResource].
func FilterSupportedResources(supported map[schema.GroupVersionResource]DiscoveredAPIResource, denyList, allowList []Resource, logger *slog.Logger) []DiscoveredAPIResource {
	var result []DiscoveredAPIResource
	for gvr, desc := range supported {
		if IsResourceAllowed(gvr.Group, gvr.Resource, allowList, denyList, DefaultDenyList, logger) {
			result = append(result, desc)
		}
	}
	return result
}

// SupportedResources returns watchable API resources from the cluster,
// including discovery-authoritative scope. It uses ServerPreferredResources
// to get the preferred API version for each resource. Subresources
// (names containing "/") are filtered and never inventoried.
func SupportedResources(client discovery.DiscoveryInterface, logger *slog.Logger) (map[schema.GroupVersionResource]DiscoveredAPIResource, error) {
	apiResources, err := client.ServerPreferredResources()
	if err != nil && apiResources == nil {
		return nil, err
	}
	if err != nil {
		logger.Warn("ServerPreferredResources returned partial results", "error", err)
	}

	result := make(map[schema.GroupVersionResource]DiscoveredAPIResource)
	for _, apiList := range apiResources {
		gv, parseErr := schema.ParseGroupVersion(apiList.GroupVersion)
		if parseErr != nil {
			logger.Warn("skipping API resource list with invalid groupVersion",
				"groupVersion", apiList.GroupVersion, "error", parseErr)
			continue
		}

		for _, apiResource := range apiList.APIResources {
			if strings.Contains(apiResource.Name, "/") {
				// Subresources are never independently inventoried.
				continue
			}
			watchable := slices.Contains(apiResource.Verbs, "watch")
			if !watchable {
				continue
			}
			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			}
			scope := ObjectScopeCluster
			if apiResource.Namespaced {
				scope = ObjectScopeNamespaced
			}
			desc, err := NewDiscoveredAPIResource(gvr, scope)
			if err != nil {
				logger.Warn("skipping invalid discovered API resource",
					"gvr", gvr.String(), "error", err)
				continue
			}
			result[desc.GVR] = desc
		}
	}
	return result, nil
}
