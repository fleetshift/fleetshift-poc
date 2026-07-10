package kubernetes

import (
	"log/slog"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// Resource describes a group of API resources for allow/deny filtering.
type Resource struct {
	ApiGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
}

// IsResourceAllowed checks whether a resource passes the allow/deny filter.
//
// Watch-all mode (allowList empty): user deny → default deny → ALLOW.
// Watch-selected mode (allowList non-empty): user deny → user allow (overrides default deny) → DENY.
func IsResourceAllowed(group, kind string, allowList, userDenyList, defaultDenyList []Resource, logger *slog.Logger) bool {
	if g, k, denied := IsResourceMatchingList(userDenyList, group, kind); denied {
		logger.Debug("deny resource: matched user deny rule",
			"group", group, "kind", kind, "ruleGroup", g, "ruleKind", k)
		return false
	}

	if len(allowList) == 0 {
		if g, k, denied := IsResourceMatchingList(defaultDenyList, group, kind); denied {
			logger.Debug("deny resource: matched default deny rule",
				"group", group, "kind", kind, "ruleGroup", g, "ruleKind", k)
			return false
		}
		return true
	}

	if g, k, allowed := IsResourceMatchingList(allowList, group, kind); allowed {
		logger.Debug("allow resource: matched allow rule",
			"group", group, "kind", kind, "ruleGroup", g, "ruleKind", k)
		return true
	}

	logger.Debug("deny resource: not in allow list",
		"group", group, "kind", kind)
	return false
}

// IsResourceMatchingList checks whether the given group and kind match any
// entry in resourceList. Wildcard "*" matches any group or kind.
func IsResourceMatchingList(resourceList []Resource, group, kind string) (string, string, bool) {
	for _, r := range resourceList {
		for _, g := range r.ApiGroups {
			for _, k := range r.Resources {
				if (g == "*" || g == group) && (k == "*" || k == kind) {
					return g, k, true
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

// FilterSupportedResources filters discovered GVRs through user deny, user allow,
// and the default deny list. User allow overrides default deny; user deny always wins.
func FilterSupportedResources(supported map[schema.GroupVersionResource]struct{}, denyList, allowList []Resource, logger *slog.Logger) []schema.GroupVersionResource {
	var result []schema.GroupVersionResource
	for gvr := range supported {
		if IsResourceAllowed(gvr.Group, gvr.Resource, allowList, denyList, DefaultDenyList, logger) {
			result = append(result, gvr)
		}
	}
	return result
}

// SupportedResources returns all GVRs on the cluster that support the WATCH verb.
// It uses ServerPreferredResources to get the preferred API version for each resource.
func SupportedResources(client discovery.DiscoveryInterface, logger *slog.Logger) (map[schema.GroupVersionResource]struct{}, error) {
	apiResources, err := client.ServerPreferredResources()
	if err != nil && apiResources == nil {
		return nil, err
	}
	if err != nil {
		logger.Warn("ServerPreferredResources returned partial results", "error", err)
	}

	// Build a filtered list containing only resources that support WATCH.
	var watchLists []*metav1.APIResourceList
	for _, apiList := range apiResources {
		groupVersion := strings.Split(apiList.GroupVersion, "/")
		// For core API group, groupVersion will be just the version (e.g. "v1").
		// Split gives a single element, so group is "".
		group := ""
		if len(groupVersion) == 2 {
			group = groupVersion[0]
		}

		var watchResources []metav1.APIResource
		for _, apiResource := range apiList.APIResources {
			_ = group // available for future allow/deny filtering
			for _, verb := range apiResource.Verbs {
				if verb == "watch" {
					watchResources = append(watchResources, apiResource)
					break
				}
			}
		}

		if len(watchResources) > 0 {
			watchLists = append(watchLists, &metav1.APIResourceList{
				GroupVersion: apiList.GroupVersion,
				APIResources: watchResources,
			})
		}
	}

	gvrList, err := discovery.GroupVersionResources(watchLists)
	return gvrList, err
}
