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

// IsResourceAllowed returns true if the resource identified by group and kind
// passes the allow/deny filter rules. Deny takes precedence: a resource present
// in both lists is denied. An empty allow list means allow-all.
func IsResourceAllowed(group, kind string, allowedList, deniedList []Resource) bool {
	// Deny resources that match the deny list.
	g, k, denied := IsResourceMatchingList(deniedList, group, kind)
	if denied {
		// Check if resource is also in the allow list -- still denied.
		_, _, allowed := IsResourceMatchingList(allowedList, group, kind)
		if allowed {
			slog.Debug("deny resource: present in both allow and deny",
				"group", group, "kind", kind)
		} else {
			slog.Debug("deny resource: matched deny rule",
				"group", group, "kind", kind, "ruleGroup", g, "ruleKind", k)
		}
		return false
	}

	// If allowList not provided, interpret it as allow all resources.
	if len(allowedList) == 0 {
		return true
	}

	g, k, allowed := IsResourceMatchingList(allowedList, group, kind)
	if allowed {
		slog.Debug("allow resource: matched allow rule",
			"group", group, "kind", kind, "ruleGroup", g, "ruleKind", k)
		return true
	}

	slog.Debug("deny resource: no matching allow or deny rule",
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

// FilterSupportedResources filters discovered GVRs through the combined deny list
// (default + user-specified) and the allow list. It returns only GVRs that pass
// the IsResourceAllowed check.
func FilterSupportedResources(supported map[schema.GroupVersionResource]struct{}, denyList, allowList []Resource) []schema.GroupVersionResource {
	combinedDeny := append(DefaultDenyList, denyList...)
	var result []schema.GroupVersionResource
	for gvr := range supported {
		if IsResourceAllowed(gvr.Group, gvr.Resource, allowList, combinedDeny) {
			result = append(result, gvr)
		}
	}
	return result
}

// SupportedResources returns all GVRs on the cluster that support the WATCH verb.
// It uses ServerPreferredResources to get the preferred API version for each resource.
func SupportedResources(client discovery.DiscoveryInterface) (map[schema.GroupVersionResource]struct{}, error) {
	apiResources, err := client.ServerPreferredResources()
	if err != nil && apiResources == nil {
		return nil, err
	}
	if err != nil {
		slog.Warn("ServerPreferredResources returned partial results", "error", err)
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
