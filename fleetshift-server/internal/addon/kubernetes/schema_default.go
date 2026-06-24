package kubernetes

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DefaultKubernetesSchema returns the built-in schema for common
// Kubernetes resource types (pods, deployments, services, etc.).
func DefaultKubernetesSchema() IndexSchema {
	s := IndexSchema{Entries: make(map[schema.GroupVersionResource]SchemaEntry)}
	for _, e := range defaultEntries {
		s.Entries[e.GVR] = e
	}
	return s
}

// defaultEntries defines the built-in set of Kubernetes resource types
// and their extraction rules.
var defaultEntries = []SchemaEntry{
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "phase", JSONPath: ".status.phase"},
			{Name: "podIP", JSONPath: ".status.podIP"},
			{Name: "hostIP", JSONPath: ".status.hostIP"},
			{Name: "nodeName", JSONPath: ".spec.nodeName"},
			{Name: "containerImages", JSONPath: ".status.containerStatuses[*].image", DataType: DataTypeSlice},
			{Name: "restartCount", JSONPath: ".status.containerStatuses[*].restartCount", DataType: DataTypeSlice},
		},
		ComputeExtra: computePodStatus,
		BuildEdges:   buildPodEdges,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"},
		Kind: "Service",
		Fields: []FieldExtraction{
			{Name: "type", JSONPath: ".spec.type"},
			{Name: "clusterIP", JSONPath: ".spec.clusterIP"},
			{Name: "ports", JSONPath: ".spec.ports", DataType: DataTypeSlice},
		},
		BuildEdges: buildServiceEdges,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"},
		Kind: "Namespace",
		Fields: []FieldExtraction{
			{Name: "phase", JSONPath: ".status.phase"},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
		Kind: "Node",
		Fields: []FieldExtraction{
			{Name: "kubeletVersion", JSONPath: ".status.nodeInfo.kubeletVersion"},
			{Name: "osImage", JSONPath: ".status.nodeInfo.osImage"},
			{Name: "memoryAllocatable", JSONPath: ".status.allocatable.memory", DataType: DataTypeBytes},
			{Name: "memoryCapacity", JSONPath: ".status.capacity.memory", DataType: DataTypeBytes},
			{Name: "cpuAllocatable", JSONPath: ".status.allocatable.cpu"},
			{Name: "cpuCapacity", JSONPath: ".status.capacity.cpu"},
			{Name: "ipAddress", JSONPath: `.status.addresses[?(@.type=="InternalIP")].address`},
		},
		ComputeExtra: computeNodeRoles,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
		Kind: "PersistentVolumeClaim",
		Fields: []FieldExtraction{
			{Name: "storageClassName", JSONPath: ".spec.storageClassName"},
			{Name: "phase", JSONPath: ".status.phase"},
			{Name: "capacity", JSONPath: ".status.capacity.storage"},
			{Name: "requestedStorage", JSONPath: ".spec.resources.requests.storage", DataType: DataTypeBytes},
		},
		BuildEdges: buildPVCEdges,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"},
		Kind: "PersistentVolume",
		Fields: []FieldExtraction{
			{Name: "storageClassName", JSONPath: ".spec.storageClassName"},
			{Name: "phase", JSONPath: ".status.phase"},
			{Name: "capacity", JSONPath: ".spec.capacity.storage"},
			{Name: "reclaimPolicy", JSONPath: ".spec.persistentVolumeReclaimPolicy"},
			{Name: "accessModes", JSONPath: ".spec.accessModes", DataType: DataTypeSlice},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
			{Name: "availableReplicas", JSONPath: ".status.availableReplicas", DataType: DataTypeNumber},
			{Name: "updatedReplicas", JSONPath: ".status.updatedReplicas", DataType: DataTypeNumber},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"},
		Kind: "StatefulSet",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"},
		Kind: "DaemonSet",
		Fields: []FieldExtraction{
			{Name: "desiredNumberScheduled", JSONPath: ".status.desiredNumberScheduled", DataType: DataTypeNumber},
			{Name: "numberReady", JSONPath: ".status.numberReady", DataType: DataTypeNumber},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		Kind: "ReplicaSet",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"},
		Kind: "Job",
		Fields: []FieldExtraction{
			{Name: "active", JSONPath: ".status.active", DataType: DataTypeNumber},
			{Name: "failed", JSONPath: ".status.failed", DataType: DataTypeNumber},
			{Name: "succeeded", JSONPath: ".status.succeeded", DataType: DataTypeNumber},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"},
		Kind: "CronJob",
		Fields: []FieldExtraction{
			{Name: "lastScheduleTime", JSONPath: ".status.lastScheduleTime"},
			{Name: "schedule", JSONPath: ".spec.schedule"},
		},
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"},
		Kind: "ConfigMap",
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"},
		Kind: "Secret",
	},
}

// computePodStatus computes an aggregate status for a pod by examining
// container statuses, phase, reason, and deletionTimestamp.
// Ported from search-collector pkg/transforms/pod.go:43-110.
func computePodStatus(r *unstructured.Unstructured, fields map[string]any) {
	// Start with phase as the base reason
	phase, _, _ := unstructured.NestedString(r.Object, "status", "phase")
	reason := phase

	// Override with status.reason if present
	if statusReason, found, _ := unstructured.NestedString(r.Object, "status", "reason"); found && statusReason != "" {
		reason = statusReason
	}

	// Check init container statuses
	initContainerStatuses, _, _ := unstructured.NestedSlice(r.Object, "status", "initContainerStatuses")
	for _, ics := range initContainerStatuses {
		cs, ok := ics.(map[string]any)
		if !ok {
			continue
		}

		// Check terminated state
		if terminated, found, _ := unstructured.NestedMap(cs, "state", "terminated"); found {
			if exitCode, _, _ := unstructured.NestedInt64(terminated, "exitCode"); exitCode != 0 {
				if r, _, _ := unstructured.NestedString(terminated, "reason"); r != "" {
					reason = "Init:" + r
				} else {
					reason = fmt.Sprintf("Init:ExitCode:%d", exitCode)
				}
				break
			}
			if signal, found, _ := unstructured.NestedInt64(terminated, "signal"); found && signal != 0 {
				if r, _, _ := unstructured.NestedString(terminated, "reason"); r != "" {
					reason = "Init:" + r
				} else {
					reason = fmt.Sprintf("Init:Signal:%d", signal)
				}
				break
			}
		}

		// Check waiting state
		if waiting, found, _ := unstructured.NestedMap(cs, "state", "waiting"); found {
			if r, _, _ := unstructured.NestedString(waiting, "reason"); r != "" {
				reason = "Init:" + r
				break
			}
		}
	}

	// Check container statuses (reverse order to prioritize the last failing container)
	containerStatuses, _, _ := unstructured.NestedSlice(r.Object, "status", "containerStatuses")
	for i := len(containerStatuses) - 1; i >= 0; i-- {
		cs, ok := containerStatuses[i].(map[string]any)
		if !ok {
			continue
		}

		// Check waiting state
		if waiting, found, _ := unstructured.NestedMap(cs, "state", "waiting"); found {
			if r, _, _ := unstructured.NestedString(waiting, "reason"); r != "" {
				reason = r
				break
			}
		}

		// Check terminated state
		if terminated, found, _ := unstructured.NestedMap(cs, "state", "terminated"); found {
			if r, _, _ := unstructured.NestedString(terminated, "reason"); r != "" {
				reason = r
				break
			}
			if signal, found, _ := unstructured.NestedInt64(terminated, "signal"); found && signal != 0 {
				reason = fmt.Sprintf("Signal:%d", signal)
				break
			}
			if exitCode, found, _ := unstructured.NestedInt64(terminated, "exitCode"); found && exitCode != 0 {
				reason = fmt.Sprintf("ExitCode:%d", exitCode)
				break
			}
		}
	}

	// Check if "Completed" but has running containers
	if reason == "Completed" {
		for _, cs := range containerStatuses {
			csMap, ok := cs.(map[string]any)
			if !ok {
				continue
			}
			if running, found, _ := unstructured.NestedMap(csMap, "state", "running"); found && running != nil {
				reason = "Running"
				break
			}
		}
	}

	// Handle deletion
	if deletionTimestamp, found, _ := unstructured.NestedString(r.Object, "metadata", "deletionTimestamp"); found && deletionTimestamp != "" {
		if reason == "NodeLost" {
			reason = "Unknown"
		} else {
			reason = "Terminating"
		}
	}

	fields["status"] = reason
}

// buildPodEdges creates a BuildEdges closure for pods that extracts node name,
// secrets, configmaps, and PVC references at extraction time, and builds edges
// at flush time.
// Ported from search-collector pkg/transforms/pod.go:146-220.
func buildPodEdges(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge {
	// Extract node name
	nodeName, _, _ := unstructured.NestedString(r.Object, "spec", "nodeName")
	namespace, _, _ := unstructured.NestedString(r.Object, "metadata", "namespace")

	// Extract secret, configmap, and PVC references from volumes
	secretNames := make(map[string]bool)
	configMapNames := make(map[string]bool)
	pvcNames := make(map[string]bool)

	volumes, _, _ := unstructured.NestedSlice(r.Object, "spec", "volumes")
	for _, vol := range volumes {
		volMap, ok := vol.(map[string]any)
		if !ok {
			continue
		}

		// Secret volumes
		if secretName, found, _ := unstructured.NestedString(volMap, "secret", "secretName"); found && secretName != "" {
			secretNames[secretName] = true
		}

		// ConfigMap volumes
		if cmName, found, _ := unstructured.NestedString(volMap, "configMap", "name"); found && cmName != "" {
			configMapNames[cmName] = true
		}

		// PVC volumes
		if pvcName, found, _ := unstructured.NestedString(volMap, "persistentVolumeClaim", "claimName"); found && pvcName != "" {
			pvcNames[pvcName] = true
		}
	}

	// Extract secret and configmap references from container env
	containers, _, _ := unstructured.NestedSlice(r.Object, "spec", "containers")
	for _, cont := range containers {
		contMap, ok := cont.(map[string]any)
		if !ok {
			continue
		}

		env, _, _ := unstructured.NestedSlice(contMap, "env")
		for _, e := range env {
			envMap, ok := e.(map[string]any)
			if !ok {
				continue
			}

			// Secret key refs
			if secretName, found, _ := unstructured.NestedString(envMap, "valueFrom", "secretKeyRef", "name"); found && secretName != "" {
				secretNames[secretName] = true
			}

			// ConfigMap key refs
			if cmName, found, _ := unstructured.NestedString(envMap, "valueFrom", "configMapKeyRef", "name"); found && cmName != "" {
				configMapNames[cmName] = true
			}
		}
	}

	// Return closure that builds edges at flush time
	return func(ns NodeStore) []Edge {
		var edges []Edge
		sourceKind := "Pod"

		// runsOn edge to Node
		if nodeName != "" {
			if nodeMap, ok := ns.ByKindNamespaceName["Node"]["_NONE"]; ok {
				if node, ok := nodeMap[nodeName]; ok {
					edges = append(edges, Edge{
						EdgeType:   EdgeRunsOn,
						SourceUID:  uid,
						DestUID:    node.UID,
						SourceKind: sourceKind,
						DestKind:   "Node",
					})
				}
			}
		}

		// attachedTo edges to Secrets
		if secretMap, ok := ns.ByKindNamespaceName["Secret"][namespace]; ok {
			for secretName := range secretNames {
				if secret, ok := secretMap[secretName]; ok {
					edges = append(edges, Edge{
						EdgeType:   EdgeAttachedTo,
						SourceUID:  uid,
						DestUID:    secret.UID,
						SourceKind: sourceKind,
						DestKind:   "Secret",
					})
				}
			}
		}

		// attachedTo edges to ConfigMaps
		if cmMap, ok := ns.ByKindNamespaceName["ConfigMap"][namespace]; ok {
			for cmName := range configMapNames {
				if cm, ok := cmMap[cmName]; ok {
					edges = append(edges, Edge{
						EdgeType:   EdgeAttachedTo,
						SourceUID:  uid,
						DestUID:    cm.UID,
						SourceKind: sourceKind,
						DestKind:   "ConfigMap",
					})
				}
			}
		}

		// attachedTo edges to PVCs
		if pvcMap, ok := ns.ByKindNamespaceName["PersistentVolumeClaim"][namespace]; ok {
			for pvcName := range pvcNames {
				if pvc, ok := pvcMap[pvcName]; ok {
					edges = append(edges, Edge{
						EdgeType:   EdgeAttachedTo,
						SourceUID:  uid,
						DestUID:    pvc.UID,
						SourceKind: sourceKind,
						DestKind:   "PersistentVolumeClaim",
					})
				}
			}
		}

		return edges
	}
}

// buildServiceEdges creates a BuildEdges closure for services that extracts
// the label selector at extraction time and matches pods at flush time.
// Ported from search-collector pkg/transforms/service.go:61-98.
func buildServiceEdges(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge {
	// Extract label selector
	selector, _, _ := unstructured.NestedStringMap(r.Object, "spec", "selector")
	namespace, _, _ := unstructured.NestedString(r.Object, "metadata", "namespace")

	return func(ns NodeStore) []Edge {
		var edges []Edge

		if podMap, ok := ns.ByKindNamespaceName["Pod"][namespace]; ok {
			for _, pod := range podMap {
				if matchesSelector(pod.Labels, selector) {
					edges = append(edges, Edge{
						EdgeType:   EdgeSelects,
						SourceUID:  uid,
						DestUID:    pod.UID,
						SourceKind: "Service",
						DestKind:   "Pod",
					})
				}
			}
		}

		return edges
	}
}

// matchesSelector checks if labels satisfy every key/value in selector.
func matchesSelector(labels map[string]string, selector map[string]string) bool {
	if len(selector) == 0 || len(labels) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// buildPVCEdges creates a BuildEdges closure for PVCs that extracts the
// volume name at extraction time and builds an attachedTo edge to the PV
// at flush time.
// Ported from search-collector pkg/transforms/persistentvolumeclaim.go:65-85.
func buildPVCEdges(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge {
	// Extract volume name
	volumeName, _, _ := unstructured.NestedString(r.Object, "spec", "volumeName")

	return func(ns NodeStore) []Edge {
		var edges []Edge

		if volumeName == "" {
			return edges
		}

		// Look up PV in cluster-scoped resources
		if pvMap, ok := ns.ByKindNamespaceName["PersistentVolume"]["_NONE"]; ok {
			if pv, ok := pvMap[volumeName]; ok {
				edges = append(edges, Edge{
					EdgeType:   EdgeAttachedTo,
					SourceUID:  uid,
					DestUID:    pv.UID,
					SourceKind: "PersistentVolumeClaim",
					DestKind:   "PersistentVolume",
				})
			}
		}

		return edges
	}
}

// computeNodeRoles extracts node roles from labels with the prefix
// "node-role.kubernetes.io/" and stores them as a comma-separated string
// in the "role" field.
// Ported from search-collector pkg/transforms/node.go:40-55.
func computeNodeRoles(r *unstructured.Unstructured, fields map[string]any) {
	labels, _, _ := unstructured.NestedStringMap(r.Object, "metadata", "labels")

	var roles []string
	rolePrefix := "node-role.kubernetes.io/"

	for key, value := range labels {
		if strings.HasPrefix(key, rolePrefix) && value == "" {
			role := strings.TrimPrefix(key, rolePrefix)
			if role != "" {
				roles = append(roles, role)
			}
		}
	}

	// Default to "worker" if no roles found
	if len(roles) == 0 {
		roles = []string{"worker"}
	}

	// Sort roles for consistent output
	sort.Strings(roles)

	fields["role"] = strings.Join(roles, ",")
}
