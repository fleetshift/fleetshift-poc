package kubernetes

import (
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
		ExtractConditions: true,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"},
		Kind: "Service",
		Fields: []FieldExtraction{
			{Name: "type", JSONPath: ".spec.type"},
			{Name: "clusterIP", JSONPath: ".spec.clusterIP"},
			{Name: "ports", JSONPath: ".spec.ports", DataType: DataTypeSlice},
		},
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
		},
		ExtractConditions: true,
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
		ExtractConditions: true,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"},
		Kind: "StatefulSet",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
		},
		ExtractConditions: true,
	},
	{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"},
		Kind: "DaemonSet",
		Fields: []FieldExtraction{
			{Name: "desiredNumberScheduled", JSONPath: ".status.desiredNumberScheduled", DataType: DataTypeNumber},
			{Name: "numberReady", JSONPath: ".status.numberReady", DataType: DataTypeNumber},
		},
		ExtractConditions: true,
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
		ExtractConditions: true,
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
