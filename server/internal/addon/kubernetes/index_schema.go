package kubernetes

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DataType controls how a JSONPath result is coerced before being
// stored as an observed field value.
type DataType string

const (
	DataTypeString    DataType = "string"
	DataTypeNumber    DataType = "number"
	DataTypeBytes     DataType = "bytes"
	DataTypeSlice     DataType = "slice"
	DataTypeMapString DataType = "mapString"
)

// FieldExtraction defines a single observed field to extract from a
// Kubernetes resource via a JSONPath expression.
type FieldExtraction struct {
	Name     string
	JSONPath string
	DataType DataType
}

// SchemaEntry describes optional enrichment for one watched Kubernetes
// GVR: field extractions, annotation extraction flags, and hooks.
// Watching itself is driven by discovery (+ allow/deny), not by presence
// in the schema; missing entries still inventory the object with base
// observation fields only.
type SchemaEntry struct {
	GVR                schema.GroupVersionResource
	Fields             []FieldExtraction
	ExtractAnnotations bool
	AnnotationSizeCap  int // max annotation value length when ExtractAnnotations is set; 0 means use the default cap
	ComputeExtra       func(r *unstructured.Unstructured, fields map[string]any)
	BuildEdges         func(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge
}

// IndexSchema is optional per-GVR enrichment configuration keyed by GVR.
// It is not the allow-list of what to watch; discovery selects watches.
type IndexSchema struct {
	Entries map[schema.GroupVersionResource]SchemaEntry
}

// GVRs returns the list of GVRs in the schema.
func (s IndexSchema) GVRs() []schema.GroupVersionResource {
	gvrs := make([]schema.GroupVersionResource, 0, len(s.Entries))
	for gvr := range s.Entries {
		gvrs = append(gvrs, gvr)
	}
	return gvrs
}
