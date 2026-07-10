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

// SchemaEntry describes how a single Kubernetes resource type is
// indexed: which GVR to watch, which fields to extract, and whether
// to extract status conditions.
type SchemaEntry struct {
	GVR                schema.GroupVersionResource
	Kind               string
	Fields             []FieldExtraction
	ExtractAnnotations bool
	AnnotationSizeCap  int
	ComputeExtra       func(r *unstructured.Unstructured, fields map[string]any)
	BuildEdges         func(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge
}

// IndexSchema is the complete set of resource types to index,
// keyed by GVR.
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
