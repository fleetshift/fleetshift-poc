package kubernetes

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIndexSchema_GVRs(t *testing.T) {
	pod := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	deploy := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	s := IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
		pod:    {GVR: pod, Kind: "Pod"},
		deploy: {GVR: deploy, Kind: "Deployment"},
	}}

	got := s.GVRs()
	if len(got) != 2 {
		t.Fatalf("GVRs() returned %d entries, want 2", len(got))
	}
	seen := map[schema.GroupVersionResource]bool{}
	for _, gvr := range got {
		seen[gvr] = true
	}
	if !seen[pod] || !seen[deploy] {
		t.Fatalf("GVRs() = %v, want both pods and deployments", got)
	}
}

func TestIndexSchema_GVRs_Empty(t *testing.T) {
	got := (IndexSchema{}).GVRs()
	if len(got) != 0 {
		t.Fatalf("empty schema GVRs() = %v, want empty", got)
	}
}

func TestDataTypeConstants(t *testing.T) {
	cases := []struct {
		got  DataType
		want string
	}{
		{DataTypeString, "string"},
		{DataTypeNumber, "number"},
		{DataTypeBytes, "bytes"},
		{DataTypeSlice, "slice"},
		{DataTypeMapString, "mapString"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("DataType %q = %q, want %q", tc.want, tc.got, tc.want)
		}
	}
}
