package platformresource

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func clusterPlatformConfig() *Config {
	return &Config{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      "v1",
			Singular:     "Cluster",
			Plural:       "Clusters",
			CollectionID: "clusters",
		},
	}
}

func TestBuildServiceDescriptors_Success(t *testing.T) {
	desc, err := BuildServiceDescriptors(clusterPlatformConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Service name.
	if got, want := string(desc.Service.FullName()), "fleetshift.v1.PlatformClusterService"; got != want {
		t.Errorf("service full name = %q, want %q", got, want)
	}

	// --- Resource message fields ---
	res := desc.Resource
	if res == nil {
		t.Fatal("resource descriptor is nil")
	}
	if got, want := string(res.Name()), "PlatformCluster"; got != want {
		t.Errorf("resource name = %q, want %q", got, want)
	}

	expectedFields := map[string]protoreflect.Kind{
		"name":        protoreflect.StringKind,
		"uid":         protoreflect.StringKind,
		"create_time": protoreflect.MessageKind,
		"update_time": protoreflect.MessageKind,
	}
	for name, wantKind := range expectedFields {
		fd := res.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			t.Errorf("resource message missing field %q", name)
			continue
		}
		if fd.Kind() != wantKind {
			t.Errorf("field %q kind = %v, want %v", name, fd.Kind(), wantKind)
		}
	}

	// Map fields: labels and effective_labels should be map<string,string>.
	for _, mapName := range []string{"labels", "effective_labels"} {
		fd := res.Fields().ByName(protoreflect.Name(mapName))
		if fd == nil {
			t.Errorf("resource message missing map field %q", mapName)
			continue
		}
		if !fd.IsMap() {
			t.Errorf("field %q should be a map, got isList=%v isMap=%v", mapName, fd.IsList(), fd.IsMap())
		}
	}

	// Repeated message fields: representations, aliases, relationships.
	for _, repeatedName := range []string{"representations", "aliases", "relationships"} {
		fd := res.Fields().ByName(protoreflect.Name(repeatedName))
		if fd == nil {
			t.Errorf("resource message missing repeated field %q", repeatedName)
			continue
		}
		if !fd.IsList() {
			t.Errorf("field %q should be a list", repeatedName)
		}
		if fd.Kind() != protoreflect.MessageKind {
			t.Errorf("field %q kind = %v, want MessageKind", repeatedName, fd.Kind())
		}
	}

	// Helper message field counts.
	repMsg := res.Fields().ByName("representations").Message()
	if repMsg == nil {
		t.Fatal("representations message descriptor is nil")
	}
	if got, want := repMsg.Fields().Len(), 6; got != want {
		t.Errorf("Representation fields = %d, want %d", got, want)
	}

	aliasMsg := res.Fields().ByName("aliases").Message()
	if aliasMsg == nil {
		t.Fatal("aliases message descriptor is nil")
	}
	if got, want := aliasMsg.Fields().Len(), 3; got != want {
		t.Errorf("Alias fields = %d, want %d", got, want)
	}

	relMsg := res.Fields().ByName("relationships").Message()
	if relMsg == nil {
		t.Fatal("relationships message descriptor is nil")
	}
	if got, want := relMsg.Fields().Len(), 4; got != want {
		t.Errorf("Relationship fields = %d, want %d", got, want)
	}

	// --- Request / response messages ---
	if desc.CreateRequest == nil {
		t.Error("CreateRequest descriptor is nil")
	} else if got, want := string(desc.CreateRequest.Name()), "CreatePlatformClusterRequest"; got != want {
		t.Errorf("CreateRequest name = %q, want %q", got, want)
	}

	if desc.GetRequest == nil {
		t.Error("GetRequest descriptor is nil")
	} else if got, want := string(desc.GetRequest.Name()), "GetPlatformClusterRequest"; got != want {
		t.Errorf("GetRequest name = %q, want %q", got, want)
	}

	if desc.ListRequest == nil {
		t.Error("ListRequest descriptor is nil")
	} else if got, want := string(desc.ListRequest.Name()), "ListPlatformClustersRequest"; got != want {
		t.Errorf("ListRequest name = %q, want %q", got, want)
	}

	if desc.ListResponse == nil {
		t.Error("ListResponse descriptor is nil")
	} else if got, want := string(desc.ListResponse.Name()), "ListPlatformClustersResponse"; got != want {
		t.Errorf("ListResponse name = %q, want %q", got, want)
	}

	// --- Service methods ---
	methods := desc.Service.Methods()
	wantMethods := []string{
		"CreatePlatformCluster",
		"GetPlatformCluster",
		"ListPlatformClusters",
	}
	if got, want := methods.Len(), len(wantMethods); got != want {
		t.Fatalf("method count = %d, want %d", got, want)
	}
	for _, name := range wantMethods {
		m := methods.ByName(protoreflect.Name(name))
		if m == nil {
			t.Errorf("missing method %q", name)
		}
	}
}

func TestBuildServiceDescriptors_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
	}{
		{name: "nil config", cfg: nil},
		{
			name: "empty singular",
			cfg: &Config{CollectionConfig: dynamicapi.CollectionConfig{
				Singular: "", Plural: "Clusters", CollectionID: "clusters",
			}},
		},
		{
			name: "empty plural",
			cfg: &Config{CollectionConfig: dynamicapi.CollectionConfig{
				Singular: "Cluster", Plural: "", CollectionID: "clusters",
			}},
		},
		{
			name: "empty collection ID",
			cfg: &Config{CollectionConfig: dynamicapi.CollectionConfig{
				Singular: "Cluster", Plural: "Clusters", CollectionID: "",
			}},
		},
		{
			name: "lowercase singular",
			cfg: &Config{CollectionConfig: dynamicapi.CollectionConfig{
				Singular: "cluster", Plural: "Clusters", CollectionID: "clusters",
			}},
		},
		{
			name: "lowercase plural",
			cfg: &Config{CollectionConfig: dynamicapi.CollectionConfig{
				Singular: "Cluster", Plural: "clusters", CollectionID: "clusters",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildServiceDescriptors(tt.cfg)
			if err == nil {
				t.Error("expected error for invalid input, got nil")
			}
		})
	}
}
