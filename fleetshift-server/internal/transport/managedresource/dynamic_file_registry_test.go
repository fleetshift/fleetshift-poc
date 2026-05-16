package managedresource_test

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

func buildClusterFileDescriptor(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	schema := kindaddon.Schema()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
	}
	spec, err := managedresource.CompileInline(
		t.Context(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	cfg := &managedresource.ResourceTypeConfig{
		ResourceType:   kindaddon.ClusterResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: spec.Message,
	}
	descs, err := managedresource.BuildServiceDescriptors(cfg, spec.Message)
	if err != nil {
		t.Fatalf("BuildServiceDescriptors: %v", err)
	}
	return descs.File
}

func buildGCPHCPClusterFileDescriptor(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	schema := gcphcpaddon.Schema("gcphcp-example")
	spec, err := managedresource.CompileInline(
		t.Context(),
		schema.ProtoFiles,
		schema.EntryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	cfg := &managedresource.ResourceTypeConfig{
		ResourceType:   gcphcpaddon.ClusterResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: spec.Message,
	}
	descs, err := managedresource.BuildServiceDescriptors(cfg, spec.Message)
	if err != nil {
		t.Fatalf("BuildServiceDescriptors: %v", err)
	}
	return descs.File
}

func TestDynamicFileRegistry_RegisterAndFind(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	fd := buildClusterFileDescriptor(t)

	if err := reg.Register(fd); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := reg.FindFileByPath(string(fd.Path()))
	if err != nil {
		t.Fatalf("FindFileByPath: %v", err)
	}
	if got.Path() != fd.Path() {
		t.Fatalf("got path %q, want %q", got.Path(), fd.Path())
	}

	// FindDescriptorByName should find the service.
	svcName := protoreflect.FullName("fleetshift.v1.KindClusterService")
	desc, err := reg.FindDescriptorByName(svcName)
	if err != nil {
		t.Fatalf("FindDescriptorByName(%s): %v", svcName, err)
	}
	if desc.FullName() != svcName {
		t.Fatalf("got name %q, want %q", desc.FullName(), svcName)
	}
}

func TestDynamicFileRegistry_RegistersMultipleManagedResourceSchemas(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	kindFD := buildClusterFileDescriptor(t)
	gcphcpFD := buildGCPHCPClusterFileDescriptor(t)

	if err := reg.Register(kindFD); err != nil {
		t.Fatalf("Register(kind): %v", err)
	}
	if err := reg.Register(gcphcpFD); err != nil {
		t.Fatalf("Register(gcphcp): %v", err)
	}

	for _, svcName := range []protoreflect.FullName{
		"fleetshift.v1.KindClusterService",
		"fleetshift.v1.GCPHCPClusterService",
	} {
		desc, err := reg.FindDescriptorByName(svcName)
		if err != nil {
			t.Fatalf("FindDescriptorByName(%s): %v", svcName, err)
		}
		if desc.FullName() != svcName {
			t.Fatalf("got name %q, want %q", desc.FullName(), svcName)
		}
	}
}

func TestDynamicFileRegistry_DuplicateRegisterReturnsError(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	fd := buildClusterFileDescriptor(t)

	if err := reg.Register(fd); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := reg.Register(fd); err == nil {
		t.Fatal("expected error on duplicate register, got nil")
	}
}

func TestDynamicFileRegistry_ReplaceUpdatesDescriptor(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	fd := buildClusterFileDescriptor(t)

	if err := reg.Register(fd); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Replace with the same descriptor (simulates reconnection with same schema).
	reg.Replace(fd)

	got, err := reg.FindFileByPath(string(fd.Path()))
	if err != nil {
		t.Fatalf("FindFileByPath after Replace: %v", err)
	}
	if got.Path() != fd.Path() {
		t.Fatalf("got path %q, want %q", got.Path(), fd.Path())
	}
}

func TestDynamicFileRegistry_DeregisterRemovesDescriptor(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	fd := buildClusterFileDescriptor(t)

	if err := reg.Register(fd); err != nil {
		t.Fatalf("Register: %v", err)
	}

	reg.Deregister(string(fd.Path()))

	_, err := reg.FindFileByPath(string(fd.Path()))
	if err == nil {
		t.Fatal("expected error after Deregister, got nil")
	}

	svcName := protoreflect.FullName("fleetshift.v1.KindClusterService")
	_, err = reg.FindDescriptorByName(svcName)
	if err == nil {
		t.Fatal("expected error for FindDescriptorByName after Deregister, got nil")
	}
}

func TestDynamicFileRegistry_DeregisterNoOp(t *testing.T) {
	reg := managedresource.NewDynamicFileRegistry()
	// Should not panic.
	reg.Deregister("nonexistent.proto")
}
