package extensionresource_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
)

func TestActiveResourceRegistry_RegisterAndGet(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	ctx := context.Background()

	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	desc := (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()

	if _, ok, err := r.GetResourceQuerySchema(ctx, rt); err != nil || ok {
		t.Fatalf("GetResourceQuerySchema before Register: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "kind.fleetshift.io.v1.ClusterService",
		HTTPPrefix:      "/apis/kind.fleetshift.io/v1/clusters",
		DescriptorPath:  "dynamic/kind/fleetshift/io/v1/cluster_service.proto",
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType:   rt,
			ServiceName:    "kind.fleetshift.io",
			TypeName:       "Cluster",
			APIVersion:     "v1",
			CollectionName: "clusters",
			SpecDescriptor: desc,
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok, err := r.GetResourceQuerySchema(ctx, rt)
	if err != nil || !ok {
		t.Fatalf("GetResourceQuerySchema after Register: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
	if got.SpecDescriptor != desc {
		t.Errorf("SpecDescriptor = %v, want %v", got.SpecDescriptor, desc)
	}
	if got.APIVersion != "v1" {
		t.Errorf("APIVersion = %q, want %q", got.APIVersion, "v1")
	}

	schemas, err := r.ListResourceQuerySchemas(ctx)
	if err != nil {
		t.Fatalf("ListResourceQuerySchemas: %v", err)
	}
	if len(schemas) != 1 || schemas[0].ResourceType != rt {
		t.Errorf("ListResourceQuerySchemas = %+v, want a single entry for %q", schemas, rt)
	}

	active, ok := r.Get(rt)
	if !ok {
		t.Fatal("Get after Register: ok=false, want true")
	}
	if active.DefaultVersion != "v1" {
		t.Errorf("DefaultVersion = %q, want %q", active.DefaultVersion, "v1")
	}
	ver, ok := active.Versions["v1"]
	if !ok {
		t.Fatal("Versions[v1] missing")
	}
	if ver.GRPCServiceName != "kind.fleetshift.io.v1.ClusterService" {
		t.Errorf("GRPCServiceName = %q, want kind.fleetshift.io.v1.ClusterService", ver.GRPCServiceName)
	}
}

func TestActiveResourceRegistry_Unregister(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	ctx := context.Background()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "svc",
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	r.UnregisterByGRPCServiceName("svc")

	if _, ok, err := r.GetResourceQuerySchema(ctx, rt); err != nil || ok {
		t.Fatalf("GetResourceQuerySchema after Unregister: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if _, ok := r.Get(rt); ok {
		t.Fatal("Get after Unregister: ok=true, want false")
	}
	if _, _, ok := r.GetByGRPCServiceName("svc"); ok {
		t.Fatal("GetByGRPCServiceName after Unregister: ok=true, want false")
	}
}

func TestActiveResourceRegistry_RegisterSameVersionReplaces(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "old.Service",
		HTTPPrefix:      "/old",
		ContentHash:     [32]byte{1},
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "new.Service",
		HTTPPrefix:      "/new",
		ContentHash:     [32]byte{2},
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("second Register: %v", err)
	}

	active, ok := r.Get(rt)
	if !ok {
		t.Fatal("Get: ok=false")
	}
	ver := active.Versions["v1"]
	if ver.GRPCServiceName != "new.Service" || ver.HTTPPrefix != "/new" || ver.ContentHash != [32]byte{2} {
		t.Errorf("replaced version = %+v, want new.Service /new hash{2}", ver)
	}
	if _, _, ok := r.GetByGRPCServiceName("old.Service"); ok {
		t.Error("old gRPC service name still indexed after replace")
	}
	if _, _, ok := r.GetByGRPCServiceName("new.Service"); !ok {
		t.Error("new gRPC service name not indexed after replace")
	}
}

func TestActiveResourceRegistry_RegisterSecondVersionErrors(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "svc.v1",
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("Register v1: %v", err)
	}

	err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v2",
		GRPCServiceName: "svc.v2",
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v2"},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Register v2: err = %v, want ErrInvalidArgument", err)
	}

	active, ok := r.Get(rt)
	if !ok {
		t.Fatal("Get after rejected Register: type missing")
	}
	if len(active.Versions) != 1 {
		t.Errorf("Versions len = %d, want 1 (rejected registration must not mutate)", len(active.Versions))
	}
	if _, _, ok := r.GetByGRPCServiceName("svc.v2"); ok {
		t.Error("rejected v2 gRPC name was indexed")
	}
}

func TestActiveResourceRegistry_GetByGRPCServiceName(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "kind.ClusterService",
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	gotRT, ver, ok := r.GetByGRPCServiceName("kind.ClusterService")
	if !ok {
		t.Fatal("GetByGRPCServiceName: ok=false")
	}
	if gotRT != rt {
		t.Errorf("ResourceType = %q, want %q", gotRT, rt)
	}
	if ver.APIVersion != "v1" {
		t.Errorf("APIVersion = %q, want v1", ver.APIVersion)
	}
}

func TestActiveResourceRegistry_Contains(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	hash := [32]byte{9}

	if r.Contains(rt, "v1", "svc", hash) {
		t.Fatal("Contains before Register: true, want false")
	}

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "svc",
		ContentHash:     hash,
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if !r.Contains(rt, "v1", "svc", hash) {
		t.Fatal("Contains matching registration: false, want true")
	}
	if r.Contains(rt, "v1", "svc", [32]byte{1}) {
		t.Fatal("Contains different hash: true, want false")
	}
	if r.Contains(rt, "v1", "other", hash) {
		t.Fatal("Contains different gRPC name: true, want false")
	}
	if r.Contains(rt, "v2", "svc", hash) {
		t.Fatal("Contains different API version: true, want false")
	}
	if r.Contains("other.fleetshift.io/Cluster", "v1", "svc", hash) {
		t.Fatal("Contains different resource type: true, want false")
	}
}

func TestActiveResourceRegistry_RegisterRejectsForeignGRPCName(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "shared.Service",
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType: "a.fleetshift.io/Widget",
			APIVersion:   "v1",
		},
	}); err != nil {
		t.Fatalf("Register A: %v", err)
	}

	err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "shared.Service",
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType: "b.fleetshift.io/Widget",
			APIVersion:   "v1",
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Register B with A's gRPC name: err = %v, want ErrInvalidArgument", err)
	}

	if _, ok := r.Get("b.fleetshift.io/Widget"); ok {
		t.Fatal("rejected registration must not create type B")
	}
	owner, ver, ok := r.GetByGRPCServiceName("shared.Service")
	if !ok || owner != "a.fleetshift.io/Widget" || ver.QuerySchema.ResourceType != "a.fleetshift.io/Widget" {
		t.Fatalf("gRPC ownership = (%q, %+v, %v), want type A", owner, ver, ok)
	}
}

func TestActiveResourceRegistry_PlatformAcquireRelease(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const coll = domain.CollectionName("clusters")
	key := extensionresource.PlatformRegistrationKey{Collection: coll}

	transport := extensionresource.ActivePlatformRegistration{
		APIVersion:      "v1",
		GRPCServiceName: "fleetshift.v1.PlatformClusterService",
		HTTPPrefix:      "/apis/fleetshift.io/v1/clusters",
		DescriptorPath:  "dynamic/fleetshift/v1/platform_cluster_service.proto",
	}

	created, err := r.AddPlatformOwner(key, "kind.ClusterService", transport)
	if err != nil {
		t.Fatalf("AddPlatformOwner first: %v", err)
	}
	if !created {
		t.Fatal("first AddPlatformOwner: created=false, want true")
	}

	got, ok := r.GetPlatform(key)
	if !ok {
		t.Fatal("GetPlatform after first acquire: missing")
	}
	if got.RefCount() != 1 || !got.HasOwner("kind.ClusterService") {
		t.Fatalf("platform after first = %+v, want one owner kind.ClusterService", got)
	}
	if got.GRPCServiceName != transport.GRPCServiceName {
		t.Errorf("GRPCServiceName = %q, want %q", got.GRPCServiceName, transport.GRPCServiceName)
	}

	ownerKey, ok := r.PlatformKeyForOwner("kind.ClusterService")
	if !ok || ownerKey != key {
		t.Fatalf("PlatformKeyForOwner = (%v, %v), want %v", ownerKey, ok, key)
	}

	created, err = r.AddPlatformOwner(key, "gcphcp.ClusterService", extensionresource.ActivePlatformRegistration{})
	if err != nil {
		t.Fatalf("AddPlatformOwner second: %v", err)
	}
	if created {
		t.Fatal("second AddPlatformOwner: created=true, want false")
	}
	got, _ = r.GetPlatform(key)
	if got.RefCount() != 2 {
		t.Fatalf("RefCount = %d, want 2", got.RefCount())
	}

	// Idempotent re-add of the same owner.
	created, err = r.AddPlatformOwner(key, "kind.ClusterService", extensionresource.ActivePlatformRegistration{})
	if err != nil || created {
		t.Fatalf("idempotent AddPlatformOwner: created=%v err=%v, want created=false err=nil", created, err)
	}
	if got, _ = r.GetPlatform(key); got.RefCount() != 2 {
		t.Fatalf("RefCount after idempotent add = %d, want 2", got.RefCount())
	}

	dropped, last := r.RemovePlatformOwner(key, "kind.ClusterService")
	if last {
		t.Fatal("RemovePlatformOwner first of two: last=true, want false")
	}
	if dropped.GRPCServiceName != "" {
		// Transport only returned when the registration is fully dropped.
		t.Errorf("partial remove returned transport %+v, want empty", dropped)
	}
	if _, ok := r.PlatformKeyForOwner("kind.ClusterService"); ok {
		t.Fatal("PlatformKeyForOwner after remove: still present")
	}

	dropped, last = r.RemovePlatformOwner(key, "gcphcp.ClusterService")
	if !last {
		t.Fatal("RemovePlatformOwner last owner: last=false, want true")
	}
	if dropped.GRPCServiceName != transport.GRPCServiceName {
		t.Errorf("dropped transport = %+v, want original", dropped)
	}
	if _, ok := r.GetPlatform(key); ok {
		t.Fatal("GetPlatform after last release: still present")
	}
}

func TestActiveResourceRegistry_PlatformRejectsExtensionGRPCCollision(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const name = "fleetshift.v1.PlatformClusterService"

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: name,
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType: "kind.fleetshift.io/Cluster",
			APIVersion:   "v1",
		},
	}); err != nil {
		t.Fatalf("Register extension: %v", err)
	}

	_, err := r.AddPlatformOwner(
		extensionresource.PlatformRegistrationKey{Collection: "clusters"},
		"kind.ClusterService",
		extensionresource.ActivePlatformRegistration{GRPCServiceName: name, APIVersion: "v1"},
	)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("AddPlatformOwner colliding with extension: err = %v, want ErrInvalidArgument", err)
	}
}

func TestActiveResourceRegistry_ExtensionRejectsPlatformGRPCCollision(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const name = "fleetshift.v1.PlatformClusterService"

	if _, err := r.AddPlatformOwner(
		extensionresource.PlatformRegistrationKey{Collection: "clusters"},
		"kind.ClusterService",
		extensionresource.ActivePlatformRegistration{GRPCServiceName: name, APIVersion: "v1"},
	); err != nil {
		t.Fatalf("AddPlatformOwner: %v", err)
	}

	err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: name,
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType: "kind.fleetshift.io/Cluster",
			APIVersion:   "v1",
		},
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Register colliding with platform: err = %v, want ErrInvalidArgument", err)
	}
}

func TestActiveResourceRegistry_GetVersion(t *testing.T) {
	r := extensionresource.NewActiveResourceRegistry()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if _, ok := r.GetVersion(rt, "v1"); ok {
		t.Fatal("GetVersion empty: ok=true, want false")
	}

	if err := r.Register(extensionresource.ActiveResourceVersion{
		APIVersion:      "v1",
		GRPCServiceName: "svc.v1",
		HTTPPrefix:      "/old",
		QuerySchema:     domain.ResourceQuerySchema{ResourceType: rt, APIVersion: "v1"},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ver, ok := r.GetVersion(rt, "v1")
	if !ok {
		t.Fatal("GetVersion after Register: ok=false")
	}
	if ver.GRPCServiceName != "svc.v1" || ver.HTTPPrefix != "/old" {
		t.Errorf("GetVersion = %+v, want svc.v1 /old", ver)
	}
	if _, ok := r.GetVersion(rt, "v2"); ok {
		t.Fatal("GetVersion for unregistered version: ok=true, want false")
	}
}
