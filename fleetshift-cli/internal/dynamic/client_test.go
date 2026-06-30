package dynamic_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/testserver"
)

func TestClient_ListResourceTypes(t *testing.T) {
	addr := testserver.Start(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := dynamic.NewClient(conn)
	types, err := client.ListResourceTypes(context.Background())
	if err != nil {
		t.Fatalf("ListResourceTypes: %v", err)
	}

	if len(types) < 2 {
		t.Fatalf("expected at least 2 resource types (Kind + GCP HCP), got %d: %v", len(types), types)
	}

	byService := make(map[string]dynamic.ResourceType, len(types))
	for _, rt := range types {
		byService[rt.ServiceName] = rt
	}

	kind, ok := byService["kind.fleetshift.v1.ClusterService"]
	if !ok {
		t.Fatalf("kind.fleetshift.v1.ClusterService not found in types: %v", types)
	}
	if kind.Singular != "Cluster" {
		t.Errorf("kind singular = %q, want Cluster", kind.Singular)
	}
	if kind.Plural != "Clusters" {
		t.Errorf("kind plural = %q, want Clusters", kind.Plural)
	}
	if kind.ProtoPackage != "kind.fleetshift.v1" {
		t.Errorf("kind proto_package = %q, want kind.fleetshift.v1", kind.ProtoPackage)
	}
	if kind.CollectionID != "clusters" {
		t.Errorf("kind collection_id = %q, want clusters", kind.CollectionID)
	}

	gcp, ok := byService["gcphcp.fleetshift.v1.ClusterService"]
	if !ok {
		t.Fatalf("gcphcp.fleetshift.v1.ClusterService not found in types: %v", types)
	}
	if gcp.ProtoPackage != "gcphcp.fleetshift.v1" {
		t.Errorf("gcphcp proto_package = %q, want gcphcp.fleetshift.v1", gcp.ProtoPackage)
	}
	if gcp.CollectionID != "clusters" {
		t.Errorf("gcphcp collection_id = %q, want clusters", gcp.CollectionID)
	}
}

func TestClient_ResolveType_AmbiguousCollection(t *testing.T) {
	addr := testserver.Start(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := dynamic.NewClient(conn)

	// Resolving "clusters" without a service should fail because both
	// Kind and GCP HCP expose the same collection.
	_, err = client.ResolveType(context.Background(), "clusters", "")
	if err == nil {
		t.Fatal("expected ambiguity error, got nil")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguity error, got: %v", err)
	}
	// The error should suggest qualified names, not bare service names.
	if !strings.Contains(err.Error(), "kind.fleetshift.v1/clusters") {
		t.Errorf("error should suggest kind qualified name, got: %v", err)
	}
	if !strings.Contains(err.Error(), "gcphcp.fleetshift.v1/clusters") {
		t.Errorf("error should suggest gcphcp qualified name, got: %v", err)
	}

	// Resolving with an explicit service should succeed.
	rt, err := client.ResolveType(context.Background(), "clusters", "kind.fleetshift.v1.ClusterService")
	if err != nil {
		t.Fatalf("ResolveType with service: %v", err)
	}
	if rt.ServiceName != "kind.fleetshift.v1.ClusterService" {
		t.Errorf("service = %q, want kind.fleetshift.v1.ClusterService", rt.ServiceName)
	}
}

func TestClient_ResolveType_QualifiedName(t *testing.T) {
	addr := testserver.Start(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := dynamic.NewClient(conn)

	// Qualified name should resolve without ambiguity, even though
	// "clusters" alone is ambiguous.
	rt, err := client.ResolveType(context.Background(), "kind.fleetshift.v1/clusters", "")
	if err != nil {
		t.Fatalf("ResolveType qualified: %v", err)
	}
	if rt.ServiceName != "kind.fleetshift.v1.ClusterService" {
		t.Errorf("service = %q, want kind.fleetshift.v1.ClusterService", rt.ServiceName)
	}
	if rt.QualifiedName() != "kind.fleetshift.v1/clusters" {
		t.Errorf("qualified = %q, want kind.fleetshift.v1/clusters", rt.QualifiedName())
	}

	// GCP HCP qualified name should also resolve.
	rt, err = client.ResolveType(context.Background(), "gcphcp.fleetshift.v1/clusters", "")
	if err != nil {
		t.Fatalf("ResolveType gcphcp qualified: %v", err)
	}
	if rt.ServiceName != "gcphcp.fleetshift.v1.ClusterService" {
		t.Errorf("service = %q, want gcphcp.fleetshift.v1.ClusterService", rt.ServiceName)
	}

	// Unknown qualified name should error.
	_, err = client.ResolveType(context.Background(), "nonexistent.v1/clusters", "")
	if err == nil {
		t.Fatal("expected error for unknown qualified name, got nil")
	}
	if !strings.Contains(err.Error(), "unknown resource type") {
		t.Errorf("expected unknown type error, got: %v", err)
	}
}

func TestClient_CreateAndGet(t *testing.T) {
	addr := testserver.Start(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := dynamic.NewClient(conn)
	rt, err := client.ResolveType(context.Background(), "clusters", "kind.fleetshift.v1.ClusterService")
	if err != nil {
		t.Fatalf("ResolveType: %v", err)
	}

	specJSON := []byte(`{"name": "test-cluster"}`)
	_, err = client.Create(context.Background(), rt, "test-cluster", specJSON)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	resp, err := client.Get(context.Background(), rt, "test-cluster")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp == nil {
		t.Fatal("Get returned nil")
	}
}
