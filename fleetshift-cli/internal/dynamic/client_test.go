package dynamic_test

import (
	"context"
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

	if len(types) == 0 {
		t.Fatal("expected at least one resource type, got none")
	}

	found := false
	for _, rt := range types {
		if rt.ServiceName == "fleetshift.v1.KindClusterService" {
			found = true
			if rt.Singular != "KindCluster" {
				t.Errorf("singular = %q, want KindCluster", rt.Singular)
			}
			if rt.Plural != "KindClusters" {
				t.Errorf("plural = %q, want KindClusters", rt.Plural)
			}
		}
	}
	if !found {
		t.Fatalf("KindClusterService not found in types: %v", types)
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
	rt, err := client.ResolveType(context.Background(), "kindclusters")
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
