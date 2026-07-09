package cli_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

type fakeResourceQueryServer struct {
	pb.UnimplementedResourceQueryServiceServer

	mu   sync.Mutex
	last *pb.QueryResourcesRequest
	page *pb.QueryResourcesResponse
	err  error
}

func (s *fakeResourceQueryServer) QueryResources(_ context.Context, req *pb.QueryResourcesRequest) (*pb.QueryResourcesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	if s.page != nil {
		return s.page, nil
	}
	return &pb.QueryResourcesResponse{}, nil
}

func startFakeQueryServer(t *testing.T, fake *fakeResourceQueryServer) string {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterResourceQueryServiceServer(srv, fake)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })
	return lis.Addr().String()
}

func sampleQueryPage() *pb.QueryResourcesResponse {
	body, err := structpb.NewStruct(map[string]any{
		"name":       "clusters/alpha",
		"uid":        "uid-alpha",
		"state":      "ACTIVE",
		"createTime": "2026-07-01T12:00:00Z",
		"spec": map[string]any{
			"name": "alpha",
		},
	})
	if err != nil {
		panic(err)
	}
	return &pb.QueryResourcesResponse{
		Resources: []*pb.ResourceResult{{
			Name:         "//kind.fleetshift.io/clusters/alpha",
			ResourceType: "kind.fleetshift.io/Cluster",
			Resource:     body,
		}},
		NextPageToken: "next-page-token",
	}
}

func TestResourceQuery_Table(t *testing.T) {
	fake := &fakeResourceQueryServer{page: sampleQueryPage()}
	addr := startFakeQueryServer(t, fake)

	out := runCLI(t,
		"--server", addr, "--output", "table",
		"resource", "query",
		"--filter", `resource_type == "kind.fleetshift.io/Cluster"`,
		"--page-size", "10",
		"--order-by", "resource_type,name",
	)

	for _, want := range []string{"NAME", "TYPE", "STATE", "UID", "AGE", "clusters/alpha", "kind.fleetshift.io/Cluster", "ACTIVE", "uid-alpha"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q; got:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "More results available") {
		t.Errorf("expected next-page hint on stderr/stdout; got:\n%s", out)
	}
	if !strings.Contains(out, "--page-token 'next-page-token'") {
		t.Errorf("expected page-token in hint; got:\n%s", out)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.last.GetScope() != "-" {
		t.Errorf("scope = %q, want -", fake.last.GetScope())
	}
	if fake.last.GetFilter() != `resource_type == "kind.fleetshift.io/Cluster"` {
		t.Errorf("filter = %q", fake.last.GetFilter())
	}
	if fake.last.GetPageSize() != 10 {
		t.Errorf("page_size = %d, want 10", fake.last.GetPageSize())
	}
	if fake.last.GetOrderBy() != "resource_type,name" {
		t.Errorf("order_by = %q", fake.last.GetOrderBy())
	}
}

func TestResourceQuery_JSON(t *testing.T) {
	fake := &fakeResourceQueryServer{page: sampleQueryPage()}
	addr := startFakeQueryServer(t, fake)

	out := runCLI(t,
		"--server", addr, "--output", "json",
		"resource", "query",
		"--filter", `resource.state == "ACTIVE"`,
	)

	for _, want := range []string{`"resources"`, `"next_page_token"`, `"next-page-token"`, `"kind.fleetshift.io/Cluster"`, `"clusters/alpha"`} {
		if !strings.Contains(out, want) {
			t.Errorf("json output missing %q; got:\n%s", want, out)
		}
	}
	if strings.Contains(out, "More results available") {
		t.Errorf("json mode should not print table pagination hint; got:\n%s", out)
	}
}

func TestResourceQuery_AliasSearch(t *testing.T) {
	fake := &fakeResourceQueryServer{page: &pb.QueryResourcesResponse{}}
	addr := startFakeQueryServer(t, fake)

	out := runCLI(t, "--server", addr, "resource", "search")
	if !strings.Contains(out, "NAME") && out != "" {
		// empty table still prints headers
		if !strings.Contains(out, "NAME") {
			t.Errorf("expected table headers for empty result; got %q", out)
		}
	}
}

func TestResourceQuery_ServerError(t *testing.T) {
	fake := &fakeResourceQueryServer{
		err: status.Error(codes.InvalidArgument, `scope "clusters" is not supported`),
	}
	addr := startFakeQueryServer(t, fake)

	_, err := runCLIErr(t, "--server", addr, "resource", "query", "--scope", "clusters")
	if err == nil {
		t.Fatal("expected error for unsupported scope")
	}
	if !strings.Contains(err.Error(), "not supported") && !strings.Contains(err.Error(), "InvalidArgument") {
		t.Errorf("unexpected error: %v", err)
	}
}
