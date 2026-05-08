package managedresource_test

import (
	"context"
	"io"
	"net"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

const kindClusterServiceName = "fleetshift.v1.KindClusterService"

func startReflectionServer(t *testing.T, activator *managedresource.DynamicSchemaActivator, mux *managedresource.DynamicServiceMux, fileReg *managedresource.DynamicFileRegistry) *grpc.ClientConn {
	t.Helper()

	srv := grpc.NewServer(grpc.UnknownServiceHandler(mux.Handle))
	managedresource.RegisterCompositeReflection(srv, mux, fileReg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func activateKindCluster(t *testing.T, activator *managedresource.DynamicSchemaActivator) {
	t.Helper()
	schema := kindaddon.Schema()
	_, err := activator.Activate(context.Background(), domain.ManagedResourceSchema{
		ResourceType: schema.ResourceType,
		Singular:     schema.Singular,
		Plural:       schema.Plural,
		ProtoFiles:   schema.ProtoFiles,
		SpecMessage:  schema.SpecMessage,
		EntryFile:    schema.EntryFile,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
}

func listServices(t *testing.T, conn *grpc.ClientConn) []string {
	t.Helper()
	client := reflectionpb.NewServerReflectionClient(conn)
	stream, err := client.ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}

	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{
			ListServices: "",
		},
	}); err != nil {
		t.Fatalf("send ListServices: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ListServices: %v", err)
	}
	listResp := resp.GetListServicesResponse()
	if listResp == nil {
		t.Fatalf("expected ListServicesResponse, got %T", resp.GetMessageResponse())
	}

	var names []string
	for _, svc := range listResp.GetService() {
		names = append(names, svc.GetName())
	}
	return names
}

func fileContainingSymbol(t *testing.T, conn *grpc.ClientConn, symbol string) *reflectionpb.ServerReflectionResponse {
	t.Helper()
	client := reflectionpb.NewServerReflectionClient(conn)
	stream, err := client.ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}

	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: symbol,
		},
	}); err != nil {
		t.Fatalf("send FileContainingSymbol: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv FileContainingSymbol: %v", err)
	}
	return resp
}

func TestReflection_ListServicesIncludesDynamic(t *testing.T) {
	mux := managedresource.NewDynamicServiceMux()
	fileReg := managedresource.NewDynamicFileRegistry()
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		FileRegistry: fileReg,
		Deps:         managedresource.Deps{},
	}

	activateKindCluster(t, activator)

	conn := startReflectionServer(t, activator, mux, fileReg)
	services := listServices(t, conn)

	if !slices.Contains(services, kindClusterServiceName) {
		t.Fatalf("expected %q in service list, got: %v", kindClusterServiceName, services)
	}
}

func TestReflection_FileContainingSymbolReturnsDynamicDescriptor(t *testing.T) {
	mux := managedresource.NewDynamicServiceMux()
	fileReg := managedresource.NewDynamicFileRegistry()
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		FileRegistry: fileReg,
		Deps:         managedresource.Deps{},
	}

	activateKindCluster(t, activator)

	conn := startReflectionServer(t, activator, mux, fileReg)
	resp := fileContainingSymbol(t, conn, kindClusterServiceName)

	fdResp := resp.GetFileDescriptorResponse()
	if fdResp == nil {
		errResp := resp.GetErrorResponse()
		if errResp != nil {
			t.Fatalf("FileContainingSymbol returned error: %s", errResp.GetErrorMessage())
		}
		t.Fatalf("expected FileDescriptorResponse, got %T", resp.GetMessageResponse())
	}

	if len(fdResp.GetFileDescriptorProto()) == 0 {
		t.Fatal("expected at least one file descriptor proto in response")
	}
}

func TestReflection_FileContainingSymbolResolvesMessages(t *testing.T) {
	mux := managedresource.NewDynamicServiceMux()
	fileReg := managedresource.NewDynamicFileRegistry()
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		FileRegistry: fileReg,
		Deps:         managedresource.Deps{},
	}

	activateKindCluster(t, activator)

	conn := startReflectionServer(t, activator, mux, fileReg)

	// The resource message should also be resolvable.
	resp := fileContainingSymbol(t, conn, "fleetshift.v1.KindCluster")
	fdResp := resp.GetFileDescriptorResponse()
	if fdResp == nil {
		errResp := resp.GetErrorResponse()
		if errResp != nil {
			t.Fatalf("FileContainingSymbol(KindCluster) error: %s", errResp.GetErrorMessage())
		}
		t.Fatalf("expected FileDescriptorResponse for KindCluster message")
	}
}

func TestReflection_DeactivateRemovesFromReflection(t *testing.T) {
	mux := managedresource.NewDynamicServiceMux()
	fileReg := managedresource.NewDynamicFileRegistry()
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		FileRegistry: fileReg,
		Deps:         managedresource.Deps{},
	}

	activateKindCluster(t, activator)

	conn := startReflectionServer(t, activator, mux, fileReg)

	// Verify it's there first.
	services := listServices(t, conn)
	if !slices.Contains(services, kindClusterServiceName) {
		t.Fatalf("service not listed before deactivate")
	}

	// Deactivate.
	schema := kindaddon.Schema()
	activator.Deactivate(application.SchemaHandle{
		ServiceName: kindClusterServiceName,
		Plural:      schema.Plural,
	})

	// Verify service is removed from listing.
	services = listServices(t, conn)
	if slices.Contains(services, kindClusterServiceName) {
		t.Fatalf("%q should not be in service list after deactivate", kindClusterServiceName)
	}

	// Verify file descriptor is no longer resolvable.
	client := reflectionpb.NewServerReflectionClient(conn)
	stream, err := client.ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: kindClusterServiceName,
		},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil && err != io.EOF {
		t.Fatalf("recv: %v", err)
	}
	if resp != nil && resp.GetFileDescriptorResponse() != nil {
		t.Fatal("expected FileContainingSymbol to fail after deactivate, but got a descriptor")
	}
}
