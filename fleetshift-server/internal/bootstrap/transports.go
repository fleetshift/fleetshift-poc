package bootstrap

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

// registerGatewayHandlers registers the static gRPC-gateway handlers against
// the resolved gRPC dial address.
func registerGatewayHandlers(ctx context.Context, gwMux *runtime.ServeMux, grpcDial string, gwOpts []grpc.DialOption) error {
	if err := pb.RegisterDeploymentServiceHandlerFromEndpoint(ctx, gwMux, grpcDial, gwOpts); err != nil {
		return fmt.Errorf("register deployment gateway: %w", err)
	}
	if err := pb.RegisterAuthMethodServiceHandlerFromEndpoint(ctx, gwMux, grpcDial, gwOpts); err != nil {
		return fmt.Errorf("register auth method gateway: %w", err)
	}
	if err := pb.RegisterSignerEnrollmentServiceHandlerFromEndpoint(ctx, gwMux, grpcDial, gwOpts); err != nil {
		return fmt.Errorf("register signer enrollment gateway: %w", err)
	}
	if err := pb.RegisterResourceQueryServiceHandlerFromEndpoint(ctx, gwMux, grpcDial, gwOpts); err != nil {
		return fmt.Errorf("register resource query gateway: %w", err)
	}
	return nil
}

// registerStaticGRPCServices registers the core FleetShift gRPC services and
// composite reflection.
func registerStaticGRPCServices(
	grpcServer *grpc.Server,
	deploymentSvc *application.DeploymentService,
	authMethodSvc *application.AuthMethodService,
	authnInterceptor *transportgrpc.AuthnInterceptor,
	signerEnrollmentSvc *application.SignerEnrollmentService,
	resourceQuerySvc *application.ResourceQueryService,
	activeResources *extensionresource.ActiveResourceRegistry,
	dynamicMux *dynamicapi.DynamicServiceMux,
	fileRegistry *dynamicapi.DynamicFileRegistry,
) {
	pb.RegisterDeploymentServiceServer(grpcServer, &transportgrpc.DeploymentServer{Deployments: deploymentSvc})
	pb.RegisterAuthMethodServiceServer(grpcServer, &transportgrpc.AuthMethodServer{
		AuthMethods: authMethodSvc,
		Authn:       authnInterceptor,
	})
	pb.RegisterSignerEnrollmentServiceServer(grpcServer, &transportgrpc.SignerEnrollmentServer{
		Enrollments: signerEnrollmentSvc,
	})
	pb.RegisterResourceQueryServiceServer(grpcServer, &transportgrpc.ResourceQueryServer{
		Queries:  resourceQuerySvc,
		Registry: activeResources,
	})
	dynamicapi.RegisterCompositeReflection(grpcServer, dynamicMux, fileRegistry)
}
