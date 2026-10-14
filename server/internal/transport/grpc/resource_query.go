package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
)

// ResourceQueryServer implements [pb.ResourceQueryServiceServer].
type ResourceQueryServer struct {
	pb.UnimplementedResourceQueryServiceServer
	Queries  *application.ResourceQueryService
	Registry *extensionresource.ActiveResourceRegistry
}

func (s *ResourceQueryServer) QueryResources(ctx context.Context, req *pb.QueryResourcesRequest) (*pb.QueryResourcesResponse, error) {
	page, err := s.Queries.QueryResources(ctx, application.QueryResourcesInput{
		Scope:     req.GetScope(),
		Filter:    req.GetFilter(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
		OrderBy:   req.GetOrderBy(),
	})
	if err != nil {
		return nil, domainError(err)
	}

	out := &pb.QueryResourcesResponse{
		Resources:     make([]*pb.ResourceResult, 0, len(page.Resources)),
		NextPageToken: page.NextPageToken,
	}
	for _, row := range page.Resources {
		result, err := s.projectRow(row)
		if err != nil {
			return nil, err
		}
		out.Resources = append(out.Resources, result)
	}
	return out, nil
}

func (s *ResourceQueryServer) projectRow(row domain.QueryResourceResult) (*pb.ResourceResult, error) {
	if row.Extension == nil {
		return nil, status.Errorf(codes.Internal, "query row %q missing extension view", row.Name)
	}
	if s.Registry == nil {
		return nil, status.Error(codes.FailedPrecondition, "resource type registry is not configured")
	}

	active, ok := s.Registry.Get(row.ResourceType)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition,
			"resource type %q is not activated; cannot project query result body", row.ResourceType)
	}
	ver, ok := active.Versions[active.DefaultVersion]
	if !ok || ver.ExtensionServiceDescriptors == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"resource type %q has no activated service descriptors", row.ResourceType)
	}

	body, err := extensionresource.ViewToStruct(ver.ExtensionServiceDescriptors, ver.Config, *row.Extension)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "project resource body: %v", err)
	}

	return &pb.ResourceResult{
		Name:         row.Name,
		ResourceType: string(row.ResourceType),
		Resource:     body,
	}, nil
}
