package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
)

type ClusterServer struct {
	pb.UnimplementedClusterServiceServer
	Clusters *application.ClusterService
}

func (s *ClusterServer) GetClusterConnectionInfo(ctx context.Context, req *pb.GetClusterConnectionInfoRequest) (*pb.GetClusterConnectionInfoResponse, error) {
	if req.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource_id is required")
	}

	endpoint, caCert, err := s.Clusters.GetConnectionInfo(ctx, req.GetResourceId())
	if err != nil {
		return nil, domainError(err)
	}

	return &pb.GetClusterConnectionInfoResponse{
		Endpoint: endpoint,
		CaCert:   caCert,
	}, nil
}

func (s *ClusterServer) GetClusterCredential(ctx context.Context, req *pb.GetClusterCredentialRequest) (*pb.GetClusterCredentialResponse, error) {
	if req.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource_id is required")
	}

	// Extract raw bearer token from context using the existing helper
	callerToken := extractBearerToken(ctx)
	if callerToken == "" {
		return nil, status.Error(codes.Unauthenticated, "bearer token required")
	}

	cred, err := s.Clusters.GetCredential(ctx, req.GetResourceId(), callerToken)
	if err != nil {
		return nil, domainError(err)
	}

	return &pb.GetClusterCredentialResponse{
		Token:      cred.Token,
		Expiration: timestamppb.New(cred.Expiration),
	}, nil
}
