package grpc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const signerEnrollmentCollection = "signerEnrollments/"

// SignerEnrollmentServer implements [pb.SignerEnrollmentServiceServer].
type SignerEnrollmentServer struct {
	pb.UnimplementedSignerEnrollmentServiceServer
	Enrollments *application.SignerEnrollmentService
}

func (s *SignerEnrollmentServer) CreateSignerEnrollment(ctx context.Context, req *pb.CreateSignerEnrollmentRequest) (*pb.SignerEnrollment, error) {
	if req.GetSignerEnrollmentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "signer_enrollment_id is required")
	}

	enrollment, err := s.Enrollments.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            domain.SignerEnrollmentID(req.GetSignerEnrollmentId()),
		IdentityToken: req.GetIdentityToken(),
	})
	if err != nil {
		return nil, domainError(err)
	}

	return signerEnrollmentToProto(enrollment), nil
}

func signerEnrollmentName(id domain.SignerEnrollmentID) string {
	return signerEnrollmentCollection + string(id)
}

func parseSignerEnrollmentName(name string) (domain.SignerEnrollmentID, error) {
	id, ok := strings.CutPrefix(name, signerEnrollmentCollection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", signerEnrollmentCollection)
	}
	return domain.SignerEnrollmentID(id), nil
}

func signerEnrollmentToProto(e domain.SignerEnrollment) *pb.SignerEnrollment {
	out := &pb.SignerEnrollment{
		Name:            signerEnrollmentName(e.ID),
		Subject:         string(e.Subject),
		Issuer:          string(e.Issuer),
		IdentityToken:   string(e.IdentityToken),
		RegistrySubject: string(e.RegistrySubject),
		RegistryId:      string(e.RegistryID),
	}
	if !e.CreatedAt.IsZero() {
		out.CreateTime = timestamppb.New(e.CreatedAt)
	}
	if !e.ExpiresAt.IsZero() {
		out.ExpireTime = timestamppb.New(e.ExpiresAt)
	}
	return out
}
