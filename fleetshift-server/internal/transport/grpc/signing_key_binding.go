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

const signingKeyBindingCollection = "signingKeyBindings/"

// SigningKeyBindingServer implements [pb.SigningKeyBindingServiceServer].
type SigningKeyBindingServer struct {
	pb.UnimplementedSigningKeyBindingServiceServer
	SigningKeys *application.SigningKeyService
}

func (s *SigningKeyBindingServer) CreateSigningKeyBinding(ctx context.Context, req *pb.CreateSigningKeyBindingRequest) (*pb.SigningKeyBinding, error) {
	if req.GetSigningKeyBindingId() == "" {
		return nil, status.Error(codes.InvalidArgument, "signing_key_binding_id is required")
	}

	binding, err := s.SigningKeys.Create(ctx, application.CreateSigningKeyBindingInput{
		ID:                  domain.SigningKeyBindingID(req.GetSigningKeyBindingId()),
		KeyBindingDoc:       req.GetKeyBindingDoc(),
		KeyBindingSignature: req.GetKeyBindingSignature(),
		IdentityToken:       req.GetIdentityToken(),
	})
	if err != nil {
		return nil, domainError(err)
	}

	return signingKeyBindingToProto(binding), nil
}

func signingKeyBindingName(id domain.SigningKeyBindingID) string {
	return signingKeyBindingCollection + string(id)
}

func parseSigningKeyBindingName(name string) (domain.SigningKeyBindingID, error) {
	id, ok := strings.CutPrefix(name, signingKeyBindingCollection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", signingKeyBindingCollection)
	}
	return domain.SigningKeyBindingID(id), nil
}

func signingKeyBindingToProto(b domain.SigningKeyBinding) *pb.SigningKeyBinding {
	out := &pb.SigningKeyBinding{
		Name:                signingKeyBindingName(b.ID),
		Subject:             string(b.Subject),
		Issuer:              string(b.Issuer),
		PublicKeyJwk:        b.PublicKeyJWK,
		Algorithm:           b.Algorithm,
		KeyBindingDoc:       b.KeyBindingDoc,
		KeyBindingSignature: b.KeyBindingSignature,
		IdentityToken:       string(b.IdentityToken),
	}
	if !b.CreatedAt.IsZero() {
		out.CreateTime = timestamppb.New(b.CreatedAt)
	}
	if !b.ExpiresAt.IsZero() {
		out.ExpireTime = timestamppb.New(b.ExpiresAt)
	}
	return out
}
