package grpc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const authMethodCollection = "authMethods/"

// AuthMethodServer implements [pb.AuthMethodServiceServer].
type AuthMethodServer struct {
	pb.UnimplementedAuthMethodServiceServer
	AuthMethods *application.AuthMethodService
	Authn       *AuthnInterceptor
}

func (s *AuthMethodServer) CreateAuthMethod(ctx context.Context, req *pb.CreateAuthMethodRequest) (*pb.AuthMethod, error) {
	if req.GetAuthMethodId() == "" {
		return nil, status.Error(codes.InvalidArgument, "auth_method_id is required")
	}
	if req.GetAuthMethod() == nil {
		return nil, status.Error(codes.InvalidArgument, "auth_method is required")
	}

	method, err := authMethodFromProto(req.GetAuthMethod())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid auth_method: %v", err)
	}

	result, err := s.AuthMethods.Create(ctx, domain.AuthMethodID(req.GetAuthMethodId()), method)
	if err != nil {
		return nil, domainError(err)
	}

	if s.Authn != nil {
		s.Authn.InvalidateMethodCache()
	}

	return authMethodToProto(result), nil
}

func (s *AuthMethodServer) GetAuthMethod(ctx context.Context, req *pb.GetAuthMethodRequest) (*pb.AuthMethod, error) {
	id, err := parseAuthMethodName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	method, err := s.AuthMethods.Get(ctx, id)
	if err != nil {
		return nil, domainError(err)
	}

	return authMethodToProto(method), nil
}

func authMethodName(id domain.AuthMethodID) string {
	return authMethodCollection + string(id)
}

func parseAuthMethodName(name string) (domain.AuthMethodID, error) {
	id, ok := strings.CutPrefix(name, authMethodCollection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", authMethodCollection)
	}
	return domain.AuthMethodID(id), nil
}

func authMethodFromProto(p *pb.AuthMethod) (domain.AuthMethod, error) {
	m := domain.AuthMethod{}
	switch p.GetType() {
	case pb.AuthMethod_TYPE_OIDC:
		m.Type = domain.AuthMethodTypeOIDC
		oc := p.GetOidcConfig()
		if oc == nil {
			return m, fmt.Errorf("oidc_config is required when type is TYPE_OIDC")
		}
		m.OIDC = &domain.OIDCConfig{
			IssuerURL:             domain.IssuerURL(oc.GetIssuerUrl()),
			Audience:              domain.Audience(oc.GetAudience()),
			KeyEnrollmentAudience: domain.Audience(oc.GetKeyEnrollmentAudience()),
		}
		if rsm := oc.GetRegistrySubjectMapping(); rsm != nil {
			m.OIDC.RegistrySubjectMapping = &domain.RegistrySubjectMapping{
				RegistryID: domain.KeyRegistryID(rsm.GetRegistryId()),
				Expression: rsm.GetExpression(),
			}
		}
	default:
		return m, fmt.Errorf("unsupported auth method type: %v", p.GetType())
	}
	return m, nil
}

func authMethodToProto(m domain.AuthMethod) *pb.AuthMethod {
	out := &pb.AuthMethod{
		Name: authMethodName(m.ID),
	}
	switch m.Type {
	case domain.AuthMethodTypeOIDC:
		out.Type = pb.AuthMethod_TYPE_OIDC
		if m.OIDC != nil {
			oc := &pb.OIDCConfig{
				IssuerUrl:             string(m.OIDC.IssuerURL),
				Audience:              string(m.OIDC.Audience),
				AuthorizationEndpoint: string(m.OIDC.AuthorizationEndpoint),
				TokenEndpoint:         string(m.OIDC.TokenEndpoint),
				JwksUri:               string(m.OIDC.JWKSURI),
				KeyEnrollmentAudience: string(m.OIDC.KeyEnrollmentAudience),
			}
			if m.OIDC.RegistrySubjectMapping != nil {
				oc.RegistrySubjectMapping = &pb.RegistrySubjectMapping{
					RegistryId: string(m.OIDC.RegistrySubjectMapping.RegistryID),
					Expression: m.OIDC.RegistrySubjectMapping.Expression,
				}
			}
			out.OidcConfig = oc
		}
	}
	return out
}
