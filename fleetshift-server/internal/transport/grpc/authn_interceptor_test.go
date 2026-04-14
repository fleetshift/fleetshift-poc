package grpc

import (
	"context"
	"errors"
	"net"
	"testing"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// fakeAuthMethodRepo is an in-memory implementation of AuthMethodRepository.
type fakeAuthMethodRepo struct {
	methods map[domain.AuthMethodID]domain.AuthMethod
}

func newFakeAuthMethodRepo() *fakeAuthMethodRepo {
	return &fakeAuthMethodRepo{methods: make(map[domain.AuthMethodID]domain.AuthMethod)}
}

func (r *fakeAuthMethodRepo) Save(ctx context.Context, method domain.AuthMethod) error {
	r.methods[method.ID] = method
	return nil
}

func (r *fakeAuthMethodRepo) Get(ctx context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	m, ok := r.methods[id]
	if !ok {
		return domain.AuthMethod{}, domain.ErrNotFound
	}
	return m, nil
}

func (r *fakeAuthMethodRepo) List(ctx context.Context) ([]domain.AuthMethod, error) {
	out := make([]domain.AuthMethod, 0, len(r.methods))
	for _, m := range r.methods {
		out = append(out, m)
	}
	return out, nil
}

// fakeOIDCDiscovery returns test metadata for any issuer URL.
type fakeOIDCDiscovery struct {
	meta domain.OIDCMetadata
}

func newFakeOIDCDiscovery() *fakeOIDCDiscovery {
	return &fakeOIDCDiscovery{
		meta: domain.OIDCMetadata{
			Issuer:                "https://issuer.example.com",
			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
			JWKSURI:               "https://issuer.example.com/.well-known/jwks.json",
		},
	}
}

func (f *fakeOIDCDiscovery) FetchMetadata(ctx context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	meta := f.meta
	meta.Issuer = issuerURL
	return meta, nil
}

// fakeOIDCTokenVerifier accepts or rejects tokens based on configuration.
type fakeOIDCTokenVerifier struct {
	acceptToken string // if non-empty, only this token is accepted
	rejectAll   bool   // if true, all tokens are rejected
	claims      domain.SubjectClaims
}

func (f *fakeOIDCTokenVerifier) Verify(ctx context.Context, config domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	if f.rejectAll {
		return domain.SubjectClaims{}, errors.New("token rejected")
	}
	if f.acceptToken != "" && rawToken != f.acceptToken {
		return domain.SubjectClaims{}, errors.New("invalid token")
	}
	return f.claims, nil
}

// authCaptureServer captures the AuthorizationContext from ListDeployments.
type authCaptureServer struct {
	pb.UnimplementedDeploymentServiceServer
	authCtx *application.AuthorizationContext
}

func (s *authCaptureServer) ListDeployments(ctx context.Context, _ *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	s.authCtx = application.AuthFromContext(ctx)
	return &pb.ListDeploymentsResponse{}, nil
}

func setupAuthnTest(t *testing.T, repo *fakeAuthMethodRepo, verifier *fakeOIDCTokenVerifier) (pb.DeploymentServiceClient, *authCaptureServer) {
	t.Helper()

	authMethodSvc := &application.AuthMethodService{
		Methods: repo,
	}

	interceptor := NewAuthnInterceptor(authMethodSvc, verifier, domain.NoOpAuthnObserver{})
	interceptor.cacheTTL = 0 // disable cache for tests

	capture := &authCaptureServer{}
	lis := bufconn.Listen(1 << 20)
	srv := grpclib.NewServer(
		grpclib.UnaryInterceptor(interceptor.Unary()),
	)
	pb.RegisterDeploymentServiceServer(srv, capture)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpclib.NewClient("passthrough:///bufconn",
		grpclib.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewDeploymentServiceClient(conn), capture
}

func TestAuthnInterceptor_NoAuthMethods_Anonymous(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	verifier := &fakeOIDCTokenVerifier{}
	client, capture := setupAuthnTest(t, repo, verifier)

	ctx := context.Background()
	_, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}

	if capture.authCtx == nil {
		t.Fatal("AuthorizationContext is nil")
	}
	if capture.authCtx.Subject != nil {
		t.Errorf("Subject = %v, want nil (anonymous)", capture.authCtx.Subject)
	}
}

func TestAuthnInterceptor_ValidToken_AuthenticatedSubject(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	ctx := context.Background()
	// Save OIDC method directly (bypass Create to avoid discovery in test)
	if err := repo.Save(ctx, domain.AuthMethod{
		ID:   "oidc-1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "test-audience",
			JWKSURI:               "https://issuer.example.com/jwks",

			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	}); err != nil {
		t.Fatalf("Save auth method: %v", err)
	}

	wantClaims := domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-123",
			Issuer:  "https://issuer.example.com",
		},
		Extra: map[string][]string{"email": {"user@example.com"}},
	}
	verifier := &fakeOIDCTokenVerifier{
		acceptToken: "valid-token",
		claims:      wantClaims,
	}
	client, capture := setupAuthnTest(t, repo, verifier)

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer valid-token")
	_, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}

	if capture.authCtx == nil {
		t.Fatal("AuthorizationContext is nil")
	}
	if capture.authCtx.Subject == nil {
		t.Fatal("Subject is nil, want authenticated claims")
	}
	if capture.authCtx.Subject.Subject != wantClaims.Subject {
		t.Errorf("Subject.Subject = %q, want %q", capture.authCtx.Subject.Subject, wantClaims.Subject)
	}
	if capture.authCtx.Subject.Issuer != wantClaims.Issuer {
		t.Errorf("Subject.Issuer = %q, want %q", capture.authCtx.Subject.Issuer, wantClaims.Issuer)
	}
}

func TestAuthnInterceptor_InvalidToken_Unauthenticated(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	ctx := context.Background()
	if err := repo.Save(ctx, domain.AuthMethod{
		ID:   "oidc-1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "test-audience",
			JWKSURI:               "https://issuer.example.com/jwks",

			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	}); err != nil {
		t.Fatalf("Save auth method: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{rejectAll: true}
	client, _ := setupAuthnTest(t, repo, verifier)

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer invalid-token")
	_, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestAuthnInterceptor_SkippedMethod_PassesThrough(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	ctx := context.Background()

	// Configure an OIDC auth method so that normal requests would be validated.
	if err := repo.Save(ctx, domain.AuthMethod{
		ID:   "oidc-1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "test-audience",
			JWKSURI:               "https://issuer.example.com/jwks",
			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	}); err != nil {
		t.Fatalf("Save auth method: %v", err)
	}

	// rejectAll verifier: any token that reaches OIDC validation will fail.
	verifier := &fakeOIDCTokenVerifier{rejectAll: true}

	authMethodSvc := &application.AuthMethodService{
		Methods: repo,
	}

	interceptor := NewAuthnInterceptor(authMethodSvc, verifier, domain.NoOpAuthnObserver{},
		WithSkipMethods("/fleetshift.v1.DeploymentService/ListDeployments"),
	)
	interceptor.cacheTTL = 0

	capture := &authCaptureServer{}
	lis := bufconn.Listen(1 << 20)
	srv := grpclib.NewServer(
		grpclib.UnaryInterceptor(interceptor.Unary()),
	)
	pb.RegisterDeploymentServiceServer(srv, capture)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpclib.NewClient("passthrough:///bufconn",
		grpclib.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := pb.NewDeploymentServiceClient(conn)

	// Send an invalid token to a skipped method — should pass through.
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer totally-invalid-token")
	_, err = client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments on skipped method: expected no error, got %v", err)
	}

	// Skipped methods should NOT attach an AuthorizationContext.
	if capture.authCtx != nil {
		t.Errorf("authCtx = %+v, want nil for skipped method", capture.authCtx)
	}
}

func TestAuthnInterceptor_NoToken_WithMethodsConfigured(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	ctx := context.Background()
	if err := repo.Save(ctx, domain.AuthMethod{
		ID:   "oidc-1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "test-audience",
			JWKSURI:               "https://issuer.example.com/jwks",

			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	}); err != nil {
		t.Fatalf("Save auth method: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{acceptToken: "valid-token"}
	client, capture := setupAuthnTest(t, repo, verifier)

	// No authorization header
	_, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}

	if capture.authCtx == nil {
		t.Fatal("AuthorizationContext is nil")
	}
	if capture.authCtx.Subject != nil {
		t.Errorf("Subject = %v, want nil (anonymous, no token sent)", capture.authCtx.Subject)
	}
}
