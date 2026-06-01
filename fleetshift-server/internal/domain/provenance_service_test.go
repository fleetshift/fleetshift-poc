package domain_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// fakeRegistryClient is a minimal in-memory RegistryClient for tests.
type fakeRegistryClient struct {
	keys map[string][]crypto.PublicKey
}

func (f *fakeRegistryClient) FetchSigningKeys(_ context.Context, endpoint string, subject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	k := endpoint + "/" + string(subject)
	keys, ok := f.keys[k]
	if !ok || len(keys) == 0 {
		return nil, errors.New("no keys")
	}
	return keys, nil
}

// fakeSignerEnrollmentRepo implements SignerEnrollmentRepository for tests.
type fakeSignerEnrollmentRepo struct {
	enrollments []domain.SignerEnrollment
}

func (r *fakeSignerEnrollmentRepo) Create(_ context.Context, e domain.SignerEnrollment) error {
	r.enrollments = append(r.enrollments, e)
	return nil
}

func (r *fakeSignerEnrollmentRepo) Get(_ context.Context, id domain.SignerEnrollmentID) (domain.SignerEnrollment, error) {
	for _, e := range r.enrollments {
		if e.ID == id {
			return e, nil
		}
	}
	return domain.SignerEnrollment{}, domain.ErrNotFound
}

func (r *fakeSignerEnrollmentRepo) ListBySubject(_ context.Context, identity domain.FederatedIdentity) ([]domain.SignerEnrollment, error) {
	var out []domain.SignerEnrollment
	for _, e := range r.enrollments {
		if e.FederatedIdentity == identity {
			out = append(out, e)
		}
	}
	return out, nil
}

func TestProvenanceService_BuildDeploymentProvenance(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-user")
	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	fakeClient := &fakeRegistryClient{
		keys: map[string][]crypto.PublicKey{
			"https://api.github.com/" + string(registrySubject): {&privKey.PublicKey},
		},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: fakeClient,
			},
		},
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-1",
			FederatedIdentity: identity,
			RegistrySubject:   registrySubject,
			RegistryID:        "github.com",
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}
	validUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	gen := domain.Generation(1)

	envelope, err := domain.BuildSignedInputEnvelope("dep-1", ms, ps, validUntil, nil, gen)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	hash := sha256.Sum256(envelope)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	prov, err := svc.BuildDeploymentProvenance(
		context.Background(), enrollments, caller,
		"dep-1", ms, ps, gen, sig, validUntil,
	)
	if err != nil {
		t.Fatalf("BuildDeploymentProvenance: %v", err)
	}

	if prov.Sig.Signer != identity {
		t.Errorf("signer = %v, want %v", prov.Sig.Signer, identity)
	}
	if prov.ExpectedGeneration != gen {
		t.Errorf("generation = %d, want %d", prov.ExpectedGeneration, gen)
	}
}

func TestProvenanceService_BuildDeploymentProvenance_BadSignature(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-user")
	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	fakeClient := &fakeRegistryClient{
		keys: map[string][]crypto.PublicKey{
			"https://api.github.com/" + string(registrySubject): {&privKey.PublicKey},
		},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: fakeClient,
			},
		},
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-1",
			FederatedIdentity: identity,
			RegistrySubject:   registrySubject,
			RegistryID:        "github.com",
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}
	validUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	gen := domain.Generation(1)

	_, err = svc.BuildDeploymentProvenance(
		context.Background(), enrollments, caller,
		"dep-1", ms, ps, gen, []byte("bad-sig"), validUntil,
	)
	if err == nil {
		t.Fatal("expected error for bad signature")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestProvenanceService_BuildManagedResourceProvenance(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-user")
	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	fakeClient := &fakeRegistryClient{
		keys: map[string][]crypto.PublicKey{
			"https://api.github.com/" + string(registrySubject): {&privKey.PublicKey},
		},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: fakeClient,
			},
		},
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-1",
			FederatedIdentity: identity,
			RegistrySubject:   registrySubject,
			RegistryID:        "github.com",
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	resourceType := domain.ResourceType("api.kind.cluster")
	resourceName := domain.ResourceName("test-cluster")
	spec := json.RawMessage(`{"replicas":3}`)
	validUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	gen := domain.Generation(2)

	envelope, err := domain.BuildManagedResourceEnvelope(
		resourceType, resourceName, spec, validUntil, nil, gen,
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	hash := sha256.Sum256(envelope)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	prov, err := svc.BuildManagedResourceProvenance(
		context.Background(), enrollments, caller,
		resourceType, resourceName, spec, gen, sig, validUntil,
	)
	if err != nil {
		t.Fatalf("BuildManagedResourceProvenance: %v", err)
	}

	if prov.Sig.Signer != identity {
		t.Errorf("signer = %v, want %v", prov.Sig.Signer, identity)
	}
	if prov.ExpectedGeneration != gen {
		t.Errorf("generation = %d, want %d", prov.ExpectedGeneration, gen)
	}
}

func TestProvenanceService_VerifySignature(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-user")
	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	fakeClient := &fakeRegistryClient{
		keys: map[string][]crypto.PublicKey{
			"https://api.github.com/" + string(registrySubject): {&privKey.PublicKey},
		},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: fakeClient,
			},
		},
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-1",
			FederatedIdentity: identity,
			RegistrySubject:   registrySubject,
			RegistryID:        "github.com",
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	doc := []byte("test-document")
	hash := sha256.Sum256(doc)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if err := svc.VerifySignature(context.Background(), enrollments, caller, doc, sig); err != nil {
		t.Fatalf("VerifySignature: %v", err)
	}
}

func TestProvenanceService_VerifySignature_InvalidSig(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-user")
	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	fakeClient := &fakeRegistryClient{
		keys: map[string][]crypto.PublicKey{
			"https://api.github.com/" + string(registrySubject): {&privKey.PublicKey},
		},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: fakeClient,
			},
		},
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-1",
			FederatedIdentity: identity,
			RegistrySubject:   registrySubject,
			RegistryID:        "github.com",
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	err = svc.VerifySignature(context.Background(), enrollments, caller, []byte("doc"), []byte("bad"))
	if err == nil {
		t.Fatal("expected error for invalid signature")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestProvenanceService_OIDC_KeyFromClaim(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	derBytes, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	base64Key := base64.StdEncoding.EncodeToString(derBytes)

	identity := domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	}

	// Build a fake JWT whose claims contain the public key.
	claimsJSON, _ := json.Marshal(map[string]any{
		"sub":        "user-1",
		"public_key": base64Key,
	})
	fakeJWT := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`)) +
		"." + base64.RawURLEncoding.EncodeToString(claimsJSON) +
		"." + base64.RawURLEncoding.EncodeToString([]byte("fake-sig"))

	authMethods := &provenanceTestAuthMethodRepo{
		methods: []domain.AuthMethod{{
			ID:   "oidc-1",
			Type: domain.AuthMethodTypeOIDC,
			OIDC: &domain.OIDCConfig{
				PublicKeyClaimExpression: `claims.public_key`,
			},
		}},
	}

	svc := &domain.ProvenanceService{
		KeyResolver: &domain.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients:    map[domain.KeyRegistryType]domain.RegistryClient{},
		},
		AuthMethods: authMethods,
	}

	enrollments := &fakeSignerEnrollmentRepo{
		enrollments: []domain.SignerEnrollment{{
			ID:                "enroll-oidc",
			FederatedIdentity: identity,
			RegistryID:        "oidc",
			IdentityToken:     domain.RawToken(fakeJWT),
		}},
	}

	caller := &domain.SubjectClaims{FederatedIdentity: identity}
	doc := []byte("test-document-oidc")
	hash := sha256.Sum256(doc)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if err := svc.VerifySignature(context.Background(), enrollments, caller, doc, sig); err != nil {
		t.Fatalf("VerifySignature (OIDC path): %v", err)
	}
}

// provenanceTestAuthMethodRepo implements domain.AuthMethodRepository for
// provenance service tests.
type provenanceTestAuthMethodRepo struct {
	methods []domain.AuthMethod
}

func (r *provenanceTestAuthMethodRepo) Save(_ context.Context, m domain.AuthMethod) error {
	r.methods = append(r.methods, m)
	return nil
}

func (r *provenanceTestAuthMethodRepo) Get(_ context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	for _, m := range r.methods {
		if m.ID == id {
			return m, nil
		}
	}
	return domain.AuthMethod{}, domain.ErrNotFound
}

func (r *provenanceTestAuthMethodRepo) List(_ context.Context) ([]domain.AuthMethod, error) {
	return r.methods, nil
}
