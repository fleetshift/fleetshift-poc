package kind_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubeaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// kindClusterFixture is the shared state for a kind cluster created
// once and reused across subtests.
type kindClusterFixture struct {
	deliveryResult domain.DeliveryResult
	adminK8s       *kubernetes.Clientset
}

// setupKindCluster creates a plain kind cluster via the kind delivery
// agent. The agent bootstraps a platform ServiceAccount with
// cluster-admin automatically; the resulting SA token is included in
// [DeliveryResult.ProducedSecrets] and the target's properties contain
// a service_account_token_ref pointing at the vault key.
func setupKindCluster(t *testing.T) *kindClusterFixture {
	t.Helper()

	checker := cluster.NewProvider()
	if _, err := checker.List(); err != nil {
		t.Skipf("container runtime not available: %v", err)
	}

	const clusterName = "fleetshift-k8s-agent"

	t.Cleanup(func() { _ = checker.Delete(clusterName, "") })
	_ = checker.Delete(clusterName, "")

	kindAgent := kindaddon.NewAgent(func(logger log.Logger) kindaddon.ClusterProvider {
		return cluster.NewProvider(cluster.ProviderWithLogger(logger))
	})

	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	target := domain.TargetInfo{ID: "kind-k8s-agent", Type: kindaddon.TargetType}
	manifests := []domain.Manifest{{
		ResourceType: kindaddon.ClusterResourceType,
		Raw:          json.RawMessage(`{"name":"` + clusterName + `"}`),
	}}

	result, err := kindAgent.Deliver(ctx, target, "setup", manifests, domain.DeliveryAuth{}, nil, signaler)
	if err != nil {
		t.Fatalf("kind Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Fatalf("kind Deliver state = %q, want Accepted", result.State)
	}

	var done domain.DeliveryResult
	select {
	case done = <-obs.done:
		if done.State != domain.DeliveryStateDelivered {
			t.Fatalf("kind delivery state = %q, want Delivered (message: %s)", done.State, done.Message)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for kind cluster creation")
	}

	kubeconfig, err := checker.KubeConfig(clusterName, false)
	if err != nil {
		t.Fatalf("KubeConfig: %v", err)
	}
	restCfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(kubeconfig))
	if err != nil {
		t.Fatalf("RESTConfigFromKubeConfig: %v", err)
	}
	adminK8s, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	return &kindClusterFixture{
		deliveryResult: done,
		adminK8s:       adminK8s,
	}
}

// TestKubernetesAgent_RealCluster exercises the kubernetes delivery
// agent against a real kind cluster. A single kind cluster is created
// once and shared across all subtests.
//
// The kind agent automatically provisions a platform ServiceAccount
// with cluster-admin and returns the token as a [domain.ProducedSecret].
// The attested delivery subtests store it in a test vault and configure
// the kubernetes agent with [kubeaddon.WithVault], exercising the full
// vault-backed credential resolution flow.
//
// Requires Docker or Podman (skipped when unavailable or -short).
func TestKubernetesAgent_RealCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real Docker test in short mode")
	}

	f := setupKindCluster(t)

	if len(f.deliveryResult.ProvisionedTargets) != 1 {
		t.Fatalf("expected 1 provisioned target, got %d", len(f.deliveryResult.ProvisionedTargets))
	}
	if len(f.deliveryResult.ProducedSecrets) != 1 {
		t.Fatalf("expected 1 produced secret (SA token), got %d", len(f.deliveryResult.ProducedSecrets))
	}

	pt := f.deliveryResult.ProvisionedTargets[0]
	saTokenRef := pt.Properties["service_account_token_ref"]
	if saTokenRef == "" {
		t.Fatal("provisioned target missing service_account_token_ref property")
	}
	if string(f.deliveryResult.ProducedSecrets[0].Ref) != saTokenRef {
		t.Fatalf("secret ref %q != target property %q",
			f.deliveryResult.ProducedSecrets[0].Ref, saTokenRef)
	}

	// Store the produced secret in a test vault, simulating
	// ProcessDeliveryOutputs.
	vault := &sqlite.VaultStore{DB: sqlite.OpenTestDB(t)}
	for _, s := range f.deliveryResult.ProducedSecrets {
		if err := vault.Put(context.Background(), s.Ref, s.Value); err != nil {
			t.Fatalf("vault Put: %v", err)
		}
	}

	// Build the target info as it would appear after
	// ProcessDeliveryOutputs registers the provisioned target.
	k8sTarget := domain.TargetInfo{
		ID:         pt.ID,
		Type:       pt.Type,
		Name:       pt.Name,
		Properties: pt.Properties,
	}

	// For token-passthrough tests, resolve the SA token directly so we
	// have a bearer token to pass in DeliveryAuth.
	saTokenBytes, err := vault.Get(context.Background(), f.deliveryResult.ProducedSecrets[0].Ref)
	if err != nil {
		t.Fatalf("vault Get: %v", err)
	}
	saToken := string(saTokenBytes)

	t.Run("TokenPassthrough", func(t *testing.T) {
		agent := kubeaddon.NewAgent()
		obs := newChannelDeliveryObserver()
		signaler := newChannelSignaler(obs)

		manifests := []domain.Manifest{{
			ResourceType: kubeaddon.ManifestResourceType,
			Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"tp-test","namespace":"default"},"data":{"mode":"passthrough"}}`),
		}}
		auth := domain.DeliveryAuth{Token: domain.RawToken(saToken)}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := agent.Deliver(ctx, k8sTarget, "tp-1", manifests, auth, nil, signaler)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if result.State != domain.DeliveryStateAccepted {
			t.Fatalf("State = %q, want Accepted", result.State)
		}

		select {
		case done := <-obs.done:
			if done.State != domain.DeliveryStateDelivered {
				t.Fatalf("async State = %q, want Delivered (message: %s)", done.State, done.Message)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for delivery")
		}

		cm, err := f.adminK8s.CoreV1().ConfigMaps("default").Get(ctx, "tp-test", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ConfigMap: %v", err)
		}
		if cm.Data["mode"] != "passthrough" {
			t.Errorf("ConfigMap data = %v, want mode=passthrough", cm.Data)
		}
	})

	t.Run("Idempotent", func(t *testing.T) {
		agent := kubeaddon.NewAgent()
		auth := domain.DeliveryAuth{Token: domain.RawToken(saToken)}
		manifest := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"idempotent-test","namespace":"default"},"data":{"v":"1"}}`)
		manifests := []domain.Manifest{{ResourceType: kubeaddon.ManifestResourceType, Raw: manifest}}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for i := range 2 {
			obs := newChannelDeliveryObserver()
			signaler := newChannelSignaler(obs)
			result, err := agent.Deliver(ctx, k8sTarget, domain.DeliveryID("idem-"+string(rune('0'+i))), manifests, auth, nil, signaler)
			if err != nil {
				t.Fatalf("Deliver[%d]: %v", i, err)
			}
			if result.State != domain.DeliveryStateAccepted {
				t.Fatalf("Deliver[%d] State = %q, want Accepted", i, result.State)
			}
			select {
			case done := <-obs.done:
				if done.State != domain.DeliveryStateDelivered {
					t.Fatalf("Deliver[%d] async = %q (message: %s)", i, done.State, done.Message)
				}
			case <-ctx.Done():
				t.Fatalf("Deliver[%d] timed out", i)
			}
		}
	})

	t.Run("MultipleManifests", func(t *testing.T) {
		agent := kubeaddon.NewAgent()
		obs := newChannelDeliveryObserver()
		signaler := newChannelSignaler(obs)
		auth := domain.DeliveryAuth{Token: domain.RawToken(saToken)}

		manifests := []domain.Manifest{
			{ResourceType: kubeaddon.ManifestResourceType, Raw: json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"multi-a","namespace":"default"},"data":{"idx":"a"}}`)},
			{ResourceType: kubeaddon.ManifestResourceType, Raw: json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"multi-b","namespace":"default"},"data":{"idx":"b"}}`)},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := agent.Deliver(ctx, k8sTarget, "multi-1", manifests, auth, nil, signaler)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if result.State != domain.DeliveryStateAccepted {
			t.Fatalf("State = %q, want Accepted", result.State)
		}

		select {
		case done := <-obs.done:
			if done.State != domain.DeliveryStateDelivered {
				t.Fatalf("async State = %q (message: %s)", done.State, done.Message)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for delivery")
		}

		for _, name := range []string{"multi-a", "multi-b"} {
			cm, err := f.adminK8s.CoreV1().ConfigMaps("default").Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get ConfigMap %q: %v", name, err)
			}
			if cm.Data["idx"] != name[len("multi-"):] {
				t.Errorf("ConfigMap %q data = %v", name, cm.Data)
			}
		}
	})

	t.Run("AttestedDelivery_VaultCredentials", func(t *testing.T) {
		configMap := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"attested-vault-test","namespace":"default"},"data":{"mode":"attested-vault"}}`)
		manifests := []domain.Manifest{{ResourceType: kubeaddon.ManifestResourceType, Raw: configMap}}

		att := buildTestAttestation(t, "attested-dep", manifests)

		agent := kubeaddon.NewAgent(
			kubeaddon.WithKeyResolver(att.keyResolver),
			kubeaddon.WithHTTPClient(att.httpClient),
			kubeaddon.WithVault(vault),
		)
		obs := newChannelDeliveryObserver()
		signaler := newChannelSignaler(obs)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		targetWithTrust := k8sTarget
		targetWithTrust.Properties = copyProps(k8sTarget.Properties)
		targetWithTrust.Properties["trust_bundle"] = att.trustBundleJSON

		result, err := agent.Deliver(ctx, targetWithTrust, "att-vault-1", manifests, domain.DeliveryAuth{}, att.attestation, signaler)
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if result.State != domain.DeliveryStateAccepted {
			t.Fatalf("State = %q, want Accepted", result.State)
		}

		select {
		case done := <-obs.done:
			if done.State != domain.DeliveryStateDelivered {
				t.Fatalf("async State = %q (message: %s)", done.State, done.Message)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for attested delivery")
		}

		cm, err := f.adminK8s.CoreV1().ConfigMaps("default").Get(ctx, "attested-vault-test", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ConfigMap: %v", err)
		}
		if cm.Data["mode"] != "attested-vault" {
			t.Errorf("ConfigMap data = %v, want mode=attested-vault", cm.Data)
		}
	})

	t.Run("AttestedDelivery_VerificationFailure", func(t *testing.T) {
		agent := kubeaddon.NewAgent()

		trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
		targetWithTrust := k8sTarget
		targetWithTrust.Properties = copyProps(k8sTarget.Properties)
		targetWithTrust.Properties["trust_bundle"] = trustBundle

		bogusAtt := &domain.Attestation{
			Input: domain.SignedInput{
				Provenance: domain.Provenance{
					Sig: domain.Signature{
						Signer: domain.FederatedIdentity{
							Issuer: "https://untrusted.example.com",
						},
					},
				},
			},
		}

		result, err := agent.Deliver(context.Background(), targetWithTrust, "att-bad", nil, domain.DeliveryAuth{}, bogusAtt, &domain.DeliverySignaler{})
		if err != nil {
			t.Fatalf("Deliver should not return error: %v", err)
		}
		if result.State != domain.DeliveryStateAuthFailed {
			t.Errorf("State = %q, want AuthFailed", result.State)
		}
	})
}

// ---------------------------------------------------------------------------
// Attestation builder for integration tests
// ---------------------------------------------------------------------------

type testAttestationBundle struct {
	attestation    *domain.Attestation
	keyResolver    *application.KeyResolver
	httpClient     *http.Client
	trustBundleJSON string
}

func buildTestAttestation(t *testing.T, depID domain.DeploymentID, manifests []domain.Manifest) testAttestationBundle {
	t.Helper()

	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))
	signerID := domain.SubjectID("integration-user")
	issuer := provider.IssuerURL()
	registrySubject := domain.RegistrySubject("gh-integration-user")

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
		Extra:    map[string]any{"preferred_username": registrySubject},
	})

	fakeReg := keyregistry.NewFake()
	fakeReg.Register("https://api.github.com", registrySubject, &privKey.PublicKey)

	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: manifests,
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"k8s-test"},
	}
	validUntil := time.Now().Add(24 * time.Hour)
	gen := domain.Generation(1)

	envelope, err := domain.BuildSignedInputEnvelope(depID, ms, ps, validUntil, nil, gen)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelopeHash := domain.HashIntent(envelope)

	hash := sha256.Sum256(envelope)
	sigBytes, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign envelope: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Content: domain.DeploymentContent{
					DeploymentID:      depID,
					ManifestStrategy:  ms,
					PlacementStrategy: ps,
				},
				Sig: domain.Signature{
					Signer:         domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
					ContentHash:    envelopeHash,
					SignatureBytes: sigBytes,
				},
				ValidUntil:         validUntil,
				ExpectedGeneration: gen,
			},
			Signer: domain.SignerAssertion{
				IdentityToken:   domain.RawToken(identityToken),
				RegistryID:      "github.com",
				RegistrySubject: registrySubject,
			},
		},
		Output: &domain.PutManifests{Manifests: manifests},
	}

	keyResolver := &application.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeReg,
		},
	}

	jwksURI := string(issuer) + "/jwks"
	trustBundle := []domain.TrustBundleEntry{{
		IssuerURL:          issuer,
		JWKSURI:            domain.EndpointURL(jwksURI),
		EnrollmentAudience: "fleetshift-enroll",
		RegistrySubjectMapping: &domain.RegistrySubjectMapping{
			RegistryID: "github.com",
			Expression: `claims.preferred_username`,
		},
	}}
	trustJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("marshal trust bundle: %v", err)
	}

	return testAttestationBundle{
		attestation:     att,
		keyResolver:     keyResolver,
		httpClient:      provider.HTTPClient(),
		trustBundleJSON: string(trustJSON),
	}
}

func copyProps(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
