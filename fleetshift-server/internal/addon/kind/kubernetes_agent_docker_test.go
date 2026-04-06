package kind_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubeaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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
			kubeaddon.WithAttestationVerifier(att.verifier),
			kubeaddon.WithVault(vault),
		)
		obs := newChannelDeliveryObserver()
		signaler := newChannelSignaler(obs)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := agent.Deliver(ctx, k8sTarget, "att-vault-1", manifests, domain.DeliveryAuth{}, att.attestation, signaler)
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
		v := attestation.NewVerifier(map[domain.IssuerURL]attestation.TrustedIssuer{})
		agent := kubeaddon.NewAgent(kubeaddon.WithAttestationVerifier(v))

		bogusAtt := &domain.Attestation{
			Input: domain.SignedInput{
				KeyBinding: domain.SigningKeyBinding{
					FederatedIdentity: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		}

		result, err := agent.Deliver(context.Background(), k8sTarget, "att-bad", nil, domain.DeliveryAuth{}, bogusAtt, &domain.DeliverySignaler{})
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
	attestation *domain.Attestation
	verifier    *attestation.Verifier
}

func buildTestAttestation(t *testing.T, depID domain.DeploymentID, manifests []domain.Manifest) testAttestationBundle {
	t.Helper()

	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))
	signerID := domain.SubjectID("integration-user")
	issuer := provider.IssuerURL()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	pubKeyJWK := ecPubKeyToJWK(t, &privKey.PublicKey)
	ecdhKey, err := privKey.PublicKey.ECDH()
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
	})

	kbDoc, kbSig := buildKBDoc(t, privKey, signerID)

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
			Content: domain.DeploymentContent{
				DeploymentID:      depID,
				ManifestStrategy:  ms,
				PlacementStrategy: ps,
			},
			Sig: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
				PublicKey:      ecdhKey.Bytes(),
				ContentHash:    envelopeHash,
				SignatureBytes: sigBytes,
			},
			KeyBinding: domain.SigningKeyBinding{
				ID:                  "kb-integ",
				FederatedIdentity:   domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
				PublicKeyJWK:        pubKeyJWK,
				Algorithm:           "ES256",
				KeyBindingDoc:       kbDoc,
				KeyBindingSignature: kbSig,
				IdentityToken:       domain.RawToken(identityToken),
			},
			ValidUntil:         validUntil,
			ExpectedGeneration: gen,
		},
		Output: &domain.PutManifests{Manifests: manifests},
	}

	jwksURI := string(issuer) + "/jwks"
	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(jwksURI),
				Audience: "fleetshift-enroll",
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
	)

	return testAttestationBundle{attestation: att, verifier: verifier}
}

func ecPubKeyToJWK(t *testing.T, pub *ecdsa.PublicKey) json.RawMessage {
	t.Helper()
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	xBytes := make([]byte, byteLen)
	yBytes := make([]byte, byteLen)
	copy(xBytes[byteLen-len(pub.X.Bytes()):], pub.X.Bytes())
	copy(yBytes[byteLen-len(pub.Y.Bytes()):], pub.Y.Bytes())
	jwk := map[string]string{
		"kty": "EC",
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(xBytes),
		"y":   base64.RawURLEncoding.EncodeToString(yBytes),
	}
	raw, err := json.Marshal(jwk)
	if err != nil {
		t.Fatalf("marshal JWK: %v", err)
	}
	return raw
}

func buildKBDoc(t *testing.T, privKey *ecdsa.PrivateKey, signerID domain.SubjectID) (doc, sig []byte) {
	t.Helper()
	ecdhKey, err := privKey.PublicKey.ECDH()
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}
	kbDoc := map[string]string{
		"public_key": base64.RawURLEncoding.EncodeToString(ecdhKey.Bytes()),
		"signer_id":  string(signerID),
	}
	docBytes, err := json.Marshal(kbDoc)
	if err != nil {
		t.Fatalf("marshal kb doc: %v", err)
	}
	hash := sha256.Sum256(docBytes)
	sigBytes, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign kb doc: %v", err)
	}
	return docBytes, sigBytes
}
