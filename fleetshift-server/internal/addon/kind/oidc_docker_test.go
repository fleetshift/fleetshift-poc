//go:build integration

package kind_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubeaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// containerHostAddr returns the hostname (or IP) that kind containers
// use to reach the test host. By default this is "host.docker.internal",
// which both Docker Desktop and Podman resolve to the host gateway.
//
// Set KIND_HOST_ADDR to override when the default doesn't work (e.g.,
// older Docker Engine on Linux, or a dual-runtime environment).
func containerHostAddr(t *testing.T) (host string, extraSANIPs []net.IP) {
	t.Helper()
	host = os.Getenv("KIND_HOST_ADDR")
	if host == "" {
		host = "host.docker.internal"
	}
	if ip := net.ParseIP(host); ip != nil {
		extraSANIPs = []net.IP{ip}
	}
	return host, extraSANIPs
}

// oidcClusterResult holds the outputs of createOIDCCluster.
type oidcClusterResult struct {
	ClusterName string
	IDP         *oidctest.Provider
	IssuerURL   domain.IssuerURL
	Kubeconfig  string
}

// createOIDCCluster creates a kind cluster with OIDC trust derived from
// the caller's identity. It starts a fake OIDC provider, delivers the
// cluster via the kind agent, and returns the kubeconfig and provider.
func createOIDCCluster(t *testing.T, clusterName string, auth domain.DeliveryAuth) oidcClusterResult {
	t.Helper()

	checker := cluster.NewProvider()
	if _, err := checker.List(); err != nil {
		t.Skipf("container runtime not available: %v", err)
	}

	t.Cleanup(func() { _ = checker.Delete(clusterName, "") })
	_ = checker.Delete(clusterName, "")

	hostAddr, extraSANIPs := containerHostAddr(t)

	idp := oidctest.Start(t,
		oidctest.WithListenAddress("0.0.0.0:0"),
		oidctest.WithAudience(string(auth.Audience[0])),
		oidctest.WithExtraSANIPs(extraSANIPs...),
	)
	dockerIssuer := domain.IssuerURL(fmt.Sprintf("https://%s:%s", hostAddr, idp.Port()))
	idp.SetIssuerURL(dockerIssuer)

	auth.Caller.Issuer = dockerIssuer

	reporter := newChannelReporter()
	kindAgent := kindaddon.NewAgent(reporter,
		func(logger log.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindaddon.WithTempDir(t.TempDir()),
		kindaddon.WithOIDCCABundle(idp.CACertPEM()),
	)

	spec := kindaddon.ClusterSpec{
		Name: clusterName,
	}
	specBytes, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}

	target := domain.TargetInfo{ID: "oidc-kind", Type: kindaddon.TargetType, Name: "OIDC Kind"}
	manifests := []domain.Manifest{{
		ResourceType: kindaddon.ClusterResourceType,
		Raw:          json.RawMessage(specBytes),
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err = kindAgent.Deliver(ctx, target, "d1:oidc-kind", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	select {
	case doneResult := <-reporter.done:
		if doneResult.State != domain.DeliveryStateDelivered {
			t.Fatalf("delivery State = %q, want %q (message: %s)", doneResult.State, domain.DeliveryStateDelivered, doneResult.Message)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery to complete")
	}

	kcStr, err := checker.KubeConfig(clusterName, false)
	if err != nil {
		t.Fatalf("KubeConfig: %v", err)
	}

	return oidcClusterResult{
		ClusterName: clusterName,
		IDP:         idp,
		IssuerURL:   dockerIssuer,
		Kubeconfig:  kcStr,
	}
}

// oidcK8sClient builds a K8s client using the kubeconfig and a bearer token.
func oidcK8sClient(t *testing.T, kubeconfig, token string) *kubernetes.Clientset {
	t.Helper()
	restCfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(kubeconfig))
	if err != nil {
		t.Fatalf("parse kubeconfig: %v", err)
	}
	restCfg.BearerToken = token
	restCfg.Username = ""
	restCfg.Password = ""
	restCfg.CertData = nil
	restCfg.KeyData = nil
	restCfg.CertFile = ""
	restCfg.KeyFile = ""

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}
	return client
}

// TestKindAddon_OIDCIntegration creates a single kind cluster with OIDC
// authentication and runs subtests that verify JWT auth, RBAC bootstrap,
// and token-passthrough delivery against it. Sharing one cluster avoids
// paying the ~20s cluster creation cost three times.
//
// Requires Docker or Podman (skipped when unavailable).
// Requires -tags integration.
func TestKindAddon_OIDCIntegration(t *testing.T) {
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{Subject: "alice"},
		},
		Audience: []domain.Audience{"fleetshift"},
	}
	res := createOIDCCluster(t, "fleetshift-oidc-test", auth)

	t.Run("Auth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		token := res.IDP.IssueToken(t, oidctest.TokenClaims{
			Subject: "alice",
			Groups:  []string{"developers"},
		})

		client := oidcK8sClient(t, res.Kubeconfig, token)

		ssr, err := client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("SelfSubjectReview: %v", err)
		}

		wantUsername := string(res.IssuerURL) + "#alice"
		if ssr.Status.UserInfo.Username != wantUsername {
			t.Errorf("Username = %q, want %q", ssr.Status.UserInfo.Username, wantUsername)
		}

		foundGroup := false
		for _, g := range ssr.Status.UserInfo.Groups {
			if g == "developers" {
				foundGroup = true
				break
			}
		}
		if !foundGroup {
			t.Errorf("Groups = %v, expected to contain %q", ssr.Status.UserInfo.Groups, "developers")
		}

		expiredToken := res.IDP.IssueToken(t, oidctest.TokenClaims{
			Subject: "alice",
			Expiry:  -time.Hour,
		})
		expiredClient := oidcK8sClient(t, res.Kubeconfig, expiredToken)
		_, err = expiredClient.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
		if err == nil {
			t.Error("expected error for expired token, got nil")
		}
	})

	t.Run("RBAC", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		aliceToken := res.IDP.IssueToken(t, oidctest.TokenClaims{Subject: "alice"})
		aliceClient := oidcK8sClient(t, res.Kubeconfig, aliceToken)

		nsList, err := aliceClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("alice Namespaces().List: %v", err)
		}
		if len(nsList.Items) == 0 {
			t.Error("expected at least one namespace")
		}

		bobToken := res.IDP.IssueToken(t, oidctest.TokenClaims{Subject: "bob"})
		bobClient := oidcK8sClient(t, res.Kubeconfig, bobToken)

		_, err = bobClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err == nil {
			t.Error("expected bob to be forbidden, got nil error")
		}
	})

	t.Run("TokenPassthrough", func(t *testing.T) {
		apiServer, caCert, err := kindaddon.ExtractClusterConnInfo([]byte(res.Kubeconfig))
		if err != nil {
			t.Fatalf("ExtractClusterConnInfo: %v", err)
		}

		k8sTarget := domain.TargetInfo{
			ID:   domain.TargetID("k8s-" + res.ClusterName),
			Type: kubeaddon.TargetType,
			Name: res.ClusterName,
			Properties: map[string]string{
				"api_server": apiServer,
				"ca_cert":    string(caCert),
			},
		}

		configMapManifest := json.RawMessage(`{
			"apiVersion": "v1",
			"kind": "ConfigMap",
			"metadata": {
				"name": "passthrough-test",
				"namespace": "default"
			},
			"data": {
				"hello": "world"
			}
		}`)
		manifests := []domain.Manifest{{
			ResourceType: kubeaddon.ManifestResourceType,
			Raw:          configMapManifest,
		}}

		kubeReporter := newChannelReporter()
		kubeAgent := kubeaddon.NewAgent(kubeReporter)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		aliceToken := res.IDP.IssueToken(t, oidctest.TokenClaims{Subject: "alice"})
		aliceAuth := domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{
				FederatedIdentity: domain.FederatedIdentity{
					Subject: "alice",
					Issuer:  res.IssuerURL,
				},
			},
			Audience: []domain.Audience{"fleetshift"},
			Token:    domain.RawToken(aliceToken),
		}

		err = kubeAgent.Deliver(ctx, k8sTarget, "d-pass:k8s-test", manifests, aliceAuth, nil, 1)
		if err != nil {
			t.Fatalf("Deliver (alice): %v", err)
		}

		select {
		case doneResult := <-kubeReporter.done:
			if doneResult.State != domain.DeliveryStateDelivered {
				t.Fatalf("alice delivery State = %q, want %q (message: %s)", doneResult.State, domain.DeliveryStateDelivered, doneResult.Message)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for alice delivery")
		}

		adminClient := oidcK8sClient(t, res.Kubeconfig, aliceToken)
		cm, err := adminClient.CoreV1().ConfigMaps("default").Get(ctx, "passthrough-test", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ConfigMap: %v", err)
		}
		if cm.Data["hello"] != "world" {
			t.Errorf("ConfigMap data = %v, want hello=world", cm.Data)
		}

		bobToken := res.IDP.IssueToken(t, oidctest.TokenClaims{Subject: "bob"})
		bobAuth := domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{
				FederatedIdentity: domain.FederatedIdentity{
					Subject: "bob",
					Issuer:  res.IssuerURL,
				},
			},
			Audience: []domain.Audience{"fleetshift"},
			Token:    domain.RawToken(bobToken),
		}

		bobManifest := json.RawMessage(`{
			"apiVersion": "v1",
			"kind": "ConfigMap",
			"metadata": {
				"name": "bob-test",
				"namespace": "default"
			},
			"data": {
				"should": "fail"
			}
		}`)

		err = kubeAgent.Deliver(ctx, k8sTarget, "d-bob:k8s-test", []domain.Manifest{{
			ResourceType: kubeaddon.ManifestResourceType,
			Raw:          bobManifest,
		}}, bobAuth, nil, 1)
		if err != nil {
			t.Fatalf("Deliver (bob): %v", err)
		}

		select {
		case doneResult := <-kubeReporter.done:
			if doneResult.State != domain.DeliveryStateAuthFailed {
				t.Fatalf("bob delivery State = %q, want %q (message: %s)", doneResult.State, domain.DeliveryStateAuthFailed, doneResult.Message)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for bob delivery")
		}
	})
}

// TestKindAddon_ManagedResource_OIDCAuth exercises the managed resource
// path with authenticated delivery against a real Docker daemon. It:
//  1. Starts a fake OIDC provider reachable from inside Docker.
//  2. Registers the kind managed resource type.
//  3. Creates a managed resource with an authenticated caller context.
//  4. Verifies the fulfillment reaches Active (including RBAC bootstrap).
//  5. Verifies the cluster is provisioned and the OIDC-issued JWT is
//     accepted by the K8s API server.
//
// This test catches regressions where DeliveryAuth is not threaded
// through the managed resource path. Skipped when Docker is not
// available. Requires -tags integration.
func TestKindAddon_ManagedResource_OIDCAuth(t *testing.T) {
	checker := cluster.NewProvider()

	if _, err := checker.List(); err != nil {
		t.Skipf("Docker not available: %v", err)
	}

	const clusterName = "fleetshift-mr-oidc"

	t.Cleanup(func() {
		_ = checker.Delete(clusterName, "")
	})
	_ = checker.Delete(clusterName, "")

	// --- Start fake OIDC provider ---
	hostAddr, extraSANIPs := containerHostAddr(t)
	idp := oidctest.Start(t,
		oidctest.WithListenAddress("0.0.0.0:0"),
		oidctest.WithAudience("fleetshift"),
		oidctest.WithExtraSANIPs(extraSANIPs...),
	)
	dockerIssuer := domain.IssuerURL(fmt.Sprintf("https://%s:%s", hostAddr, idp.Port()))
	idp.SetIssuerURL(dockerIssuer)

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}
	mrReporter := buildReporter(store, reg)

	// --- Wire agent with OIDC CA trust ---
	kindAgent := kindaddon.NewAgent(mrReporter,
		func(logger log.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindaddon.WithTempDir(t.TempDir()),
		kindaddon.WithOIDCCABundle(idp.CACertPEM()),
	)
	router := delivery.NewRoutingDeliveryService()
	router.Register(kindaddon.TargetType, kindAgent)

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:           store,
		Delivery:        router,
		Strategies:      domain.StrategyFactory{Store: store},
		CleanupSignaler: reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createMRSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createMRWf, err := reg.RegisterCreateManagedResource(createMRSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	typeSvc := &application.ManagedResourceTypeService{Store: store}
	resourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createMRWf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register target with accepted resource types.
	{
		tx, _ := store.Begin(ctx)
		_ = tx.Targets().Create(ctx, domain.TargetInfo{
			ID:                    "kind-mr-oidc",
			Type:                  kindaddon.TargetType,
			Name:                  "Docker Kind Provider (MR OIDC)",
			AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType},
		})
		_ = tx.Commit()
	}

	// Register managed resource type.
	_, err = typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType: kindaddon.ClusterResourceType,
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "kind-mr-oidc"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "kind-addon", Issuer: "https://kind.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		},
	})
	if err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	// Create managed resource with authenticated caller context.
	spec := json.RawMessage(`{"name":"` + clusterName + `"}`)

	authCtx := application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  dockerIssuer,
			},
		},
		Audience: []domain.Audience{"fleetshift"},
		Token:    "platform-token",
	})

	view, err := resourceSvc.Create(authCtx, application.CreateManagedResourceInput{
		ResourceType: kindaddon.ClusterResourceType,
		Name:         domain.ResourceName(clusterName),
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("Create managed resource: %v", err)
	}

	// Wait for fulfillment to reach Active (includes RBAC bootstrap).
	awaitFulfillment(ctx, t, store, view.Fulfillment.ID, domain.FulfillmentStateActive)

	// Verify OIDC auth works: issue a JWT and authenticate against the cluster.
	token := idp.IssueToken(t, oidctest.TokenClaims{
		Subject: "alice",
		Groups:  []string{"developers"},
	})

	kc, err := checker.KubeConfig(clusterName, false)
	if err != nil {
		t.Fatalf("KubeConfig: %v", err)
	}

	client := oidcK8sClient(t, kc, token)
	review, err := client.AuthenticationV1().TokenReviews().Create(ctx,
		&authv1.TokenReview{
			Spec: authv1.TokenReviewSpec{Token: token},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("TokenReview: %v", err)
	}
	if !review.Status.Authenticated {
		t.Fatalf("expected token to be authenticated; got error: %s", review.Status.Error)
	}
	wantUser := string(dockerIssuer) + "#alice"
	if review.Status.User.Username != wantUser {
		t.Errorf("Username = %q, want %q", review.Status.User.Username, wantUser)
	}
}
