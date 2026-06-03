package gcphcp

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func TestBuildGuestBootstrapRESTConfig_UsesSystemTrust(t *testing.T) {
	cfg := buildGuestBootstrapRESTConfig("https://guest.example:6443", "broker-token", nil)

	if cfg.Host != "https://guest.example:6443" {
		t.Fatalf("Host = %q, want %q", cfg.Host, "https://guest.example:6443")
	}
	if cfg.BearerToken != "broker-token" {
		t.Fatalf("BearerToken = %q, want broker-token", cfg.BearerToken)
	}
	if cfg.TLSClientConfig.Insecure {
		t.Fatal("expected verified TLS, got insecure config")
	}
	if len(cfg.TLSClientConfig.CAData) != 0 {
		t.Fatalf("CAData = %q, want empty system-trust config", string(cfg.TLSClientConfig.CAData))
	}
}

func TestBuildGuestBootstrapRESTConfig_WithCACert(t *testing.T) {
	caCert := []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
	cfg := buildGuestBootstrapRESTConfig("https://guest.example:6443", "broker-token", caCert)

	if cfg.Host != "https://guest.example:6443" {
		t.Fatalf("Host = %q, want %q", cfg.Host, "https://guest.example:6443")
	}
	if cfg.BearerToken != "broker-token" {
		t.Fatalf("BearerToken = %q, want broker-token", cfg.BearerToken)
	}
	if cfg.TLSClientConfig.Insecure {
		t.Fatal("expected verified TLS, got insecure config")
	}
	if string(cfg.TLSClientConfig.CAData) != string(caCert) {
		t.Fatalf("CAData = %q, want test CA cert", string(cfg.TLSClientConfig.CAData))
	}
}

func TestBuildGuestBootstrapRESTConfig_WithNilCACert(t *testing.T) {
	cfg := buildGuestBootstrapRESTConfig("https://guest.example:6443", "broker-token", nil)

	if len(cfg.TLSClientConfig.CAData) != 0 {
		t.Fatalf("CAData = %q, want empty for nil CA", string(cfg.TLSClientConfig.CAData))
	}
}

func TestRequestPlatformSAToken_CreatesShortLivedToken(t *testing.T) {
	client := fake.NewSimpleClientset()
	createCalls := 0
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		createCalls++

		createAction, ok := action.(ktesting.CreateAction)
		if !ok {
			t.Fatalf("expected CreateAction, got %T", action)
		}
		req, ok := createAction.GetObject().(*authv1.TokenRequest)
		if !ok {
			t.Fatalf("expected TokenRequest, got %T", createAction.GetObject())
		}
		if req.Spec.ExpirationSeconds == nil || *req.Spec.ExpirationSeconds != defaultPlatformTokenExpirySeconds {
			t.Fatalf("ExpirationSeconds = %v, want %d", req.Spec.ExpirationSeconds, defaultPlatformTokenExpirySeconds)
		}

		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{
				Token: "short-lived-token",
			},
		}, nil
	})

	ref, token, err := requestPlatformSAToken(context.Background(), client, "guest-target")
	if err != nil {
		t.Fatalf("requestPlatformSAToken() error = %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("token create calls = %d, want 1", createCalls)
	}
	if ref != "targets/guest-target/sa-token" {
		t.Fatalf("ref = %q, want %q", ref, "targets/guest-target/sa-token")
	}
	if string(token) != "short-lived-token" {
		t.Fatalf("token = %q, want short-lived-token", string(token))
	}
}

func TestBootstrapGuestCluster_UsesVerifiedTLSAndTokenRequest(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() {
		newKubernetesClientForConfig = origNewClient
	}()

	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{
				Token: "short-lived-token",
			},
		}, nil
	})

	var captured *rest.Config
	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		captured = rest.CopyConfig(cfg)
		return client, nil
	}

	result, err := BootstrapGuestCluster(
		context.Background(),
		"https://guest.example:6443",
		"broker-token",
		"guest-target",
	)
	if err != nil {
		t.Fatalf("BootstrapGuestCluster() error = %v", err)
	}

	if captured == nil {
		t.Fatal("expected kubernetes client config to be captured")
	}
	if captured.TLSClientConfig.Insecure {
		t.Fatal("expected verified TLS, got insecure config")
	}
	if len(captured.TLSClientConfig.CAData) != 0 {
		t.Fatalf("captured CAData = %q, want empty system-trust config", string(captured.TLSClientConfig.CAData))
	}
	if result.SATokenRef != "targets/guest-target/sa-token" {
		t.Fatalf("SATokenRef = %q, want %q", result.SATokenRef, "targets/guest-target/sa-token")
	}
	if string(result.SAToken) != "short-lived-token" {
		t.Fatalf("SAToken = %q, want %q", string(result.SAToken), "short-lived-token")
	}
	if len(result.CACert) != 0 {
		t.Fatalf("CACert = %q, want empty system-trust output", string(result.CACert))
	}
}

func TestCreatePlatformRBAC_ReconcilesExistingSubjects(t *testing.T) {
	client := fake.NewSimpleClientset(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName + "-cluster-admin"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      "wrong-service-account",
			Namespace: platformSANamespace,
		}},
	})

	if err := createPlatformRBAC(context.Background(), client); err != nil {
		t.Fatalf("createPlatformRBAC() error = %v", err)
	}

	got, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), platformSAName+"-cluster-admin", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ClusterRoleBinding: %v", err)
	}

	wantSubjects := []rbacv1.Subject{{
		Kind:      "ServiceAccount",
		Name:      platformSAName,
		Namespace: platformSANamespace,
	}}
	if !reflect.DeepEqual(got.Subjects, wantSubjects) {
		t.Fatalf("subjects = %#v, want %#v", got.Subjects, wantSubjects)
	}
}

func TestCreatePlatformRBAC_RecreatesExistingBindingWhenRoleRefDiffers(t *testing.T) {
	client := fake.NewSimpleClientset(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName + "-cluster-admin"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "view",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      platformSAName,
			Namespace: platformSANamespace,
		}},
	})

	if err := createPlatformRBAC(context.Background(), client); err != nil {
		t.Fatalf("createPlatformRBAC() error = %v", err)
	}

	got, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), platformSAName+"-cluster-admin", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ClusterRoleBinding: %v", err)
	}

	if got.RoleRef.Name != "cluster-admin" {
		t.Fatalf("roleRef.name = %q, want cluster-admin", got.RoleRef.Name)
	}
}

func TestIsCertVerificationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "x509 UnknownAuthorityError",
			err:  x509.UnknownAuthorityError{},
			want: true,
		},
		{
			name: "wrapped x509 UnknownAuthorityError",
			err:  fmt.Errorf("connect failed: %w", &x509.UnknownAuthorityError{}),
			want: true,
		},
		{
			name: "generic network error",
			err:  fmt.Errorf("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "x509 CertificateInvalidError (not unknown authority)",
			err:  x509.CertificateInvalidError{Reason: x509.Expired},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCertVerificationError(tt.err); got != tt.want {
				t.Fatalf("isCertVerificationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProbeWithSystemTrust_SuccessReturnsClientAndNilLeafDER(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	client := fake.NewSimpleClientset()
	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		return client, nil
	}

	gotClient, leafDER, err := probeWithSystemTrust("https://guest.example:6443", "broker-token")
	if err != nil {
		t.Fatalf("probeWithSystemTrust() error = %v", err)
	}
	if gotClient == nil {
		t.Fatal("expected non-nil client on success")
	}
	if leafDER != nil {
		t.Fatalf("leafDER = %v, want nil on success", leafDER)
	}
}

func TestProbeWithSystemTrust_NonX509ErrorReturnsNilLeafDER(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		return nil, fmt.Errorf("connection refused")
	}

	gotClient, leafDER, err := probeWithSystemTrust("https://guest.example:6443", "broker-token")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if gotClient != nil {
		t.Fatal("expected nil client on error")
	}
	if leafDER != nil {
		t.Fatalf("leafDER = %v, want nil for non-x509 error", leafDER)
	}
}

func generateTestCA(t *testing.T) (*x509.Certificate, crypto.Signer, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-root-ca", OrganizationalUnit: []string{"openshift"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key, pemBytes
}

func generateTestLeaf(t *testing.T, ca *x509.Certificate, caKey crypto.Signer) (*x509.Certificate, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kubernetes", Organization: []string{"kubernetes"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return cert, der
}

func TestExtractCAFromGuest_Success(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	ca, caKey, caPEM := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)
	fingerprint := leafFingerprint(leafDER)

	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-root-ca.crt",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"ca.crt": string(caPEM),
		},
	})

	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		if !cfg.TLSClientConfig.Insecure {
			t.Fatal("expected insecure config for CA extraction")
		}
		return client, nil
	}

	gotCA, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", fingerprint, leafDER)
	if err != nil {
		t.Fatalf("extractCAFromGuest() error = %v", err)
	}
	if string(gotCA) != string(caPEM) {
		t.Fatalf("extracted CA does not match expected CA PEM")
	}
}

func TestExtractCAFromGuest_ConfigMapMissing(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	client := fake.NewSimpleClientset()
	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		return client, nil
	}

	fingerprint := leafFingerprint([]byte("dummy-leaf"))
	_, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", fingerprint, []byte("dummy-leaf"))
	if err == nil {
		t.Fatal("expected error for missing ConfigMap")
	}
}

func TestExtractCAFromGuest_CADoesNotSignLeaf(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	ca, caKey, _ := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)
	fingerprint := leafFingerprint(leafDER)

	// Generate a second, unrelated CA
	_, _, wrongCAPEM := generateTestCA(t)

	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-root-ca.crt",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"ca.crt": string(wrongCAPEM),
		},
	})

	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		return client, nil
	}

	_, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", fingerprint, leafDER)
	if err == nil {
		t.Fatal("expected error when CA does not sign the leaf cert")
	}
}
