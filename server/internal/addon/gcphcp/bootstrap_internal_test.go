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
	"strings"
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

func TestBuildGuestBootstrapRESTConfig(t *testing.T) {
	tests := []struct {
		name       string
		caCert     []byte
		wantCAData []byte
	}{
		{
			name:       "nil CA uses system trust",
			caCert:     nil,
			wantCAData: nil,
		},
		{
			name:       "non-nil CA populates CAData",
			caCert:     []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
			wantCAData: []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := buildGuestBootstrapRESTConfig("https://guest.example:6443", "broker-token", tt.caCert)

			if cfg.Host != "https://guest.example:6443" {
				t.Fatalf("Host = %q, want %q", cfg.Host, "https://guest.example:6443")
			}
			if cfg.BearerToken != "broker-token" {
				t.Fatalf("BearerToken = %q, want broker-token", cfg.BearerToken)
			}
			if cfg.TLSClientConfig.Insecure {
				t.Fatal("expected verified TLS, got insecure config")
			}
			if string(cfg.TLSClientConfig.CAData) != string(tt.wantCAData) {
				t.Fatalf("CAData = %q, want %q", string(cfg.TLSClientConfig.CAData), string(tt.wantCAData))
			}
		})
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

func TestRequestPlatformSAToken_EmptyTokenReturnsError(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{Token: ""},
		}, nil
	})

	_, _, err := requestPlatformSAToken(context.Background(), client, "guest-target")
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	if got := err.Error(); !strings.Contains(got, "returned empty token") {
		t.Fatalf("error = %q, want it to contain %q", got, "returned empty token")
	}
}

func TestBootstrapGuestCluster_Phase1Success_ReturnsEmptyCACert(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	defer func() {
		probeWithSystemTrustFn = origProbe
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

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return client, nil, nil
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

	gotCA, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", leafDER)
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

	_, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", []byte("dummy-leaf"))
	if err == nil {
		t.Fatal("expected error for missing ConfigMap")
	}
}

func TestExtractCAFromGuest_EmptyCACrtKey(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	tests := []struct {
		name string
		data map[string]string
	}{
		{name: "missing ca.crt key", data: map[string]string{"other": "data"}},
		{name: "empty ca.crt value", data: map[string]string{"ca.crt": ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-root-ca.crt",
					Namespace: "kube-system",
				},
				Data: tt.data,
			})
			newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
				return client, nil
			}

			_, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", []byte("dummy-leaf"))
			if err == nil {
				t.Fatal("expected error for empty/missing ca.crt")
			}
			if got := err.Error(); !strings.Contains(got, "missing ca.crt data key") {
				t.Fatalf("error = %q, want it to contain %q", got, "missing ca.crt data key")
			}
		})
	}
}

func TestExtractCAFromGuest_CADoesNotSignLeaf(t *testing.T) {
	origNewClient := newKubernetesClientForConfig
	defer func() { newKubernetesClientForConfig = origNewClient }()

	ca, caKey, _ := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)

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

	_, err := extractCAFromGuest(context.Background(), "https://guest.example:6443", "broker-token", leafDER)
	if err == nil {
		t.Fatal("expected error when CA does not sign the leaf cert")
	}
}

func TestBootstrapGuestCluster_Phase2FallbackPopulatesCACert(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	origExtract := extractCAFromGuestFn
	origNewClient := newKubernetesClientForConfig
	defer func() {
		probeWithSystemTrustFn = origProbe
		extractCAFromGuestFn = origExtract
		newKubernetesClientForConfig = origNewClient
	}()

	ca, caKey, caPEM := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return nil, leafDER, &x509.UnknownAuthorityError{}
	}

	extractCAFromGuestFn = func(ctx context.Context, endpoint, token string, ld []byte) ([]byte, error) {
		if string(ld) != string(leafDER) {
			t.Fatalf("extractCAFromGuest received wrong leafDER")
		}
		return caPEM, nil
	}

	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{Token: "short-lived-token"},
		}, nil
	})

	var capturedCfg *rest.Config
	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		capturedCfg = rest.CopyConfig(cfg)
		return client, nil
	}

	result, err := BootstrapGuestCluster(context.Background(), "https://guest.example:6443", "broker-token", "guest-target")
	if err != nil {
		t.Fatalf("BootstrapGuestCluster() error = %v", err)
	}
	if string(result.CACert) != string(caPEM) {
		t.Fatalf("CACert not populated with extracted CA")
	}
	if capturedCfg == nil {
		t.Fatal("expected Phase 3 REST config to be captured")
	}
	if string(capturedCfg.TLSClientConfig.CAData) != string(caPEM) {
		t.Fatal("Phase 3 REST config missing extracted CA in CAData")
	}
}

func TestBootstrapGuestCluster_NonX509ErrorDoesNotFallback(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	origExtract := extractCAFromGuestFn
	defer func() {
		probeWithSystemTrustFn = origProbe
		extractCAFromGuestFn = origExtract
	}()

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return nil, nil, fmt.Errorf("connection refused")
	}

	extractCalled := false
	extractCAFromGuestFn = func(ctx context.Context, endpoint, token string, ld []byte) ([]byte, error) {
		extractCalled = true
		return nil, nil
	}

	_, err := BootstrapGuestCluster(context.Background(), "https://guest.example:6443", "broker-token", "guest-target")
	if err == nil {
		t.Fatal("expected error")
	}
	if extractCalled {
		t.Fatal("extractCAFromGuest should not be called for non-x509 errors")
	}
}

func TestBootstrapGuestCluster_X509ErrorWithNilLeafDER(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	origExtract := extractCAFromGuestFn
	defer func() {
		probeWithSystemTrustFn = origProbe
		extractCAFromGuestFn = origExtract
	}()

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return nil, nil, &x509.UnknownAuthorityError{}
	}

	extractCalled := false
	extractCAFromGuestFn = func(ctx context.Context, endpoint, token string, ld []byte) ([]byte, error) {
		extractCalled = true
		return nil, nil
	}

	_, err := BootstrapGuestCluster(context.Background(), "https://guest.example:6443", "broker-token", "guest-target")
	if err == nil {
		t.Fatal("expected error")
	}
	if extractCalled {
		t.Fatal("extractCAFromGuest should not be called when leafDER is nil")
	}
}

func TestBootstrapGuestCluster_ExtractCAFailure(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	origExtract := extractCAFromGuestFn
	defer func() {
		probeWithSystemTrustFn = origProbe
		extractCAFromGuestFn = origExtract
	}()

	ca, caKey, _ := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return nil, leafDER, &x509.UnknownAuthorityError{}
	}

	extractCAFromGuestFn = func(ctx context.Context, endpoint, token string, ld []byte) ([]byte, error) {
		return nil, fmt.Errorf("configmap not found")
	}

	_, err := BootstrapGuestCluster(context.Background(), "https://guest.example:6443", "broker-token", "guest-target")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "extract guest cluster CA") {
		t.Fatalf("error = %q, want it to contain %q", got, "extract guest cluster CA")
	}
}

func TestBootstrapGuestCluster_Phase3ClientCreationFailure(t *testing.T) {
	origProbe := probeWithSystemTrustFn
	origExtract := extractCAFromGuestFn
	origNewClient := newKubernetesClientForConfig
	defer func() {
		probeWithSystemTrustFn = origProbe
		extractCAFromGuestFn = origExtract
		newKubernetesClientForConfig = origNewClient
	}()

	ca, caKey, caPEM := generateTestCA(t)
	_, leafDER := generateTestLeaf(t, ca, caKey)

	probeWithSystemTrustFn = func(endpoint, token string) (kubernetes.Interface, []byte, error) {
		return nil, leafDER, &x509.UnknownAuthorityError{}
	}

	extractCAFromGuestFn = func(ctx context.Context, endpoint, token string, ld []byte) ([]byte, error) {
		return caPEM, nil
	}

	newKubernetesClientForConfig = func(cfg *rest.Config) (kubernetes.Interface, error) {
		return nil, fmt.Errorf("TLS handshake failed")
	}

	_, err := BootstrapGuestCluster(context.Background(), "https://guest.example:6443", "broker-token", "guest-target")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); !strings.Contains(got, "create verified kubernetes client") {
		t.Fatalf("error = %q, want it to contain %q", got, "create verified kubernetes client")
	}
}

func TestValidateCASignsLeaf_BadPEM(t *testing.T) {
	err := validateCASignsLeaf([]byte("not-valid-pem"), []byte{0x30, 0x82})
	if err == nil {
		t.Fatal("expected error for bad PEM")
	}
	if got := err.Error(); !strings.Contains(got, "failed to parse CA certificate PEM") {
		t.Fatalf("error = %q, want it to contain %q", got, "failed to parse CA certificate PEM")
	}
}

func TestValidateCASignsLeaf_BadLeafDER(t *testing.T) {
	_, _, caPEM := generateTestCA(t)

	err := validateCASignsLeaf(caPEM, []byte("not-valid-der"))
	if err == nil {
		t.Fatal("expected error for bad leaf DER")
	}
	if got := err.Error(); !strings.Contains(got, "parse captured leaf certificate") {
		t.Fatalf("error = %q, want it to contain %q", got, "parse captured leaf certificate")
	}
}
