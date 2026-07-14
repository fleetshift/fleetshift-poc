package gcphcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	platformSAName                          = "fleetshift-platform"
	platformSANamespace                     = "kube-system"
	defaultPlatformTokenExpirySeconds int64 = 30 * 24 * 3600
	rootCAConfigMapName                     = "kube-root-ca.crt"
)

// Swappable in tests to avoid real TLS connections and API server calls.
var (
	newKubernetesClientForConfig = func(c *rest.Config) (kubernetes.Interface, error) {
		return kubernetes.NewForConfig(c)
	}
	probeWithSystemTrustFn = probeWithSystemTrust
	extractCAFromGuestFn   = extractCAFromGuest
)

// BootstrapResult contains the credentials and metadata obtained from
// bootstrapping a guest cluster.
type BootstrapResult struct {
	SATokenRef domain.SecretRef
	SAToken    []byte
	CACert     []byte
}

// DeliverySecretRef returns the vault key for storing the guest cluster
// ServiceAccount token.
func DeliverySecretRef(targetID domain.TargetID) domain.SecretRef {
	return domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID))
}

// BootstrapGuestCluster creates a ServiceAccount with cluster-admin RBAC
// on the guest cluster and returns a bearer token for it (TokenRequest
// expiry is defaultPlatformTokenExpirySeconds).
//
// Connection strategy:
//  1. probe with system trust (publicly trusted certs)
//  2. on x509 error, extract root CA from kube-root-ca.crt with leaf-cert
//     pinning and CA-to-leaf chain validation
//  3. reconnect with the extracted CA for privileged operations
func BootstrapGuestCluster(
	ctx context.Context,
	guestEndpoint, brokerToken string,
	targetID domain.TargetID,
) (BootstrapResult, error) {
	// Step 1: try with system trust
	client, leafDER, probeErr := probeWithSystemTrustFn(guestEndpoint, brokerToken)

	var caCert []byte
	if probeErr != nil {
		if !isCertVerificationError(probeErr) || leafDER == nil {
			return BootstrapResult{}, fmt.Errorf("probe guest cluster: %w", probeErr)
		}

		// Step 2: extract CA from guest cluster
		var err error
		caCert, err = extractCAFromGuestFn(ctx, guestEndpoint, brokerToken, leafDER)
		if err != nil {
			return BootstrapResult{}, fmt.Errorf("extract guest cluster CA: %w", err)
		}

		// Step 3: reconnect with extracted CA
		cfg := buildGuestBootstrapRESTConfig(guestEndpoint, brokerToken, caCert)
		client, err = newKubernetesClientForConfig(cfg)
		if err != nil {
			return BootstrapResult{}, fmt.Errorf("create verified kubernetes client: %w", err)
		}
	}

	if err := createPlatformSA(ctx, client); err != nil {
		return BootstrapResult{}, err
	}

	if err := createPlatformRBAC(ctx, client); err != nil {
		return BootstrapResult{}, err
	}

	ref, token, err := requestPlatformSAToken(ctx, client, targetID)
	if err != nil {
		return BootstrapResult{}, err
	}

	return BootstrapResult{
		SATokenRef: ref,
		SAToken:    token,
		CACert:     caCert,
	}, nil
}

// buildGuestBootstrapRESTConfig builds a REST config for guest bootstrap
// using the broker token and optional extracted CA PEM.
func buildGuestBootstrapRESTConfig(guestEndpoint, brokerToken string, caCert []byte) *rest.Config {
	cfg := &rest.Config{
		Host:        guestEndpoint,
		BearerToken: brokerToken,
	}
	if len(caCert) > 0 {
		cfg.TLSClientConfig.CAData = caCert
	}
	return cfg
}

// createPlatformSA creates the fleetshift-platform ServiceAccount in kube-system.
// It ignores AlreadyExists errors to make the operation idempotent.
func createPlatformSA(ctx context.Context, client kubernetes.Interface) error {
	_, err := client.CoreV1().ServiceAccounts(platformSANamespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ServiceAccount %s/%s: %w", platformSANamespace, platformSAName, err)
	}
	return nil
}

// createPlatformRBAC creates a ClusterRoleBinding granting cluster-admin to
// the fleetshift-platform ServiceAccount. It ignores AlreadyExists errors to
// make the operation idempotent.
func createPlatformRBAC(ctx context.Context, client kubernetes.Interface) error {
	desired := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName + "-cluster-admin"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      platformSAName,
			Namespace: platformSANamespace,
		}},
	}

	bindings := client.RbacV1().ClusterRoleBindings()
	_, err := bindings.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ClusterRoleBinding for %s: %w", platformSAName, err)
	}

	existing, err := bindings.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing ClusterRoleBinding for %s: %w", platformSAName, err)
	}

	if !equality.Semantic.DeepEqual(existing.RoleRef, desired.RoleRef) {
		if err := bindings.Delete(ctx, desired.Name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete conflicting ClusterRoleBinding for %s: %w", platformSAName, err)
		}
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("recreate ClusterRoleBinding for %s: %w", platformSAName, err)
		}
		return nil
	}

	if equality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		return nil
	}

	updated := existing.DeepCopy()
	updated.Subjects = desired.Subjects
	if _, err := bindings.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ClusterRoleBinding for %s: %w", platformSAName, err)
	}
	return nil
}

// requestPlatformSAToken mints a TokenRequest for the platform SA and
// returns the vault SecretRef plus token bytes. Expiry is
// defaultPlatformTokenExpirySeconds.
func requestPlatformSAToken(
	ctx context.Context,
	client kubernetes.Interface,
	targetID domain.TargetID,
) (domain.SecretRef, []byte, error) {
	tokenReq, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		ctx,
		platformSAName,
		&authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To(defaultPlatformTokenExpirySeconds),
			},
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		return "", nil, fmt.Errorf("create token for %s/%s: %w", platformSANamespace, platformSAName, err)
	}
	if tokenReq.Status.Token == "" {
		return "", nil, fmt.Errorf("create token for %s/%s returned empty token", platformSANamespace, platformSAName)
	}

	return DeliverySecretRef(targetID), []byte(tokenReq.Status.Token), nil
}

// isCertVerificationError reports whether err is an x509 unknown-authority
// failure that should trigger CA extraction fallback.
func isCertVerificationError(err error) bool {
	if err == nil {
		return false
	}
	var unknownAuth x509.UnknownAuthorityError
	if errors.As(err, &unknownAuth) {
		return true
	}
	var unknownAuthPtr *x509.UnknownAuthorityError
	return errors.As(err, &unknownAuthPtr)
}

// probeWithSystemTrust dials the guest API with system trust. On unknown-
// authority failure it returns the captured leaf DER alongside the error.
func probeWithSystemTrust(guestEndpoint, brokerToken string) (kubernetes.Interface, []byte, error) {
	var capturedLeafDER []byte

	cfg := &rest.Config{
		Host:        guestEndpoint,
		BearerToken: brokerToken,
	}
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &leafCaptureTransport{base: rt, capture: &capturedLeafDER}
	})

	client, err := newKubernetesClientForConfig(cfg)
	if err == nil {
		_, err = client.Discovery().ServerVersion()
	}
	if err != nil {
		if isCertVerificationError(err) && capturedLeafDER != nil {
			return nil, capturedLeafDER, err
		}
		return nil, nil, err
	}

	return client, nil, nil
}

// leafCaptureTransport wraps a RoundTripper and captures the peer leaf
// certificate DER when the underlying trip fails (typically x509 verify).
type leafCaptureTransport struct {
	base    http.RoundTripper
	capture *[]byte
}

// RoundTrip delegates to the base transport and, on error, probes the peer leaf cert.
func (t *leafCaptureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		host := req.URL.Hostname()
		port := req.URL.Port()
		if port == "" {
			port = "443"
		}
		*t.capture = probeLeafCert(host + ":" + port)
	}
	return resp, err
}

// probeLeafCert dials addr insecurely and returns the peer leaf certificate DER.
func probeLeafCert(addr string) []byte {
	conn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 5 * time.Second},
		"tcp", addr,
		&tls.Config{InsecureSkipVerify: true},
	)
	if err != nil {
		return nil
	}
	defer conn.Close()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil
	}
	return certs[0].Raw
}

// TODO: Review insecure TLS usage here and in probeLeafCert once migrated to
// CLM backend — the insecure connection may be avoidable if CLM provides a
// trusted CA path or certificate bundle.
func extractCAFromGuest(
	ctx context.Context,
	guestEndpoint, brokerToken string,
	leafDER []byte,
) ([]byte, error) {
	cfg := &rest.Config{
		Host:        guestEndpoint,
		BearerToken: brokerToken,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	client, err := newKubernetesClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create insecure kubernetes client for CA extraction: %w", err)
	}

	cm, err := client.CoreV1().ConfigMaps(platformSANamespace).Get(
		ctx,
		rootCAConfigMapName,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("read %s/%s ConfigMap: %w", platformSANamespace, rootCAConfigMapName, err)
	}

	caPEM, ok := cm.Data["ca.crt"]
	if !ok || caPEM == "" {
		return nil, fmt.Errorf("ConfigMap %s/%s missing ca.crt data key", platformSANamespace, rootCAConfigMapName)
	}

	if err := validateCASignsLeaf([]byte(caPEM), leafDER); err != nil {
		return nil, fmt.Errorf("extracted CA does not sign the server certificate: %w", err)
	}

	return []byte(caPEM), nil
}

// validateCASignsLeaf checks that caPEM cryptographically signs leafDER,
// pinning verify time to the leaf's NotBefore to avoid clock-skew false negatives.
func validateCASignsLeaf(caPEM, leafDER []byte) error {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("failed to parse CA certificate PEM")
	}

	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return fmt.Errorf("parse captured leaf certificate: %w", err)
	}

	// Pin to the leaf's own validity window so we only check the cryptographic
	// signing relationship, not temporal validity. The cert was just served in a
	// live TLS handshake; clock skew or short-lived OpenShift CAs could cause
	// spurious rejections with time.Now(). The reconnect TLS stack enforces expiry.
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: leaf.NotBefore.Add(time.Second),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	return err
}
