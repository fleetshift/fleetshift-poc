package gcphcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"net"
	"strings"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	platformSAName           = "fleetshift-platform"
	platformSANamespace      = "kube-system"
	platformTokenExpiry      = 24 * 3600
	defaultCACertReadTimeout = 30 * time.Second
)

var caCertReadTimeout = defaultCACertReadTimeout

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
// on the guest cluster and returns a bearer token and CA certificate for it.
// This simulates the credential provisioning that a real fleetlet agent
// would perform.
//
// The function uses the broker token to authenticate to the guest cluster
// endpoint, creates the necessary resources, and extracts the credentials
// needed for ongoing platform access.
func BootstrapGuestCluster(ctx context.Context, guestEndpoint, brokerToken string, targetID domain.TargetID) (BootstrapResult, error) {
	// Create REST config with broker token and insecure TLS
	// (narrow bootstrap exception per design spec 10.5)
	cfg := &rest.Config{
		Host:        guestEndpoint,
		BearerToken: brokerToken,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("create kubernetes client: %w", err)
	}

	if err := createPlatformSA(ctx, client); err != nil {
		return BootstrapResult{}, err
	}

	if err := createPlatformRBAC(ctx, client); err != nil {
		return BootstrapResult{}, err
	}

	token, err := createSAToken(ctx, client)
	if err != nil {
		return BootstrapResult{}, err
	}

	caCtx, cancel := context.WithTimeout(ctx, caCertReadTimeout)
	defer cancel()

	caCert, err := readCACert(caCtx, guestEndpoint)
	if err != nil {
		return BootstrapResult{}, err
	}

	return BootstrapResult{
		SATokenRef: DeliverySecretRef(targetID),
		SAToken:    token,
		CACert:     caCert,
	}, nil
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
	_, err := client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
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
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ClusterRoleBinding for %s: %w", platformSAName, err)
	}
	return nil
}

// createSAToken creates a token for the fleetshift-platform ServiceAccount
// with a 24-hour expiry.
func createSAToken(ctx context.Context, client kubernetes.Interface) ([]byte, error) {
	tokenReq, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		ctx, platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](platformTokenExpiry),
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create token for %s/%s: %w", platformSANamespace, platformSAName, err)
	}
	return []byte(tokenReq.Status.Token), nil
}

// readCACert retrieves the CA certificate from the guest cluster endpoint
// by establishing a TLS connection and reading the peer certificates.
func readCACert(ctx context.Context, endpoint string) ([]byte, error) {
	host := endpoint
	for _, prefix := range []string{"https://", "http://"} {
		host = strings.TrimPrefix(host, prefix)
	}

	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("dial guest endpoint for CA: %w", err)
	}
	conn := tls.Client(rawConn, &tls.Config{InsecureSkipVerify: true})
	defer conn.Close()

	if err := conn.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("handshake guest endpoint for CA: %w", err)
	}

	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil, fmt.Errorf("no certificates from guest endpoint")
	}
	var buf bytes.Buffer
	for _, cert := range certs {
		if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
			return nil, fmt.Errorf("encode certificate: %w", err)
		}
	}
	return buf.Bytes(), nil
}
