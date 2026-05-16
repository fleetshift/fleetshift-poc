package gcphcp

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	platformSAName                     = "fleetshift-platform"
	platformSANamespace                = "kube-system"
	platformSATokenSecretName          = platformSAName + "-token"
	defaultPlatformSATokenPollInterval = 250 * time.Millisecond
	defaultPlatformSATokenWaitTimeout  = 30 * time.Second
)

var (
	platformSATokenSecretPollInterval = defaultPlatformSATokenPollInterval
	platformSATokenSecretWaitTimeout  = defaultPlatformSATokenWaitTimeout
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
// on the guest cluster and returns a durable bearer token and CA certificate
// for it, sourced from a manually managed service-account-token Secret.
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

	secretName, err := ensurePlatformSATokenSecret(ctx, client)
	if err != nil {
		return BootstrapResult{}, err
	}

	token, caCert, err := waitForPlatformSATokenSecretData(ctx, client, secretName)
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

func ensurePlatformSATokenSecret(ctx context.Context, client kubernetes.Interface) (string, error) {
	secrets := client.CoreV1().Secrets(platformSANamespace)
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformSATokenSecretName,
			Namespace: platformSANamespace,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: platformSAName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}

	if _, err := secrets.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create service account token secret %s/%s: %w", platformSANamespace, desired.Name, err)
	}

	secret, err := secrets.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get service account token secret %s/%s: %w", platformSANamespace, desired.Name, err)
	}
	if err := validatePlatformSATokenSecret(secret); err != nil {
		return "", err
	}
	return secret.Name, nil
}

func validatePlatformSATokenSecret(secret *corev1.Secret) error {
	if secret.Type != corev1.SecretTypeServiceAccountToken {
		return fmt.Errorf(
			"service account token secret %s/%s has type %q, want %q",
			secret.Namespace, secret.Name, secret.Type, corev1.SecretTypeServiceAccountToken,
		)
	}
	if secret.Annotations[corev1.ServiceAccountNameKey] != platformSAName {
		return fmt.Errorf(
			"service account token secret %s/%s targets service account %q, want %q",
			secret.Namespace, secret.Name, secret.Annotations[corev1.ServiceAccountNameKey], platformSAName,
		)
	}
	return nil
}

func waitForPlatformSATokenSecretData(
	ctx context.Context,
	client kubernetes.Interface,
	secretName string,
) ([]byte, []byte, error) {
	waitCtx, cancel := context.WithTimeout(ctx, platformSATokenSecretWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(platformSATokenSecretPollInterval)
	defer ticker.Stop()

	for {
		secret, err := client.CoreV1().Secrets(platformSANamespace).Get(waitCtx, secretName, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("get service account token secret %s/%s: %w", platformSANamespace, secretName, err)
		}
		if err := validatePlatformSATokenSecret(secret); err != nil {
			return nil, nil, err
		}

		token := secret.Data["token"]
		caCert := secret.Data["ca.crt"]
		if len(token) > 0 && len(caCert) > 0 {
			return token, caCert, nil
		}

		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				return nil, nil, fmt.Errorf(
					"timeout waiting for service account token secret %s/%s to be populated: %w",
					platformSANamespace, secretName, waitCtx.Err(),
				)
			}
			return nil, nil, waitCtx.Err()
		case <-ticker.C:
		}
	}
}
