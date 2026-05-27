package gcphcp

import (
	"context"
	"fmt"

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
	platformSAName                 = "fleetshift-platform"
	platformSANamespace            = "kube-system"
	defaultPlatformTokenExpirySeconds int64 = 24 * 3600
)

var newKubernetesClientForConfig = func(c *rest.Config) (kubernetes.Interface, error) {
	return kubernetes.NewForConfig(c)
}

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
// on the guest cluster and returns a short-lived bearer token for it.
//
// The function uses the broker token to authenticate to the guest cluster
// endpoint, creates the necessary resources, and extracts the credentials
// needed for ongoing platform access. Bootstrap uses the host's normal
// system trust store to verify the guest API endpoint.
func BootstrapGuestCluster(
	ctx context.Context,
	guestEndpoint, brokerToken string,
	targetID domain.TargetID,
) (BootstrapResult, error) {
	cfg := buildGuestBootstrapRESTConfig(guestEndpoint, brokerToken)

	client, err := newKubernetesClientForConfig(cfg)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("create kubernetes client: %w", err)
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
	}, nil
}

func buildGuestBootstrapRESTConfig(guestEndpoint, brokerToken string) *rest.Config {
	return &rest.Config{
		Host:        guestEndpoint,
		BearerToken: brokerToken,
	}
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
