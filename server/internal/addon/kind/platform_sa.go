package kind

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
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	platformSAName      = "fleetshift-platform"
	platformSANamespace = "kube-system"
	// TokenRequest lifetime (30 days). Credentials are not rotated automatically.
	platformTokenExpirySeconds = 30 * 24 * 3600
)

// bootstrapPlatformSA ensures a ServiceAccount with cluster-admin RBAC
// on the cluster and returns a fresh bearer token for it. SA and CRB
// creation is upsert-safe (get-or-create); TokenRequest is always minted
// fresh. This simulates the credential provisioning that a real fleetlet
// agent would perform by mounting its own ServiceAccount token securely.
//
// Existing SA/RBAC resources are reused (AlreadyExists is not a failure);
// drifted bindings are reconciled. A new TokenRequest is always issued
// so same-generation retries still produce a token reference and secret.
//
// The returned [domain.SecretRef] is a vault key suitable for storing
// the token via [domain.ProducedSecret]; the target stores a reference
// to it rather than the raw credential.
func bootstrapPlatformSA(ctx context.Context, kubeconfig []byte, targetID domain.TargetID) (domain.SecretRef, []byte, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return "", nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return bootstrapPlatformSAWithClient(ctx, client, targetID)
}

func bootstrapPlatformSAWithClient(ctx context.Context, client kubernetes.Interface, targetID domain.TargetID) (domain.SecretRef, []byte, error) {
	if err := ensurePlatformSA(ctx, client); err != nil {
		return "", nil, err
	}
	if err := ensurePlatformRBAC(ctx, client); err != nil {
		return "", nil, err
	}
	return requestPlatformSAToken(ctx, client, targetID)
}

func ensurePlatformSA(ctx context.Context, client kubernetes.Interface) error {
	_, err := client.CoreV1().ServiceAccounts(platformSANamespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ServiceAccount %s/%s: %w", platformSANamespace, platformSAName, err)
	}
	return nil
}

// ensurePlatformRBAC ensures a ClusterRoleBinding granting cluster-admin
// to the platform ServiceAccount. On AlreadyExists it reconciles RoleRef
// and Subjects to the desired binding.
func ensurePlatformRBAC(ctx context.Context, client kubernetes.Interface) error {
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
	return ensureClusterRoleBinding(ctx, client, desired)
}

func requestPlatformSAToken(ctx context.Context, client kubernetes.Interface, targetID domain.TargetID) (domain.SecretRef, []byte, error) {
	tokenReq, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		ctx, platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](platformTokenExpirySeconds),
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("create token for %s/%s: %w", platformSANamespace, platformSAName, err)
	}
	if tokenReq.Status.Token == "" {
		return "", nil, fmt.Errorf("create token for %s/%s returned empty token", platformSANamespace, platformSAName)
	}
	ref := domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID))
	return ref, []byte(tokenReq.Status.Token), nil
}

// ensureClusterRoleBinding creates desired, or reconciles an existing
// binding with the same name. RoleRef conflicts are delete+recreate
// (RoleRef is immutable); subject drift is updated in place.
func ensureClusterRoleBinding(ctx context.Context, client kubernetes.Interface, desired *rbacv1.ClusterRoleBinding) error {
	bindings := client.RbacV1().ClusterRoleBindings()
	_, err := bindings.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ClusterRoleBinding %q: %w", desired.Name, err)
	}

	existing, err := bindings.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing ClusterRoleBinding %q: %w", desired.Name, err)
	}

	if !equality.Semantic.DeepEqual(existing.RoleRef, desired.RoleRef) {
		if err := bindings.Delete(ctx, desired.Name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete conflicting ClusterRoleBinding %q: %w", desired.Name, err)
		}
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("recreate ClusterRoleBinding %q: %w", desired.Name, err)
		}
		return nil
	}

	if equality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		return nil
	}

	updated := existing.DeepCopy()
	updated.Subjects = desired.Subjects
	if _, err := bindings.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ClusterRoleBinding %q: %w", desired.Name, err)
	}
	return nil
}
