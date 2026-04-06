package kind

import (
	"context"
	"fmt"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	platformSAName      = "fleetshift-platform"
	platformSANamespace = "kube-system"
	// 24 hours — short-lived for the prototype; a fleetlet would rotate.
	platformTokenExpirySeconds = 24 * 3600
)

// bootstrapPlatformSA creates a ServiceAccount with cluster-admin RBAC
// on the newly created cluster and returns a bearer token for it. This
// simulates the credential provisioning that a real fleetlet agent
// would perform by mounting its own ServiceAccount token securely.
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

	_, err = client.CoreV1().ServiceAccounts(platformSANamespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName},
	}, metav1.CreateOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("create ServiceAccount %s/%s: %w", platformSANamespace, platformSAName, err)
	}

	_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
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
	if err != nil {
		return "", nil, fmt.Errorf("create ClusterRoleBinding for %s: %w", platformSAName, err)
	}

	tokenReq, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		ctx, platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](platformTokenExpirySeconds),
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("create token for %s/%s: %w", platformSANamespace, platformSAName, err)
	}

	ref := domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID))
	return ref, []byte(tokenReq.Status.Token), nil
}
