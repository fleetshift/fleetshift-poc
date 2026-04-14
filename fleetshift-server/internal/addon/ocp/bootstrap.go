package ocp

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
	platformSAName             = "fleetshift-platform"
	platformNamespace          = "fleetshift-system"
	defaultPlatformTokenExpiry = 24 * 3600 // seconds
)

// BootstrapResult contains the vault references and credentials generated
// during cluster bootstrap.
type BootstrapResult struct {
	SATokenRef    domain.SecretRef
	SAToken       []byte
	KubeconfigRef domain.SecretRef
	SSHKeyRef     domain.SecretRef
}

// BootstrapCluster performs post-provisioning setup on a newly created OCP
// cluster using the kubeadmin kubeconfig. It creates the fleetshift-system
// namespace, provisions a ServiceAccount with cluster-admin privileges,
// optionally grants cluster-admin to the caller via OIDC username binding,
// and generates a bearer token for the platform ServiceAccount.
//
// tokenExpirySeconds controls the SA token lifetime. Pass 0 to use the
// default (24 hours).
func BootstrapCluster(
	ctx context.Context,
	kubeconfig []byte,
	targetID domain.TargetID,
	caller *domain.SubjectClaims,
	issuerURL domain.IssuerURL,
	tokenExpirySeconds int64,
) (*BootstrapResult, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	// 1. Create namespace fleetshift-system
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: platformNamespace},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create namespace %s: %w", platformNamespace, err)
	}

	// 2. Create ServiceAccount fleetshift-platform
	_, err = client.CoreV1().ServiceAccounts(platformNamespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create ServiceAccount %s/%s: %w", platformNamespace, platformSAName, err)
	}

	// 3. Create ClusterRoleBinding fleetshift-platform-cluster-admin
	_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fleetshift-platform-cluster-admin"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      platformSAName,
			Namespace: platformNamespace,
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create ClusterRoleBinding for %s: %w", platformSAName, err)
	}

	// 4. If caller is not nil and has Email, create ClusterRoleBinding for OIDC user
	if caller != nil {
		if emails, ok := caller.Extra["email"]; ok && len(emails) > 0 {
			email := emails[0]
			oidcUsername := fmt.Sprintf("oidc:%s", email)

			_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "fleetshift-caller-admin"},
				RoleRef: rbacv1.RoleRef{
					APIGroup: "rbac.authorization.k8s.io",
					Kind:     "ClusterRole",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{{
					Kind:     "User",
					Name:     oidcUsername,
					APIGroup: "rbac.authorization.k8s.io",
				}},
			}, metav1.CreateOptions{})
			if err != nil {
				return nil, fmt.Errorf("create ClusterRoleBinding for caller %s: %w", oidcUsername, err)
			}
		}
	}

	// 5. Generate a bearer token for the SA via TokenRequest API
	expiry := int64(tokenExpirySeconds)
	if expiry <= 0 {
		expiry = defaultPlatformTokenExpiry
	}
	tokenReq, err := client.CoreV1().ServiceAccounts(platformNamespace).CreateToken(
		ctx, platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To(expiry),
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create token for %s/%s: %w", platformNamespace, platformSAName, err)
	}

	// 6. Return BootstrapResult with vault ref paths
	return &BootstrapResult{
		SATokenRef:    domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID)),
		SAToken:       []byte(tokenReq.Status.Token),
		KubeconfigRef: domain.SecretRef(fmt.Sprintf("targets/%s/kubeconfig", targetID)),
		SSHKeyRef:     domain.SecretRef(fmt.Sprintf("targets/%s/ssh-key", targetID)),
	}, nil
}
