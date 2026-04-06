package kind

import (
	"context"
	"fmt"
	"os"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// OIDCSpec configures the K8s API server's OIDC authentication on a
// kind cluster. The issuer URL and audience (client ID) are derived
// from the caller's identity via [domain.DeliveryAuth]. CA trust for
// the OIDC issuer is infrastructure config on the [Agent], not
// per-cluster config.
type OIDCSpec struct {
	UsernameClaim string `json:"usernameClaim,omitempty"` // default: "sub"
	GroupsClaim   string `json:"groupsClaim,omitempty"`   // default: "groups"
}

func (s *OIDCSpec) usernameClaim() string {
	if s.UsernameClaim != "" {
		return s.UsernameClaim
	}
	return "sub"
}

func (s *OIDCSpec) groupsClaim() string {
	if s.GroupsClaim != "" {
		return s.GroupsClaim
	}
	return "groups"
}

const oidcCACertContainerPath = "/etc/kubernetes/pki/oidc-ca.pem"

// BuildKindOIDCConfig generates a kind cluster config YAML with OIDC
// API server flags and (optionally) a CA cert mount. issuerURL and
// audience are derived from the caller's identity; spec provides
// infrastructure config (claim mappings, CA bundle). caCertHostPath
// may be empty if no CA bundle is configured.
func BuildKindOIDCConfig(issuerURL domain.IssuerURL, audience domain.Audience, spec *OIDCSpec, caCertHostPath string) ([]byte, error) {
	if issuerURL == "" {
		return nil, fmt.Errorf("oidc: issuerURL is required")
	}
	if audience == "" {
		return nil, fmt.Errorf("oidc: audience is required")
	}

	var extraArgs strings.Builder
	fmt.Fprintf(&extraArgs, "        oidc-issuer-url: %q\n", string(issuerURL))
	fmt.Fprintf(&extraArgs, "        oidc-client-id: %q\n", string(audience))
	fmt.Fprintf(&extraArgs, "        oidc-username-claim: %q\n", spec.usernameClaim())
	fmt.Fprintf(&extraArgs, "        oidc-groups-claim: %q\n", spec.groupsClaim())
	if caCertHostPath != "" {
		fmt.Fprintf(&extraArgs, "        oidc-ca-file: %q\n", oidcCACertContainerPath)
	}

	var extraMounts string
	if caCertHostPath != "" {
		extraMounts = fmt.Sprintf(`    extraMounts:
      - hostPath: %s
        containerPath: %s
        readOnly: true
`, caCertHostPath, oidcCACertContainerPath)
	}

	config := fmt.Sprintf(`kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
%skubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
%s`, extraMounts, extraArgs.String())

	return []byte(config), nil
}

// bootstrapRBAC creates a ClusterRoleBinding granting the caller
// cluster-admin on the newly created kind cluster. This uses the
// admin kubeconfig the kind agent already has in hand.
func bootstrapRBAC(ctx context.Context, kubeconfig []byte, issuerURL domain.IssuerURL, caller *domain.SubjectClaims) error {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("parse kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}

	// K8s OIDC authentication formats the username as "issuer#sub".
	username := string(issuerURL) + "#" + string(caller.Subject)

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fleetshift-admin-" + string(caller.Subject),
		},
		Subjects: []rbacv1.Subject{{
			Kind:     "User",
			Name:     username,
			APIGroup: "rbac.authorization.k8s.io",
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}

	_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create ClusterRoleBinding for %q: %w", username, err)
	}
	return nil
}

// writeCABundle writes the CA bundle to a temp file in dir and returns
// the path. If dir is empty, [os.TempDir] is used. The caller is
// responsible for cleanup.
func writeCABundle(caBundle []byte, dir string) (string, error) {
	f, err := os.CreateTemp(dir, "kind-oidc-ca-*.pem")
	if err != nil {
		return "", fmt.Errorf("create CA bundle temp file: %w", err)
	}
	if _, err := f.Write(caBundle); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("write CA bundle: %w", err)
	}
	f.Close()
	return f.Name(), nil
}
