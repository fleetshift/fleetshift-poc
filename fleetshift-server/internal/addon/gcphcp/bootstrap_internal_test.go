package gcphcp

import (
	"context"
	"reflect"
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func TestBuildGuestBootstrapRESTConfig_UsesSystemTrust(t *testing.T) {
	cfg := buildGuestBootstrapRESTConfig("https://guest.example:6443", "broker-token")

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
