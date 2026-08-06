package kind

import (
	"context"
	"strings"
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestEnsurePlatformSA_CreatesAndIsIdempotent(t *testing.T) {
	client := fake.NewSimpleClientset()
	if err := ensurePlatformSA(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformSA create: %v", err)
	}
	sa, err := client.CoreV1().ServiceAccounts(platformSANamespace).Get(
		context.Background(), platformSAName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("Get SA: %v", err)
	}
	if sa.Name != platformSAName {
		t.Fatalf("SA name = %q, want %q", sa.Name, platformSAName)
	}
	if err := ensurePlatformSA(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformSA AlreadyExists: %v", err)
	}
}

func TestEnsurePlatformRBAC_CreatesBinding(t *testing.T) {
	client := fake.NewSimpleClientset()
	if err := ensurePlatformRBAC(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformRBAC: %v", err)
	}
	crb, err := client.RbacV1().ClusterRoleBindings().Get(
		context.Background(), platformSAName+"-cluster-admin", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("Get CRB: %v", err)
	}
	if crb.RoleRef.Name != "cluster-admin" {
		t.Fatalf("RoleRef.Name = %q, want cluster-admin", crb.RoleRef.Name)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != platformSAName {
		t.Fatalf("Subjects = %+v, want platform SA", crb.Subjects)
	}
}

func TestEnsurePlatformRBAC_ReconcilesSubjectsOnAlreadyExists(t *testing.T) {
	existing := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName + "-cluster-admin"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      "wrong-sa",
			Namespace: platformSANamespace,
		}},
	}
	client := fake.NewSimpleClientset(existing)
	if err := ensurePlatformRBAC(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformRBAC reconcile: %v", err)
	}
	crb, err := client.RbacV1().ClusterRoleBindings().Get(
		context.Background(), platformSAName+"-cluster-admin", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("Get CRB: %v", err)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != platformSAName {
		t.Fatalf("Subjects = %+v, want platform SA after reconcile", crb.Subjects)
	}
}

func TestBootstrapPlatformSASequence_MintsFreshToken(t *testing.T) {
	client := fake.NewSimpleClientset()
	createTokenCalls := 0
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		createTokenCalls++
		createAction, ok := action.(ktesting.CreateAction)
		if !ok {
			t.Fatalf("expected CreateAction, got %T", action)
		}
		req, ok := createAction.GetObject().(*authv1.TokenRequest)
		if !ok {
			t.Fatalf("expected TokenRequest, got %T", createAction.GetObject())
		}
		if req.Spec.ExpirationSeconds == nil || *req.Spec.ExpirationSeconds != platformTokenExpirySeconds {
			t.Fatalf("ExpirationSeconds = %v, want %d", req.Spec.ExpirationSeconds, platformTokenExpirySeconds)
		}
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{Token: "minted-token"},
		}, nil
	})

	if err := ensurePlatformSA(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformSA: %v", err)
	}
	if err := ensurePlatformRBAC(context.Background(), client); err != nil {
		t.Fatalf("ensurePlatformRBAC: %v", err)
	}

	tokenReq, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		context.Background(), platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](platformTokenExpirySeconds),
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if tokenReq.Status.Token != "minted-token" {
		t.Fatalf("token = %q, want minted-token", tokenReq.Status.Token)
	}
	if createTokenCalls != 1 {
		t.Fatalf("token create calls = %d, want 1", createTokenCalls)
	}

	if _, err := client.CoreV1().ServiceAccounts(platformSANamespace).CreateToken(
		context.Background(), platformSAName, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](platformTokenExpirySeconds),
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("second CreateToken: %v", err)
	}
	if createTokenCalls != 2 {
		t.Fatalf("token create calls = %d, want 2 (always mint fresh)", createTokenCalls)
	}

	ref := domain.SecretRef("targets/k8s-demo/sa-token")
	if !strings.HasPrefix(string(ref), "targets/") {
		t.Fatalf("unexpected secret ref shape %q", ref)
	}
}

func TestEnsurePlatformSA_CreateError(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		return true, nil, context.DeadlineExceeded
	})
	err := ensurePlatformSA(context.Background(), client)
	if err == nil {
		t.Fatal("expected create error")
	}
	if !strings.Contains(err.Error(), "create ServiceAccount") {
		t.Fatalf("error = %q, want create ServiceAccount context", err)
	}
}
