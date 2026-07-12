package kind

import (
	"context"
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestEnsureCallerAdminBinding_Creates(t *testing.T) {
	client := fake.NewSimpleClientset()
	caller := &domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "alice",
			Issuer:  "https://issuer.example",
		},
	}
	if err := ensureCallerAdminBinding(context.Background(), client, caller.Issuer, caller); err != nil {
		t.Fatalf("ensureCallerAdminBinding: %v", err)
	}
	got, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), "fleetshift-admin-alice", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get binding: %v", err)
	}
	if got.RoleRef.Name != "cluster-admin" {
		t.Fatalf("RoleRef.Name = %q", got.RoleRef.Name)
	}
	if len(got.Subjects) != 1 || got.Subjects[0].Name != "https://issuer.example#alice" {
		t.Fatalf("Subjects = %+v", got.Subjects)
	}
}

func TestEnsureCallerAdminBinding_AlreadyExistsSucceeds(t *testing.T) {
	caller := &domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "alice",
			Issuer:  "https://issuer.example",
		},
	}
	existing := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fleetshift-admin-alice"},
		Subjects: []rbacv1.Subject{{
			Kind:     "User",
			Name:     "https://issuer.example#alice",
			APIGroup: "rbac.authorization.k8s.io",
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	client := fake.NewSimpleClientset(existing)
	if err := ensureCallerAdminBinding(context.Background(), client, caller.Issuer, caller); err != nil {
		t.Fatalf("retry ensureCallerAdminBinding: %v", err)
	}
}

func TestEnsureCallerAdminBinding_ReconcilesDriftedSubjects(t *testing.T) {
	caller := &domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "alice",
			Issuer:  "https://issuer.example",
		},
	}
	existing := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fleetshift-admin-alice"},
		Subjects: []rbacv1.Subject{{
			Kind:     "User",
			Name:     "stale-user",
			APIGroup: "rbac.authorization.k8s.io",
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	client := fake.NewSimpleClientset(existing)
	if err := ensureCallerAdminBinding(context.Background(), client, caller.Issuer, caller); err != nil {
		t.Fatalf("ensureCallerAdminBinding: %v", err)
	}
	got, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), "fleetshift-admin-alice", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Subjects[0].Name != "https://issuer.example#alice" {
		t.Fatalf("Subjects not reconciled: %+v", got.Subjects)
	}
}

func TestBootstrapPlatformSA_IdempotentRetryReturnsToken(t *testing.T) {
	existingSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: platformSAName, Namespace: platformSANamespace},
	}
	existingCRB := &rbacv1.ClusterRoleBinding{
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
	client := fake.NewSimpleClientset(existingSA, existingCRB)
	tokenCalls := 0
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		tokenCalls++
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{Token: "retry-token"},
		}, nil
	})

	ref, token, err := bootstrapPlatformSAWithClient(context.Background(), client, "k8s-demo")
	if err != nil {
		t.Fatalf("bootstrapPlatformSAWithClient: %v", err)
	}
	if tokenCalls != 1 {
		t.Fatalf("token requests = %d, want 1", tokenCalls)
	}
	if ref != "targets/k8s-demo/sa-token" {
		t.Fatalf("ref = %q", ref)
	}
	if string(token) != "retry-token" {
		t.Fatalf("token = %q", token)
	}
}

func TestBootstrapPlatformSA_CreatesThenRetry(t *testing.T) {
	client := fake.NewSimpleClientset()
	tokens := []string{"first-token", "second-token"}
	var n int
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		tok := tokens[n]
		n++
		return true, &authv1.TokenRequest{
			Status: authv1.TokenRequestStatus{Token: tok},
		}, nil
	})

	ref1, tok1, err := bootstrapPlatformSAWithClient(context.Background(), client, "k8s-demo")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	ref2, tok2, err := bootstrapPlatformSAWithClient(context.Background(), client, "k8s-demo")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if ref1 != ref2 {
		t.Fatalf("refs differ: %q vs %q", ref1, ref2)
	}
	if string(tok1) != "first-token" || string(tok2) != "second-token" {
		t.Fatalf("tokens = %q, %q", tok1, tok2)
	}
	// SA must still exist (AlreadyExists path reused it).
	if _, err := client.CoreV1().ServiceAccounts(platformSANamespace).Get(context.Background(), platformSAName, metav1.GetOptions{}); err != nil {
		t.Fatalf("SA missing after retry: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), platformSAName+"-cluster-admin", metav1.GetOptions{}); err != nil {
		t.Fatalf("CRB missing after retry: %v", err)
	}
}

func TestBootstrapPlatformSA_CreateFailsOnNonAlreadyExists(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "token" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewServiceUnavailable("api down")
	})
	_, _, err := bootstrapPlatformSAWithClient(context.Background(), client, "k8s-demo")
	if err == nil {
		t.Fatal("expected error")
	}
}
