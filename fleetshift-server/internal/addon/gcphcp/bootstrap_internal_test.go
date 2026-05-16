package gcphcp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestEnsurePlatformSATokenSecret_CreatesExpectedSecret(t *testing.T) {
	client := fake.NewSimpleClientset()

	secretName, err := ensurePlatformSATokenSecret(context.Background(), client)
	if err != nil {
		t.Fatalf("ensurePlatformSATokenSecret() error = %v", err)
	}
	if secretName != platformSATokenSecretName {
		t.Fatalf("secret name = %q, want %q", secretName, platformSATokenSecretName)
	}

	got, err := client.CoreV1().Secrets(platformSANamespace).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Secret: %v", err)
	}
	if got.Type != corev1.SecretTypeServiceAccountToken {
		t.Fatalf("secret type = %q, want %q", got.Type, corev1.SecretTypeServiceAccountToken)
	}
	if got.Annotations[corev1.ServiceAccountNameKey] != platformSAName {
		t.Fatalf("service-account annotation = %q, want %q", got.Annotations[corev1.ServiceAccountNameKey], platformSAName)
	}
}

func TestEnsurePlatformSATokenSecret_RejectsConflictingSecret(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformSATokenSecretName,
			Namespace: platformSANamespace,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: "different-service-account",
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	})

	_, err := ensurePlatformSATokenSecret(context.Background(), client)
	if err == nil {
		t.Fatal("expected conflicting secret error")
	}
	if !strings.Contains(err.Error(), "different-service-account") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForPlatformSATokenSecretData_ReturnsTokenAndCAFromSecret(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformSATokenSecretName,
			Namespace: platformSANamespace,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: platformSAName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
		Data: map[string][]byte{
			"token":  []byte("durable-token"),
			"ca.crt": []byte("cluster-ca"),
		},
	})

	token, caCert, err := waitForPlatformSATokenSecretData(context.Background(), client, platformSATokenSecretName)
	if err != nil {
		t.Fatalf("waitForPlatformSATokenSecretData() error = %v", err)
	}
	if string(token) != "durable-token" {
		t.Fatalf("token = %q, want durable-token", string(token))
	}
	if string(caCert) != "cluster-ca" {
		t.Fatalf("ca cert = %q, want cluster-ca", string(caCert))
	}
}

func TestWaitForPlatformSATokenSecretData_PollsUntilSecretPopulated(t *testing.T) {
	origInterval := platformSATokenSecretPollInterval
	origTimeout := platformSATokenSecretWaitTimeout
	platformSATokenSecretPollInterval = 5 * time.Millisecond
	platformSATokenSecretWaitTimeout = 50 * time.Millisecond
	defer func() {
		platformSATokenSecretPollInterval = origInterval
		platformSATokenSecretWaitTimeout = origTimeout
	}()

	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformSATokenSecretName,
			Namespace: platformSANamespace,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: platformSAName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	})

	secretGVR := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	getCalls := 0
	client.PrependReactor("get", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls == 2 {
			err := client.Tracker().Update(secretGVR, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      platformSATokenSecretName,
					Namespace: platformSANamespace,
					Annotations: map[string]string{
						corev1.ServiceAccountNameKey: platformSAName,
					},
				},
				Type: corev1.SecretTypeServiceAccountToken,
				Data: map[string][]byte{
					"token":  []byte("durable-token"),
					"ca.crt": []byte("cluster-ca"),
				},
			}, platformSANamespace)
			if err != nil {
				t.Fatalf("update tracker: %v", err)
			}
		}
		return false, nil, nil
	})

	token, caCert, err := waitForPlatformSATokenSecretData(context.Background(), client, platformSATokenSecretName)
	if err != nil {
		t.Fatalf("waitForPlatformSATokenSecretData() error = %v", err)
	}
	if getCalls < 2 {
		t.Fatalf("expected multiple get calls, got %d", getCalls)
	}
	if string(token) != "durable-token" || string(caCert) != "cluster-ca" {
		t.Fatalf("unexpected populated data: token=%q ca=%q", string(token), string(caCert))
	}
}

func TestWaitForPlatformSATokenSecretData_TimesOutWhenControllerNeverPopulatesSecret(t *testing.T) {
	origInterval := platformSATokenSecretPollInterval
	origTimeout := platformSATokenSecretWaitTimeout
	platformSATokenSecretPollInterval = 5 * time.Millisecond
	platformSATokenSecretWaitTimeout = 20 * time.Millisecond
	defer func() {
		platformSATokenSecretPollInterval = origInterval
		platformSATokenSecretWaitTimeout = origTimeout
	}()

	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformSATokenSecretName,
			Namespace: platformSANamespace,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: platformSAName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	})

	start := time.Now()
	_, _, err := waitForPlatformSATokenSecretData(context.Background(), client, platformSATokenSecretName)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "timeout waiting for service account token secret") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < platformSATokenSecretWaitTimeout {
		t.Fatalf("returned too quickly: %v", elapsed)
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
