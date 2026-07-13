package kind

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// hangOnGetClient wraps a clientset so ConfigMap Get blocks until ctx is done
// (or hang is closed). Used to assert ownership ConfigMap calls honor timeouts.
type hangOnGetClient struct {
	kubernetes.Interface
	hang <-chan struct{}
}

func (c *hangOnGetClient) CoreV1() typedcorev1.CoreV1Interface {
	return &hangOnGetCoreV1{CoreV1Interface: c.Interface.CoreV1(), hang: c.hang}
}

type hangOnGetCoreV1 struct {
	typedcorev1.CoreV1Interface
	hang <-chan struct{}
}

func (c *hangOnGetCoreV1) ConfigMaps(namespace string) typedcorev1.ConfigMapInterface {
	return &hangOnGetConfigMaps{
		ConfigMapInterface: c.CoreV1Interface.ConfigMaps(namespace),
		hang:               c.hang,
	}
}

type hangOnGetConfigMaps struct {
	typedcorev1.ConfigMapInterface
	hang <-chan struct{}
}

func (c *hangOnGetConfigMaps) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.ConfigMap, error) {
	select {
	case <-c.hang:
		return c.ConfigMapInterface.Get(ctx, name, opts)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestKubeGenerationStore_GetAppliesTimeout(t *testing.T) {
	orig := ownershipConfigMapTimeout
	ownershipConfigMapTimeout = 25 * time.Millisecond
	t.Cleanup(func() { ownershipConfigMapTimeout = orig })

	hang := make(chan struct{})
	s := &kubeGenerationStore{
		newClient: func([]byte) (kubernetes.Interface, error) {
			return &hangOnGetClient{Interface: fake.NewSimpleClientset(), hang: hang}, nil
		},
	}

	start := time.Now()
	_, _, err := s.Get(context.Background(), "fs--demo", nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get err=%v, want deadline exceeded", err)
	}
	if elapsed < ownershipConfigMapTimeout {
		t.Fatalf("Get returned too quickly: %v", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("Get hung too long: %v", elapsed)
	}
}

func TestKubeGenerationStore_CheckAndAdvanceAppliesTimeout(t *testing.T) {
	orig := ownershipConfigMapTimeout
	ownershipConfigMapTimeout = 25 * time.Millisecond
	t.Cleanup(func() { ownershipConfigMapTimeout = orig })

	hang := make(chan struct{})
	s := &kubeGenerationStore{
		newClient: func([]byte) (kubernetes.Interface, error) {
			return &hangOnGetClient{Interface: fake.NewSimpleClientset(), hang: hang}, nil
		},
	}

	_, _, err := s.CheckAndAdvance(context.Background(), "fs--demo", nil, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CheckAndAdvance err=%v, want deadline exceeded", err)
	}
}

func TestKubeGenerationStore_GetPreservesParentCancellation(t *testing.T) {
	orig := ownershipConfigMapTimeout
	ownershipConfigMapTimeout = time.Minute
	t.Cleanup(func() { ownershipConfigMapTimeout = orig })

	hang := make(chan struct{})
	s := &kubeGenerationStore{
		newClient: func([]byte) (kubernetes.Interface, error) {
			return &hangOnGetClient{Interface: fake.NewSimpleClientset(), hang: hang}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.Get(ctx, "fs--demo", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Get err=%v, want canceled", err)
	}
}

func TestKubeGenerationStore_CheckAndAdvanceSucceeds(t *testing.T) {
	client := fake.NewSimpleClientset()
	s := &kubeGenerationStore{
		newClient: func([]byte) (kubernetes.Interface, error) {
			return client, nil
		},
	}
	d, g, err := s.CheckAndAdvance(context.Background(), "fs--demo", nil, 3)
	if err != nil || d != GenerationCreated || g != 3 {
		t.Fatalf("create: disp=%v gen=%d err=%v", d, g, err)
	}
	d, g, err = s.CheckAndAdvance(context.Background(), "fs--demo", nil, 3)
	if err != nil || d != GenerationSame || g != 3 {
		t.Fatalf("same: disp=%v gen=%d err=%v", d, g, err)
	}
	d, g, err = s.CheckAndAdvance(context.Background(), "fs--demo", nil, 5)
	if err != nil || d != GenerationAdvanced || g != 5 {
		t.Fatalf("advance: disp=%v gen=%d err=%v", d, g, err)
	}
	recorded, found, err := s.Get(context.Background(), "fs--demo", nil)
	if err != nil || !found || recorded != 5 {
		t.Fatalf("Get: gen=%d found=%v err=%v", recorded, found, err)
	}
}
