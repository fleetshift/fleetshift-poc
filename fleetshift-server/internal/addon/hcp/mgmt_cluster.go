package hcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// mgmtCluster abstracts management cluster operations for testability.
type mgmtCluster interface {
	applyResources(ctx context.Context, hc hyperv1.HostedCluster, nodePools []hyperv1.NodePool, secrets []corev1.Secret) error
	waitForAvailable(ctx context.Context, name string) error
	getAdminKubeconfig(ctx context.Context, name string) ([]byte, error)
	deleteNodePools(ctx context.Context, spec ClusterSpec) error
	deleteHostedCluster(ctx context.Context, name string) error
	waitForDeletion(ctx context.Context, name string) error
}

var (
	hostedClusterGVR = schema.GroupVersionResource{
		Group:    "hypershift.openshift.io",
		Version:  "v1beta1",
		Resource: "hostedclusters",
	}
	nodePoolGVR = schema.GroupVersionResource{
		Group:    "hypershift.openshift.io",
		Version:  "v1beta1",
		Resource: "nodepools",
	}
)

// kubeMgmtCluster implements mgmtCluster using real K8s clients.
type kubeMgmtCluster struct {
	clientset     kubernetes.Interface
	dynamicClient dynamic.Interface
}

func newKubeMgmtCluster(kubeconfig []byte) (*kubeMgmtCluster, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parse mgmt kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}
	return &kubeMgmtCluster{clientset: cs, dynamicClient: dc}, nil
}

func (m *kubeMgmtCluster) applyResources(ctx context.Context, hc hyperv1.HostedCluster, nodePools []hyperv1.NodePool, secrets []corev1.Secret) error {
	// Create secrets first (pull secret, SSH key, etcd encryption key).
	for _, s := range secrets {
		_, err := m.clientset.CoreV1().Secrets(s.Namespace).Create(ctx, &s, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create secret %s/%s: %w", s.Namespace, s.Name, err)
		}
	}

	// Create HostedCluster CRD via dynamic client.
	hcUnstructured, err := toUnstructured(&hc, hostedClusterGVR)
	if err != nil {
		return fmt.Errorf("convert HostedCluster to unstructured: %w", err)
	}
	_, err = m.dynamicClient.Resource(hostedClusterGVR).Namespace(hc.Namespace).Create(ctx, hcUnstructured, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create HostedCluster %s: %w", hc.Name, err)
	}

	// Create NodePool CRDs.
	for _, np := range nodePools {
		npUnstructured, err := toUnstructured(&np, nodePoolGVR)
		if err != nil {
			return fmt.Errorf("convert NodePool to unstructured: %w", err)
		}
		_, err = m.dynamicClient.Resource(nodePoolGVR).Namespace(np.Namespace).Create(ctx, npUnstructured, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create NodePool %s: %w", np.Name, err)
		}
	}
	return nil
}

func (m *kubeMgmtCluster) waitForAvailable(ctx context.Context, name string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := m.dynamicClient.Resource(hostedClusterGVR).Namespace(hostedClusterNamespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get HostedCluster %q: %w", name, err)
		}

		conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
		if err == nil && found {
			for _, c := range conditions {
				cond, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				if cond["type"] == "Available" && cond["status"] == "True" {
					return nil
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (m *kubeMgmtCluster) getAdminKubeconfig(ctx context.Context, name string) ([]byte, error) {
	// The admin kubeconfig is in <name>-admin-kubeconfig secret in the
	// control plane namespace (clusters-<name>).
	cpNamespace := hostedClusterNamespace + "-" + name
	secret, err := m.clientset.CoreV1().Secrets(cpNamespace).Get(ctx, "admin-kubeconfig", metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get admin-kubeconfig secret in %s: %w", cpNamespace, err)
	}
	kc, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("admin-kubeconfig secret in %s missing 'kubeconfig' key", cpNamespace)
	}
	return kc, nil
}

func (m *kubeMgmtCluster) deleteNodePools(ctx context.Context, spec ClusterSpec) error {
	for _, np := range spec.NodePools {
		poolName := spec.Name + "-" + np.Name
		err := m.dynamicClient.Resource(nodePoolGVR).Namespace(hostedClusterNamespace).Delete(ctx, poolName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete NodePool %q: %w", poolName, err)
		}
	}
	return nil
}

func (m *kubeMgmtCluster) deleteHostedCluster(ctx context.Context, name string) error {
	err := m.dynamicClient.Resource(hostedClusterGVR).Namespace(hostedClusterNamespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (m *kubeMgmtCluster) waitForDeletion(ctx context.Context, name string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := m.dynamicClient.Resource(hostedClusterGVR).Namespace(hostedClusterNamespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("check HostedCluster %q deletion: %w", name, err)
		}

		time.Sleep(10 * time.Second)
	}
}

// toUnstructured converts a typed K8s object to unstructured with proper
// GVK set for the dynamic client.
func toUnstructured(obj interface{}, gvr schema.GroupVersionResource) (*unstructured.Unstructured, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &u.Object); err != nil {
		return nil, err
	}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvr.Group,
		Version: gvr.Version,
		Kind:    gvrToKind(gvr),
	})
	return u, nil
}

func gvrToKind(gvr schema.GroupVersionResource) string {
	switch gvr.Resource {
	case "hostedclusters":
		return "HostedCluster"
	case "nodepools":
		return "NodePool"
	default:
		return ""
	}
}

// Ensure kubeMgmtCluster satisfies the interface at compile time.
var _ mgmtCluster = (*kubeMgmtCluster)(nil)
