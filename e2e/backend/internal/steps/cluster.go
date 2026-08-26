package steps

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const (
	kubernetesTargetPrefix = "k8s-"
	kubernetesResourceType = "kubernetes"
	configMapName          = "test-config"
	configMapFromKey       = "from"
	configMapFromValue     = "fleetshift-e2e-backend"

	clusterCommandTimeout = 10 * time.Second
	clusterPollInterval   = 500 * time.Millisecond

	clusterStateActive = "ACTIVE"
	clusterStateFailed = "FAILED"
	clusterReadyTrue   = "True"
)

// clusterCondition is the JSON subset of a fleetctl cluster condition.
type clusterCondition struct {
	Status string `json:"status"`
}

// clusterView is the JSON subset parsed from fleetctl resource get/list.
type clusterView struct {
	Name        string                      `json:"name"`
	State       string                      `json:"state"`
	PauseReason string                      `json:"pauseReason"`
	Conditions  map[string]clusterCondition `json:"conditions"`
}

// UniqueID returns prefix + "-" + 8 hex chars. Use for per-test deployment
// ids and Kubernetes namespaces on the shared Kind pool (do not key those
// off the cluster name).
func UniqueID(t *testing.T, prefix string) string {
	t.Helper()
	if strings.TrimSpace(prefix) == "" {
		t.Fatal("UniqueID prefix is required")
	}
	id := prefix + "-" + uniqueHex8(t)
	t.Logf("%s", id)
	return id
}

// uniqueHex8 returns 8 lowercase hex characters.
func uniqueHex8(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", b)
}

// kubernetesTargetID is the kubernetes target ID for clusterName (k8s-{id}).
func kubernetesTargetID(clusterName string) string {
	return kubernetesTargetPrefix + clusterName
}

// kubernetesTargetIDs joins kubernetes target IDs for clusterNames.
func kubernetesTargetIDs(clusterNames ...string) string {
	ids := make([]string, len(clusterNames))
	for i, n := range clusterNames {
		ids[i] = kubernetesTargetID(n)
	}
	return strings.Join(ids, ",")
}

// CreateConfigMapDeploymentOn submits an unsigned ConfigMap in namespace onto
// the kubernetes targets for clusterNames, using the fixture's ops --config-dir.
// It does not wait until Active.
func CreateConfigMapDeploymentOn(t *testing.T, f *harness.Fixture, id, namespace string, clusterNames ...string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	CreateConfigMapDeploymentAs(t, f, f.ConfigDir(), id, namespace, clusterNames...)
}

// CreateConfigMapDeploymentAs submits an unsigned ConfigMap in namespace onto
// the kubernetes targets for clusterNames, using configDir as fleetctl --config-dir.
// It does not wait until Active.
func CreateConfigMapDeploymentAs(t *testing.T, f *harness.Fixture, configDir, id, namespace string, clusterNames ...string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Expect(clusterNames).NotTo(gomega.BeEmpty())

	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]string{
			"name":      configMapName,
			"namespace": namespace,
		},
		"data": map[string]string{
			configMapFromKey: configMapFromValue,
		},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	createKubernetesDeployment(t, f, configDir, id, kubernetesTargetIDs(clusterNames...), manifest)
}

// CreateNamespaceDeploymentOn submits an unsigned Namespace onto the kubernetes
// targets for clusterNames, using the fixture's ops --config-dir.
// It does not wait until Active.
func CreateNamespaceDeploymentOn(t *testing.T, f *harness.Fixture, id, namespace string, clusterNames ...string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Expect(namespace).NotTo(gomega.BeEmpty())
	g.Expect(clusterNames).NotTo(gomega.BeEmpty())

	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]string{
			"name": namespace,
		},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	createKubernetesDeployment(t, f, f.ConfigDir(), id, kubernetesTargetIDs(clusterNames...), manifest)
}

// createKubernetesDeployment writes manifest to a temp file and runs
// `deployment create` with static placement on targetIDs.
func createKubernetesDeployment(t *testing.T, f *harness.Fixture, configDir, id, targetIDs string, manifest []byte) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())

	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	g.Expect(os.WriteFile(manifestPath, manifest, 0o600)).To(gomega.Succeed())

	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.RunWithConfigDir(ctx, configDir, "deployment", "create",
		"--id", id,
		"--manifest-file", manifestPath,
		"--resource-type", kubernetesResourceType,
		"--placement-type", "static",
		"--target-ids", targetIDs,
	)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
}

// waitForClusterReady polls fleetctl resource get until the named cluster of
// resourceType is ACTIVE, unpaused, and conditions.Ready is True.
// fleetctl emits CREATING/ACTIVE/DELETING/FAILED; RUNNING is a UI display
// label, not this JSON field. FAILED and PausedAuth fail the test immediately.
func waitForClusterReady(t *testing.T, f *harness.Fixture, resourceType, name string, timeout time.Duration) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "resource", "get", resourceType, name)
		if res.Err != nil {
			log.logf("cluster %s get: %s", name, fleetctlDetail(res))
		}
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		cl, err := parseCluster(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		gm.Expect(cl.Name).To(gomega.Equal(jsonClusterName(name)), fleetctlDetail(res))
		log.logf("cluster %s state=%s pauseReason=%s ready=%s",
			cl.Name, cl.State, cl.PauseReason, cl.Conditions["Ready"].Status)
		if msg := clusterTerminalFailure(cl); msg != "" {
			t.Fatalf("%s\n%s", msg, fleetctlDetail(res))
		}
		gm.Expect(clusterReady(cl)).To(gomega.BeTrue(),
			"state=%s pauseReason=%s ready=%s\n%s",
			cl.State, cl.PauseReason, cl.Conditions["Ready"].Status, fleetctlDetail(res))
	}).WithTimeout(timeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// deleteCluster submits fleetctl resource delete for resourceType/name. It does not wait until gone.
func deleteCluster(t *testing.T, f *harness.Fixture, resourceType, name string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "resource", "delete", resourceType, name)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
}

// cleanupCluster best-effort deletes resourceType/name. For t.Cleanup; ignores
// errors (already gone, or create never succeeded).
func cleanupCluster(t *testing.T, f *harness.Fixture, resourceType, name string) {
	t.Helper()
	if f == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	_ = f.Run(ctx, "resource", "delete", resourceType, name)
}

// waitForClusterGone polls until resource get is gRPC NotFound and list does not contain the cluster.
func waitForClusterGone(t *testing.T, f *harness.Fixture, resourceType, name string, timeout time.Duration) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	want := jsonClusterName(name)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		get := f.Run(ctx, "resource", "get", resourceType, name)
		list := f.Run(ctx, "resource", "list", resourceType)
		gm.Expect(list.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(list))
		clusters, err := parseClusterList(list.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		names := make([]string, len(clusters))
		listed := false
		for i, c := range clusters {
			names[i] = c.Name
			if c.Name == want {
				listed = true
			}
		}
		log.logf("cluster %s getOK=%t listed=%t", name, get.Err == nil, listed)
		gm.Expect(rpcNotFound(get)).To(gomega.BeTrue(), fleetctlDetail(get))
		gm.Expect(names).NotTo(gomega.ContainElement(want))
	}).WithTimeout(timeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// clusterReady reports whether fleetctl JSON shows an unpaused ACTIVE cluster
// whose Ready condition is True (API reachable via inventory).
func clusterReady(c clusterView) bool {
	if strings.TrimSpace(c.PauseReason) != "" {
		return false
	}
	if c.State != clusterStateActive {
		return false
	}
	ready, ok := c.Conditions["Ready"]
	return ok && ready.Status == clusterReadyTrue
}

// clusterTerminalFailure is a non-empty reason when polling for ready should stop.
func clusterTerminalFailure(c clusterView) string {
	if c.State == clusterStateFailed {
		if p := strings.TrimSpace(c.PauseReason); p != "" {
			return "cluster " + c.Name + " FAILED: " + p
		}
		return "cluster " + c.Name + " FAILED"
	}
	if p := strings.TrimSpace(c.PauseReason); p != "" {
		return "cluster " + c.Name + " paused (" + c.State + "): " + p
	}
	return ""
}

// parseCluster unmarshals fleetctl resource get JSON.
func parseCluster(stdout string) (clusterView, error) {
	var cl clusterView
	if err := json.Unmarshal([]byte(stdout), &cl); err != nil {
		return clusterView{}, fmt.Errorf("parse cluster: %w", err)
	}
	return cl, nil
}

// parseClusterList unmarshals fleetctl resource list JSON.
func parseClusterList(stdout string) ([]clusterView, error) {
	var clusters []clusterView
	if err := json.Unmarshal([]byte(stdout), &clusters); err != nil {
		return nil, fmt.Errorf("parse cluster list: %w", err)
	}
	return clusters, nil
}

// parseConfigMapData unmarshals kubectl configmap JSON and returns .data.
func parseConfigMapData(stdout []byte) (map[string]string, error) {
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(stdout, &cm); err != nil {
		return nil, fmt.Errorf("parse configmap: %w", err)
	}
	return cm.Data, nil
}

// jsonClusterName is the fleetctl JSON `name` for a cluster id.
func jsonClusterName(id string) string {
	if strings.HasPrefix(id, "clusters/") {
		return id
	}
	return "clusters/" + id
}
