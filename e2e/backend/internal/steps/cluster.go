package steps

import (
	"context"
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
	configMapNamespace     = "default"
	configMapName          = "test-config"

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

// ConfigMapDeploymentID is the fleetctl deployment id for the ConfigMap sent to clusterName.
func ConfigMapDeploymentID(clusterName string) string {
	return "cm-" + clusterName
}

// kubernetesTargetID is the kubernetes target ID for clusterName (k8s-{id}).
func kubernetesTargetID(clusterName string) string {
	return kubernetesTargetPrefix + clusterName
}

// CreateConfigMapDeployment submits fleetctl deployment create for an unsigned
// ConfigMap onto the cluster's kubernetes target. It does not wait until Active.
func CreateConfigMapDeployment(t *testing.T, f *harness.Fixture, clusterName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())

	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]string{
			"name":      configMapName,
			"namespace": configMapNamespace,
		},
		"data": map[string]string{
			"from": "fleetshift-e2e-backend",
		},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	manifestPath := filepath.Join(t.TempDir(), "configmap.json")
	g.Expect(os.WriteFile(manifestPath, manifest, 0o600)).To(gomega.Succeed())

	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "deployment", "create",
		"--id", ConfigMapDeploymentID(clusterName),
		"--manifest-file", manifestPath,
		"--resource-type", kubernetesResourceType,
		"--placement-type", "static",
		"--target-ids", kubernetesTargetID(clusterName),
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

// waitForClusterGone polls until resource get fails and list does not contain the cluster.
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
		gm.Expect(get.Err).To(gomega.HaveOccurred())
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
