package steps

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const (
	kindClusterType        = "kind.fleetshift.v1/clusters"
	kubernetesTargetPrefix = "k8s-"
	kubernetesResourceType = "kubernetes"
	configMapNamespace     = "default"
	configMapName          = "test-config"

	clusterCommandTimeout  = 10 * time.Second
	clusterWaitTimeout     = 1 * time.Minute
	clusterPollInterval    = 500 * time.Millisecond
	configMapAssertTimeout = 30 * time.Second
	kindOIDCWaitTimeout    = 30 * time.Second

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

// UniqueKindClusterID returns an RFC1123 Kind cluster id with KindClusterIDPrefix
// so harness leftover cleanup matches this suite.
func UniqueKindClusterID(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%s%x", harness.KindClusterIDPrefix, b)
	t.Logf("kind cluster %s", name)
	return name
}

// ConfigMapDeploymentID is the fleetctl deployment id for the ConfigMap sent to clusterName.
func ConfigMapDeploymentID(clusterName string) string {
	return "cm-" + clusterName
}

// kubernetesTargetID is the kubernetes target ID for clusterName (k8s-{id}).
func kubernetesTargetID(clusterName string) string {
	return kubernetesTargetPrefix + clusterName
}

// CreateKindCluster submits fleetctl resource create for a Kind cluster. It does not wait until ready.
func CreateKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())

	spec, err := json.Marshal(map[string]string{"name": name})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	specPath := filepath.Join(t.TempDir(), "spec.json")
	g.Expect(os.WriteFile(specPath, spec, 0o600)).To(gomega.Succeed())

	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "resource", "create", kindClusterType,
		"--id", name, "--spec-file", specPath)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	cl, err := parseCluster(res.Stdout)
	g.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	g.Expect(cl.Name).To(gomega.Equal(jsonClusterName(name)))
}

// WaitForClusterReady polls resource get until the Kind cluster is ACTIVE,
// unpaused, and conditions.Ready is True. fleetctl emits CREATING/ACTIVE/
// DELETING/FAILED; RUNNING is a UI display label, not this JSON field.
// FAILED and PausedAuth fail the test immediately.
func WaitForClusterReady(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "resource", "get", kindClusterType, name)
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		cl, err := parseCluster(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		gm.Expect(cl.Name).To(gomega.Equal(jsonClusterName(name)), fleetctlDetail(res))
		if msg := clusterTerminalFailure(cl); msg != "" {
			t.Fatalf("%s\n%s", msg, fleetctlDetail(res))
		}
		gm.Expect(clusterReady(cl)).To(gomega.BeTrue(),
			"state=%s pauseReason=%s ready=%s\n%s",
			cl.State, cl.PauseReason, cl.Conditions["Ready"].Status, fleetctlDetail(res))
	}).WithTimeout(clusterWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// CreateConfigMapDeployment submits fleetctl deployment create for an unsigned
// ConfigMap onto the Kind cluster. It does not wait until Active.
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

// AssertConfigMapOnCluster checks that default/test-config exists on the Kind cluster
// with data from=fleetshift-e2e-backend.
func AssertConfigMapOnCluster(t *testing.T, f *harness.Fixture, clusterName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	g.Eventually(func(gm gomega.Gomega) {
		id, err := harness.KindControlPlaneID(hostName)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "podman", "exec", id,
			"kubectl", "--kubeconfig=/etc/kubernetes/admin.conf",
			"get", "configmap", configMapName,
			"-n", configMapNamespace, "-o", "json")
		out, err := cmd.CombinedOutput()
		gm.Expect(err).NotTo(gomega.HaveOccurred(), strings.TrimSpace(string(out)))
		data, err := parseConfigMapData(out)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		gm.Expect(data).To(gomega.HaveKeyWithValue("from", "fleetshift-e2e-backend"))
	}).WithTimeout(configMapAssertTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// DeleteKindCluster submits fleetctl resource delete for the Kind cluster. It does not wait until gone.
func DeleteKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "resource", "delete", kindClusterType, name)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
}

// CleanupKindCluster best-effort deletes the Kind cluster. For t.Cleanup;
// ignores errors (already gone, or create never succeeded).
func CleanupKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	if f == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	_ = f.Run(ctx, "resource", "delete", kindClusterType, name)
}

// WaitForClusterGone polls until resource get fails and list does not contain the cluster.
func WaitForClusterGone(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	want := jsonClusterName(name)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		get := f.Run(ctx, "resource", "get", kindClusterType, name)
		gm.Expect(get.Err).To(gomega.HaveOccurred())
		list := f.Run(ctx, "resource", "list", kindClusterType)
		gm.Expect(list.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(list))
		clusters, err := parseClusterList(list.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		names := make([]string, len(clusters))
		for i, c := range clusters {
			names[i] = c.Name
		}
		gm.Expect(names).NotTo(gomega.ContainElement(want))
	}).WithTimeout(clusterWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// AssertHostKindClusterGone checks the host has no Kind node containers for this cluster.
func AssertHostKindClusterGone(t *testing.T, f *harness.Fixture, clusterName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	g.Eventually(func(gm gomega.Gomega) {
		ids, err := harness.KindNodeIDs(hostName)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		gm.Expect(ids).To(gomega.BeEmpty())
	}).WithTimeout(clusterWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// clusterReady reports whether fleetctl JSON shows an unpaused ACTIVE Kind
// cluster whose Ready condition is True (API reachable via inventory).
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

// jsonClusterName is the fleetctl JSON `name` for a Kind cluster id.
func jsonClusterName(id string) string {
	if strings.HasPrefix(id, "clusters/") {
		return id
	}
	return "clusters/" + id
}
