package steps

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	kindClusterType = "kind.fleetshift.v1/clusters"

	kindClusterWaitTimeout     = 1 * time.Minute
	kindConfigMapAssertTimeout = 30 * time.Second
	kindOIDCWaitTimeout        = 30 * time.Second
	kindOIDCProbePath          = "/api/v1/namespaces"
)

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

// WaitForKindClusterReady polls resource get until the Kind cluster is ACTIVE,
// unpaused, and conditions.Ready is True. fleetctl emits CREATING/ACTIVE/
// DELETING/FAILED; RUNNING is a UI display label, not this JSON field.
// FAILED and PausedAuth fail the test immediately.
func WaitForKindClusterReady(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	waitForClusterReady(t, f, kindClusterType, name, kindClusterWaitTimeout)
}

// WaitForKindOIDC presents the suite Dex access token to the Kind cluster
// API server — the same credential path unsigned kubernetes deliveries use.
// 401 means kube-apiserver has not yet accepted OIDC (JWKS / loopback-forward);
// this polls until GET /api/v1/namespaces returns 200.
func WaitForKindOIDC(t *testing.T, f *harness.Fixture, clusterName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	t.Logf("kind OIDC check %s (%s)", clusterName, hostName)
	g.Eventually(func() error {
		token, err := f.AccessToken()
		if err != nil {
			return err
		}
		apiURL, caPEM, err := harness.KindHostAPI(hostName)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		return probeKindOIDC(ctx, apiURL, caPEM, token)
	}).WithTimeout(kindOIDCWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// probeKindOIDC GETs apiURL/api/v1/namespaces with Bearer token and the cluster CA.
func probeKindOIDC(ctx context.Context, apiURL string, caPEM []byte, token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("empty access token")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("parse cluster CA: no certificates found")
	}
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    pool,
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(apiURL, "/")+kindOIDCProbePath, nil)
	if err != nil {
		return fmt.Errorf("kind OIDC request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("kind OIDC: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("kind OIDC GET %s: %s %s", kindOIDCProbePath, resp.Status, strings.TrimSpace(string(body)))
}

// AssertConfigMapOnKindCluster checks that default/test-config exists on the Kind cluster
// with data from=fleetshift-e2e-backend.
func AssertConfigMapOnKindCluster(t *testing.T, f *harness.Fixture, clusterName string) {
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
	}).WithTimeout(kindConfigMapAssertTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// DeleteKindCluster submits fleetctl resource delete for the Kind cluster. It does not wait until gone.
func DeleteKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	deleteCluster(t, f, kindClusterType, name)
}

// CleanupKindCluster best-effort deletes the Kind cluster. For t.Cleanup;
// ignores errors (already gone, or create never succeeded).
func CleanupKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	cleanupCluster(t, f, kindClusterType, name)
}

// WaitForKindClusterGone polls until resource get fails and list does not contain the Kind cluster.
func WaitForKindClusterGone(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	waitForClusterGone(t, f, kindClusterType, name, kindClusterWaitTimeout)
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
	}).WithTimeout(kindClusterWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}
