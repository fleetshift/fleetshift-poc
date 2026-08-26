package steps

import (
	"bytes"
	"context"
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

	kindClusterWaitTimeout = 2 * time.Minute
	// kindObjectAssertTimeout covers ConfigMap and Namespace get/gone polls.
	kindObjectAssertTimeout = 1 * time.Minute
	kindOIDCWaitTimeout     = 30 * time.Second
	kindOIDCProbePath       = "/api/v1/namespaces"
	kindAPIBodyLimit        = 1024
)

// UniqueKindClusterID returns an RFC1123 Kind cluster id with KindClusterIDPrefix
// so harness leftover cleanup matches this suite. Use this only for tests that
// create and delete a private cluster (lifecycle). Workload tests must call
// SharedKind or SharedKindPair instead.
func UniqueKindClusterID(t *testing.T) string {
	t.Helper()
	name := harness.KindClusterIDPrefix + uniqueHex8(t)
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
	t.Logf("kind OIDC check %s (%s)", clusterName, harness.HostKindClusterName(clusterName))
	pollKindAPI(t, f, clusterName, "", "kind OIDC", func(ctx context.Context, apiURL string, caPEM []byte, token string) error {
		return probeKindOIDC(ctx, apiURL, caPEM, token)
	})
}

// CreateNamespaceViaOIDC POSTs namespace to the Kind API with the suite
// access token — the run-as-me write path, not fleetctl delivery.
func CreateNamespaceViaOIDC(t *testing.T, f *harness.Fixture, clusterName, ns string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(ns).NotTo(gomega.BeEmpty())
	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]string{"name": ns},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	pollKindAPI(t, f, clusterName, "", "kind OIDC write", func(ctx context.Context, apiURL string, caPEM []byte, token string) error {
		st, resp, err := kindAPIRequest(ctx, apiURL, caPEM, token, http.MethodPost, kindOIDCProbePath, body)
		if err != nil {
			return err
		}
		if st == http.StatusCreated || st == http.StatusOK || st == http.StatusConflict {
			return nil
		}
		return fmt.Errorf("create namespace %s: %d %s", ns, st, strings.TrimSpace(string(resp)))
	})
}

// AssertNamespaceViaOIDC GETs namespace with the suite token.
func AssertNamespaceViaOIDC(t *testing.T, f *harness.Fixture, clusterName, ns string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(ns).NotTo(gomega.BeEmpty())
	path := kindOIDCProbePath + "/" + ns
	pollKindAPI(t, f, clusterName, "", "kind OIDC get ns", func(ctx context.Context, apiURL string, caPEM []byte, token string) error {
		st, resp, err := kindAPIRequest(ctx, apiURL, caPEM, token, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		if st == http.StatusOK {
			return nil
		}
		return fmt.Errorf("GET %s: %d %s", path, st, strings.TrimSpace(string(resp)))
	})
}

// AssertKindOIDCForbidden GETs /api/v1/namespaces with token from configDir
// and requires HTTP 403 (authenticated but not cluster-admin). KindHostAPI
// and transport errors are retried; any other HTTP status (including 401)
// fails the test immediately.
func AssertKindOIDCForbidden(t *testing.T, f *harness.Fixture, clusterName, configDir string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	token, err := f.AccessTokenFrom(configDir)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	hostName := harness.HostKindClusterName(clusterName)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		apiURL, caPEM, err := harness.KindHostAPI(hostName)
		if err != nil {
			log.logf("kind OIDC forbid %s: api: %v", clusterName, err)
		}
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		st, resp, err := kindAPIRequest(ctx, apiURL, caPEM, token, http.MethodGet, kindOIDCProbePath, nil)
		if err != nil {
			log.logf("kind OIDC forbid %s: %s", clusterName, clip(err.Error(), 80))
		}
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(st).To(gomega.Equal(http.StatusForbidden), strings.TrimSpace(string(resp)))
	}).WithTimeout(kindOIDCWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// pollKindAPI retries KindHostAPI + token + fn until fn succeeds.
// Empty configDir uses the suite AccessToken.
func pollKindAPI(t *testing.T, f *harness.Fixture, clusterName, configDir, label string, fn func(ctx context.Context, apiURL string, caPEM []byte, token string) error) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	log := newPollLog(t)
	g.Eventually(func() error {
		var (
			token string
			err   error
		)
		if configDir == "" {
			token, err = f.AccessToken()
		} else {
			token, err = f.AccessTokenFrom(configDir)
		}
		if err != nil {
			log.logf("%s %s: token: %v", label, clusterName, err)
			return err
		}
		apiURL, caPEM, err := harness.KindHostAPI(hostName)
		if err != nil {
			log.logf("%s %s: api: %v", label, clusterName, err)
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
		defer cancel()
		if err := fn(ctx, apiURL, caPEM, token); err != nil {
			log.logf("%s %s: %s", label, clusterName, clip(err.Error(), 80))
			return err
		}
		return nil
	}).WithTimeout(kindOIDCWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// probeKindOIDC GETs apiURL/api/v1/namespaces with Bearer token and the cluster CA.
func probeKindOIDC(ctx context.Context, apiURL string, caPEM []byte, token string) error {
	st, body, err := kindAPIRequest(ctx, apiURL, caPEM, token, http.MethodGet, kindOIDCProbePath, nil)
	if err != nil {
		return err
	}
	if st == http.StatusOK {
		return nil
	}
	return fmt.Errorf("kind OIDC GET %s: %d %s %s", kindOIDCProbePath, st, http.StatusText(st), strings.TrimSpace(string(body)))
}

// kindAPIRequest sends method+path to the Kind API with Bearer token and cluster CA.
func kindAPIRequest(ctx context.Context, apiURL string, caPEM []byte, token, method, path string, body []byte) (int, []byte, error) {
	if strings.TrimSpace(token) == "" {
		return 0, nil, fmt.Errorf("empty access token")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return 0, nil, fmt.Errorf("parse cluster CA: no certificates found")
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
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(apiURL, "/")+path, rdr)
	if err != nil {
		return 0, nil, fmt.Errorf("kind API request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("kind API: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, kindAPIBodyLimit))
	return resp.StatusCode, respBody, nil
}

// AssertConfigMapInNamespaceOnKindCluster checks that test-config exists in namespace
// with data from=fleetshift-e2e-backend.
func AssertConfigMapInNamespaceOnKindCluster(t *testing.T, f *harness.Fixture, clusterName, namespace string) {
	t.Helper()
	assertKindObjectPresent(t, f, clusterName, "configmap "+namespace+"/"+configMapName,
		func(gm gomega.Gomega, out []byte) {
			data, err := parseConfigMapData(out)
			gm.Expect(err).NotTo(gomega.HaveOccurred())
			gm.Expect(data).To(gomega.HaveKeyWithValue(configMapFromKey, configMapFromValue))
		},
		"get", "configmap", configMapName, "-n", namespace, "-o", "json")
}

// AssertConfigMapGoneOnKindCluster checks that test-config is absent from namespace.
func AssertConfigMapGoneOnKindCluster(t *testing.T, f *harness.Fixture, clusterName, namespace string) {
	t.Helper()
	assertKindObjectGone(t, f, clusterName, "configmap "+namespace+"/"+configMapName,
		"get", "configmap", configMapName, "-n", namespace)
}

// AssertNamespaceOnKindCluster checks that namespace exists on the Kind cluster.
func AssertNamespaceOnKindCluster(t *testing.T, f *harness.Fixture, clusterName, namespace string) {
	t.Helper()
	assertKindObjectPresent(t, f, clusterName, "namespace "+namespace,
		func(gm gomega.Gomega, out []byte) {
			name, err := parseMetadataName(out)
			gm.Expect(err).NotTo(gomega.HaveOccurred())
			gm.Expect(name).To(gomega.Equal(namespace))
		},
		"get", "namespace", namespace, "-o", "json")
}

// AssertNamespaceGoneOnKindCluster checks that namespace is absent from the Kind cluster.
func AssertNamespaceGoneOnKindCluster(t *testing.T, f *harness.Fixture, clusterName, namespace string) {
	t.Helper()
	assertKindObjectGone(t, f, clusterName, "namespace "+namespace, "get", "namespace", namespace)
}

// assertKindObjectPresent polls kubectl until the get succeeds, then runs check.
func assertKindObjectPresent(t *testing.T, f *harness.Fixture, clusterName, label string, check func(gomega.Gomega, []byte), kubectlArgs ...string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		out, err := kubectlOnKind(hostName, kubectlArgs...)
		if err != nil {
			log.logf("%s %s: %s", label, hostName, clip(strings.TrimSpace(string(out)), 120))
		}
		gm.Expect(err).NotTo(gomega.HaveOccurred(), strings.TrimSpace(string(out)))
		check(gm, out)
	}).WithTimeout(kindObjectAssertTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// assertKindObjectGone polls kubectl until the get is a Kubernetes NotFound.
func assertKindObjectGone(t *testing.T, f *harness.Fixture, clusterName, label string, kubectlArgs ...string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	hostName := harness.HostKindClusterName(clusterName)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		out, err := kubectlOnKind(hostName, kubectlArgs...)
		if err == nil {
			log.logf("%s %s: still present", label, hostName)
		} else {
			log.logf("%s %s: %s", label, hostName, clip(strings.TrimSpace(string(out)), 80))
		}
		gm.Expect(kubectlNotFound(out, err)).To(gomega.BeTrue(), strings.TrimSpace(string(out)))
	}).WithTimeout(kindObjectAssertTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}

// kubectlNotFound reports a kubectl get failure whose output is Kubernetes NotFound.
func kubectlNotFound(out []byte, err error) bool {
	return err != nil && strings.Contains(string(out), "NotFound")
}

// kubectlOnKind runs kubectl as cluster-admin inside the Kind control-plane container.
func kubectlOnKind(hostName string, args ...string) ([]byte, error) {
	id, err := harness.KindControlPlaneID(hostName)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	cmdArgs := append([]string{"exec", id, "kubectl", "--kubeconfig=/etc/kubernetes/admin.conf"}, args...)
	cmd := exec.CommandContext(ctx, "podman", cmdArgs...)
	return cmd.CombinedOutput()
}

// parseMetadataName unmarshals Kubernetes object JSON and returns .metadata.name.
func parseMetadataName(stdout []byte) (string, error) {
	var obj struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(stdout, &obj); err != nil {
		return "", fmt.Errorf("parse metadata name: %w", err)
	}
	return obj.Metadata.Name, nil
}

// DeleteKindCluster submits fleetctl resource delete for the Kind cluster. It does not wait until gone.
// It refuses the suite shared pool ids (kind-e2e-a / kind-e2e-b).
func DeleteKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	if isSharedKindCluster(name) {
		t.Fatalf("refusing to delete suite shared kind cluster %s", name)
	}
	deleteCluster(t, f, kindClusterType, name)
}

// CleanupKindCluster best-effort deletes the Kind cluster. For t.Cleanup;
// ignores errors (already gone, or create never succeeded). Skips the suite
// shared pool so a confused Cleanup cannot tear down SharedKind.
func CleanupKindCluster(t *testing.T, f *harness.Fixture, name string) {
	t.Helper()
	if isSharedKindCluster(name) {
		t.Logf("skipping cleanup of suite shared kind cluster %s", name)
		return
	}
	cleanupCluster(t, f, kindClusterType, name)
}

// WaitForKindClusterGone polls until resource get is gRPC NotFound and list does not contain the Kind cluster.
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
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ids, err := harness.KindNodeIDs(hostName)
		if err != nil {
			log.logf("host kind %s: %v", hostName, err)
		}
		log.logf("host kind %s nodes=%v", hostName, ids)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		gm.Expect(ids).To(gomega.BeEmpty())
	}).WithTimeout(kindClusterWaitTimeout).WithPolling(clusterPollInterval).Should(gomega.Succeed())
}
