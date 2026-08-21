package steps

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const kindOIDCProbePath = "/api/v1/namespaces"

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
