package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const (
	gatewayCommandTimeout = 10 * time.Second
	gatewayBodyLimit      = 1 << 20
)

// AssertLivezOK GETs the HTTPS /livez probe without a bearer token.
func AssertLivezOK(t *testing.T, f *harness.Fixture) {
	t.Helper()
	assertHealthOK(t, f, "/livez")
}

// AssertReadyzOK GETs the HTTPS /readyz probe without a bearer token.
func AssertReadyzOK(t *testing.T, f *harness.Fixture) {
	t.Helper()
	assertHealthOK(t, f, "/readyz")
}

// AssertDeploymentsUnauthorized GETs /v1/deployments without a bearer token.
func AssertDeploymentsUnauthorized(t *testing.T, f *harness.Fixture) {
	t.Helper()
	assertDeploymentsGET(t, f, "", http.StatusUnauthorized)
}

// AssertDeploymentsUnauthorizedBadToken GETs /v1/deployments with a garbage bearer token.
func AssertDeploymentsUnauthorizedBadToken(t *testing.T, f *harness.Fixture) {
	t.Helper()
	assertDeploymentsGET(t, f, "not-a-jwt", http.StatusUnauthorized)
}

// AssertDeploymentsIncludeBootstrap GETs /v1/deployments with the suite token
// and requires the bootstrap deployment to be listed.
func AssertDeploymentsIncludeBootstrap(t *testing.T, f *harness.Fixture, bootstrapID string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	token, err := f.AccessToken()
	g.Expect(err).NotTo(gomega.HaveOccurred())
	body := assertDeploymentsGET(t, f, token, http.StatusOK)
	names, err := parseGatewayDeploymentNames(body)
	g.Expect(err).NotTo(gomega.HaveOccurred(), string(body))
	g.Expect(names).To(gomega.ContainElement(jsonDeploymentName(bootstrapID)), string(body))
}

// assertDeploymentsGET GETs /v1/deployments and requires want status.
func assertDeploymentsGET(t *testing.T, f *harness.Fixture, token string, want int) []byte {
	t.Helper()
	st, body, err := gatewayGET(t, f, "/v1/deployments", token)
	g := gomega.NewWithT(t)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(st).To(gomega.Equal(want), string(body))
	return body
}

// assertHealthOK GETs path and requires HTTP 200 with body "ok".
func assertHealthOK(t *testing.T, f *harness.Fixture, path string) {
	t.Helper()
	g := gomega.NewWithT(t)
	st, body, err := gatewayGET(t, f, path, "")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(st).To(gomega.Equal(http.StatusOK))
	g.Expect(strings.TrimSpace(string(body))).To(gomega.Equal("ok"))
}

// gatewayGET GETs UIOrigin+path using the sandbox CA. token is sent as Bearer
// when non-empty.
func gatewayGET(t *testing.T, f *harness.Fixture, path, token string) (int, []byte, error) {
	t.Helper()
	client, err := f.HTTPSClient()
	if err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), gatewayCommandTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, harness.UIOrigin+path, nil)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, gatewayBodyLimit))
	if err != nil {
		return resp.StatusCode, body, err
	}
	return resp.StatusCode, body, nil
}

// parseGatewayDeploymentNames reads names from grpc-gateway ListDeployments JSON.
func parseGatewayDeploymentNames(body []byte) ([]string, error) {
	var resp struct {
		Deployments []struct {
			Name string `json:"name"`
		} `json:"deployments"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse gateway deployments: %w", err)
	}
	names := make([]string, 0, len(resp.Deployments))
	for _, d := range resp.Deployments {
		names = append(names, d.Name)
	}
	return names, nil
}
