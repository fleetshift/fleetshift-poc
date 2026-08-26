// Package steps holds Gomega action helpers for backend E2E scenarios.
// Each helper takes a *harness.Fixture.
package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const (
	// OpsEmail and DevEmail are the bundled Dex sandbox login emails.
	OpsEmail = "ops@fleetshift.local"
	DevEmail = "dev@fleetshift.local"
)

// LoginAsOps asserts TestMain logged in as ops (inspect-token email).
func LoginAsOps(t *testing.T, f *harness.Fixture) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	AssertInspectTokenEmail(t, f, f.ConfigDir(), OpsEmail)
}

// AssertCredentialsIsolated checks that a command with an empty --config-dir
// cannot use the suite tokens.
func AssertCredentialsIsolated(t *testing.T, f *harness.Fixture) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Expect(f.CredentialsPath()).To(gomega.BeAnExistingFile())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := f.RunUnauthenticated(t, ctx, "deployment", "list")
	g.Expect(res.Err).To(gomega.HaveOccurred())
	combined := res.Stderr + " " + errString(res.Err)
	g.Expect(strings.ToLower(combined)).To(gomega.ContainSubstring("unauthenticated"))
}

// LoginAsDev logs persona "dev" into a new config dir and returns that directory.
func LoginAsDev(t *testing.T, f *harness.Fixture) string {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	dir, err := f.LoginAs(harness.PersonaDev)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = f.AccessTokenFrom(dir)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return dir
}

// AssertInspectTokenEmail checks `auth inspect-token` JSON for email.
func AssertInspectTokenEmail(t *testing.T, f *harness.Fixture, configDir, wantEmail string) {
	t.Helper()
	res := fleetctlAs(t, f, configDir, "auth", "inspect-token")
	g := gomega.NewWithT(t)
	email, err := tokenEmail(res.Stdout)
	g.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	g.Expect(email).To(gomega.Equal(wantEmail), fleetctlDetail(res))
}

// AssertDeploymentListAs checks that `deployment list` succeeds with configDir.
func AssertDeploymentListAs(t *testing.T, f *harness.Fixture, configDir string) {
	t.Helper()
	fleetctlAs(t, f, configDir, "deployment", "list")
}

// fleetctlAs runs fleetctl with configDir and requires success.
func fleetctlAs(t *testing.T, f *harness.Fixture, configDir string, args ...string) harness.FleetctlResult {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := f.RunWithConfigDir(ctx, configDir, args...)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	return res
}

// tokenEmail reads email from fleetctl auth inspect-token JSON.
func tokenEmail(stdout string) (string, error) {
	var v struct {
		AccessToken *inspectTokenJWT `json:"access_token"`
		IDToken     *inspectTokenJWT `json:"id_token"`
	}
	if err := json.Unmarshal([]byte(stdout), &v); err != nil {
		return "", fmt.Errorf("parse inspect-token: %w", err)
	}
	for _, tok := range []*inspectTokenJWT{v.AccessToken, v.IDToken} {
		if tok == nil {
			continue
		}
		if email, ok := tok.Claims["email"].(string); ok && strings.TrimSpace(email) != "" {
			return email, nil
		}
	}
	return "", fmt.Errorf("inspect-token JSON has no email claim")
}

// inspectTokenJWT is the decoded JWT object in fleetctl auth inspect-token JSON.
type inspectTokenJWT struct {
	Claims map[string]any `json:"claims"`
}

// errString returns err.Error, or "" if err is nil.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
