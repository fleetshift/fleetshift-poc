// Package steps holds Gomega action helpers for backend E2E scenarios.
// Each helper takes a *harness.Fixture.
package steps

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

// LoginAsOps asserts TestMain already logged in as ops (credentials.json exists).
func LoginAsOps(t *testing.T, f *harness.Fixture) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Expect(f.CredentialsPath()).To(gomega.BeAnExistingFile())
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

// errString returns err.Error, or "" if err is nil.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
