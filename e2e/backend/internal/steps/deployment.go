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
	deploymentCommandTimeout = 10 * time.Second
	deploymentWaitTimeout    = 30 * time.Second
	deploymentPollInterval   = 500 * time.Millisecond
)

// deploymentView is the JSON subset parsed from fleetctl deployment list/get.
type deploymentView struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// WaitForListedDeployment polls `deployment list` until wantName appears.
func WaitForListedDeployment(t *testing.T, f *harness.Fixture, wantName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "deployment", "list")
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), res.Stderr)
		deps, err := parseDeploymentList(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		names := make([]string, len(deps))
		for i, d := range deps {
			names[i] = d.Name
		}
		gm.Expect(names).To(gomega.ContainElement(jsonDeploymentName(wantName)))
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// WaitForDeploymentActive polls `deployment get` until the named deployment is STATE_ACTIVE.
func WaitForDeploymentActive(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "deployment", "get", id)
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), res.Stderr)
		dep, err := parseDeployment(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		gm.Expect(dep.Name).To(gomega.Equal(jsonDeploymentName(id)))
		gm.Expect(dep.State).To(gomega.Equal("STATE_ACTIVE"))
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// parseDeploymentList unmarshals fleetctl deployment list JSON.
func parseDeploymentList(stdout string) ([]deploymentView, error) {
	var deps []deploymentView
	if err := json.Unmarshal([]byte(stdout), &deps); err != nil {
		return nil, fmt.Errorf("parse deployment list: %w", err)
	}
	return deps, nil
}

// parseDeployment unmarshals fleetctl deployment get JSON.
func parseDeployment(stdout string) (deploymentView, error) {
	var dep deploymentView
	if err := json.Unmarshal([]byte(stdout), &dep); err != nil {
		return deploymentView{}, fmt.Errorf("parse deployment: %w", err)
	}
	return dep, nil
}

// jsonDeploymentName is the fleetctl JSON `name` for id (proto resource name).
func jsonDeploymentName(id string) string {
	if strings.HasPrefix(id, "deployments/") {
		return id
	}
	return "deployments/" + id
}
