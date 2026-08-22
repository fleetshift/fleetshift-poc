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

	deploymentStateActive = "STATE_ACTIVE"
	deploymentStateFailed = "STATE_FAILED"
)

// deploymentView is the JSON subset parsed from fleetctl deployment list/get.
type deploymentView struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	PauseReason string `json:"pauseReason"`
}

// WaitForListedDeployment polls `deployment list` until wantName appears.
func WaitForListedDeployment(t *testing.T, f *harness.Fixture, wantName string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	want := jsonDeploymentName(wantName)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "deployment", "list")
		if res.Err != nil {
			log.logf("deployment list: %s", fleetctlDetail(res))
		}
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		deps, err := parseDeploymentList(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		names := make([]string, len(deps))
		for i, d := range deps {
			names[i] = d.Name
		}
		log.logf("deployment list: want %s have %s", want, strings.Join(names, ","))
		gm.Expect(names).To(gomega.ContainElement(want), fleetctlDetail(res))
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// WaitForDeploymentActive polls `deployment get` until the named deployment is STATE_ACTIVE.
// PausedAuth (non-empty pauseReason) and STATE_FAILED fail the test immediately.
func WaitForDeploymentActive(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
		defer cancel()
		res := f.Run(ctx, "deployment", "get", id)
		if res.Err != nil {
			log.logf("deployment %s get: %s", id, fleetctlDetail(res))
		}
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		dep, err := parseDeployment(res.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		gm.Expect(dep.Name).To(gomega.Equal(jsonDeploymentName(id)), fleetctlDetail(res))
		log.logf("deployment %s state=%s pauseReason=%s", dep.Name, dep.State, dep.PauseReason)
		if msg := deploymentTerminalFailure(dep); msg != "" {
			t.Fatalf("%s\n%s", msg, fleetctlDetail(res))
		}
		gm.Expect(dep.State).To(gomega.Equal(deploymentStateActive), fleetctlDetail(res))
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// deploymentTerminalFailure is a non-empty reason when polling for Active should stop.
func deploymentTerminalFailure(dep deploymentView) string {
	if dep.State == deploymentStateFailed {
		if p := strings.TrimSpace(dep.PauseReason); p != "" {
			return "deployment " + dep.Name + " STATE_FAILED: " + p
		}
		return "deployment " + dep.Name + " STATE_FAILED"
	}
	if p := strings.TrimSpace(dep.PauseReason); p != "" {
		return "deployment " + dep.Name + " paused (" + dep.State + "): " + p
	}
	return ""
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
