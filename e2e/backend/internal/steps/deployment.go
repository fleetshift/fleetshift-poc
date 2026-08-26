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
	deploymentWaitTimeout    = 1 * time.Minute
	deploymentPollInterval   = 500 * time.Millisecond

	deploymentStateActive = "STATE_ACTIVE"
	deploymentStateFailed = "STATE_FAILED"

	// deliveryAuthPause is the orchestration sentinel prefix on pauseReason
	// ("delivery auth failed: pausing for fresh credentials").
	deliveryAuthPause = "delivery auth failed"
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
	pollDeploymentGet(t, f, id, func(gm gomega.Gomega, dep deploymentView, res harness.FleetctlResult) {
		if msg := deploymentTerminalFailure(dep); msg != "" {
			t.Fatalf("%s\n%s", msg, fleetctlDetail(res))
		}
		gm.Expect(dep.State).To(gomega.Equal(deploymentStateActive), fleetctlDetail(res))
	})
}

// WaitForDeploymentResumedActive polls until STATE_ACTIVE with an empty pauseReason.
// STATE_FAILED fails immediately. A leftover pauseReason is retried (resume in flight).
func WaitForDeploymentResumedActive(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	pollDeploymentGet(t, f, id, func(gm gomega.Gomega, dep deploymentView, res harness.FleetctlResult) {
		if dep.State == deploymentStateFailed {
			t.Fatalf("%s\n%s", deploymentTerminalFailure(dep), fleetctlDetail(res))
		}
		gm.Expect(deploymentPaused(dep)).To(gomega.BeFalse(), fleetctlDetail(res))
		gm.Expect(dep.State).To(gomega.Equal(deploymentStateActive), fleetctlDetail(res))
	})
}

// WaitForDeploymentPaused polls `deployment get` until pauseReason is a
// delivery auth failure. STATE_FAILED fails the test immediately. Other
// pause reasons keep polling (they are not the PausedAuth this helper asserts).
func WaitForDeploymentPaused(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	pollDeploymentGet(t, f, id, func(gm gomega.Gomega, dep deploymentView, res harness.FleetctlResult) {
		if dep.State == deploymentStateFailed {
			t.Fatalf("deployment %s STATE_FAILED before pause\n%s", dep.Name, fleetctlDetail(res))
		}
		gm.Expect(dep.PauseReason).To(gomega.ContainSubstring(deliveryAuthPause), fleetctlDetail(res))
	})
}

// pollDeploymentGet polls `deployment get` until check succeeds.
func pollDeploymentGet(t *testing.T, f *harness.Fixture, id string, check func(gomega.Gomega, deploymentView, harness.FleetctlResult)) {
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
		check(gm, dep, res)
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// ResumeDeployment submits `deployment resume` for id. It does not wait until Active.
func ResumeDeployment(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	runDeployment(t, f, "resume", id)
}

// DeleteDeployment submits `deployment delete` for id. It does not wait until gone.
func DeleteDeployment(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	runDeployment(t, f, "delete", id)
}

// runDeployment runs `deployment verb id` and requires success.
func runDeployment(t *testing.T, f *harness.Fixture, verb, id string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "deployment", verb, id)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
}

// CleanupDeployment best-effort deletes id. For t.Cleanup; ignores errors
// (already gone, or create never succeeded).
func CleanupDeployment(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	if f == nil || strings.TrimSpace(id) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
	defer cancel()
	_ = f.Run(ctx, "deployment", "delete", id)
}

// WaitForDeploymentGone polls until `deployment get` is gRPC NotFound and list does not contain id.
func WaitForDeploymentGone(t *testing.T, f *harness.Fixture, id string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	want := jsonDeploymentName(id)
	log := newPollLog(t)
	g.Eventually(func(gm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), deploymentCommandTimeout)
		defer cancel()
		get := f.Run(ctx, "deployment", "get", id)
		list := f.Run(ctx, "deployment", "list")
		gm.Expect(list.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(list))
		deps, err := parseDeploymentList(list.Stdout)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		names := make([]string, len(deps))
		listed := false
		for i, d := range deps {
			names[i] = d.Name
			if d.Name == want {
				listed = true
			}
		}
		log.logf("deployment %s getOK=%t listed=%t", id, get.Err == nil, listed)
		gm.Expect(rpcNotFound(get)).To(gomega.BeTrue(), fleetctlDetail(get))
		gm.Expect(names).NotTo(gomega.ContainElement(want))
	}).WithTimeout(deploymentWaitTimeout).WithPolling(deploymentPollInterval).Should(gomega.Succeed())
}

// deploymentPaused reports a non-empty pauseReason (PausedAuth).
func deploymentPaused(dep deploymentView) bool {
	return strings.TrimSpace(dep.PauseReason) != ""
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
