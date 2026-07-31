// Package e2e holds Go end-to-end scenario tests for FleetShift.
package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery/fake"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testenv"
)

// TestHermeticAPI_GatedProgressInventoryDelete covers:
// authenticate → create → gate and observe progress → release success →
// observe controlled inventory → delete.
func TestHermeticAPI_GatedProgressInventoryDelete(t *testing.T) {
	g := NewWithT(t)
	start := time.Now()
	env := testenv.StartT(t)
	t.Cleanup(func() {
		env.Artifacts.RecordTestResult(t.Name(), !t.Failed(), nil, time.Since(start))
		testenv.AssertArtifactTestResult(t, env, t.Name())
	})

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: "ops-user"})
	g.Expect(err).NotTo(HaveOccurred())

	conn, err := env.DialGRPC()
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewDeploymentServiceClient(conn)
	query := pb.NewResourceQueryServiceClient(conn)

	g.Expect(env.Delivery.Gate()).To(Succeed())

	created, err := createHermeticDeployment(t, client, token, "gated-1")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(created.GetState()).To(Equal(pb.Deployment_STATE_CREATING))

	pollCtx, pollCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer pollCancel()
	g.Eventually(func(g Gomega) {
		dep, err := getDeployment(client, token, "deployments/gated-1", pollCtx)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_CREATING))
		g.Expect(dep.GetReconciling()).To(BeTrue())
		g.Expect(env.Delivery.Calls()).NotTo(BeEmpty())
	}).WithContext(pollCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())

	g.Expect(env.Delivery.ReportProgress("applying manifests")).To(Succeed())
	g.Eventually(func(g Gomega) {
		var progress int
		for _, r := range env.Delivery.Reports() {
			if r.Kind == fake.ReportEvent && r.Event != nil && r.Event.Message == "applying manifests" {
				progress++
			}
		}
		g.Expect(progress).To(BeNumerically(">=", 1))
	}).WithTimeout(testenv.DefaultEventualTimeout).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())

	g.Expect(env.Delivery.Release()).To(Succeed())

	activeCtx, activeCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer activeCancel()
	g.Eventually(func(g Gomega) {
		dep, err := getDeployment(client, token, "deployments/gated-1", activeCtx)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_ACTIVE))
		g.Expect(dep.GetReconciling()).To(BeFalse())
	}).WithContext(activeCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())

	invCtx, invCancel := testenv.CallContext(context.Background())
	defer invCancel()
	g.Expect(env.Inventory.ReplaceLabels(invCtx, "widgets/w1", map[string]string{"env": "e2e"})).To(Succeed())

	queryCtx, queryCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer queryCancel()
	g.Eventually(func(g Gomega) {
		callCtx, cancel := testenv.PollCallContext(queryCtx)
		defer cancel()
		page, err := query.QueryResources(
			testenv.AuthedContext(callCtx, token),
			&pb.QueryResourcesRequest{
				Scope:    "-",
				Filter:   `resourceType == "hermetic.fleetshift.io/Widget" && resource.localLabels["env"] == "e2e"`,
				PageSize: 10,
			},
		)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(page.GetResources()).NotTo(BeEmpty())
		g.Expect(page.GetResources()[0].GetName()).To(HaveSuffix("widgets/w1"))
		g.Expect(page.GetResources()[0].GetResourceType()).To(Equal(string(testenv.HermeticInventoryType)))
	}).WithContext(queryCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())

	delCtx, delCancel := testenv.CallContext(context.Background())
	defer delCancel()
	_, err = client.DeleteDeployment(
		testenv.AuthedContext(delCtx, token),
		&pb.DeleteDeploymentRequest{Name: "deployments/gated-1"},
	)
	g.Expect(err).NotTo(HaveOccurred())

	goneCtx, goneCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer goneCancel()
	g.Eventually(func(g Gomega) {
		_, err := getDeployment(client, token, "deployments/gated-1", goneCtx)
		g.Expect(err).To(HaveOccurred())
	}).WithContext(goneCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())
}

// TestHermeticAPI_TransientDeliveryFailureRetry covers:
// authenticate → create → inject transient delivery failure → observe
// product retry → succeed and observe the result.
func TestHermeticAPI_TransientDeliveryFailureRetry(t *testing.T) {
	g := NewWithT(t)
	start := time.Now()
	env := testenv.StartT(t)
	t.Cleanup(func() {
		env.Artifacts.RecordTestResult(t.Name(), !t.Failed(), nil, time.Since(start))
		testenv.AssertArtifactTestResult(t, env, t.Name())
	})

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: "retry-user"})
	g.Expect(err).NotTo(HaveOccurred())

	conn, err := env.DialGRPC()
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewDeploymentServiceClient(conn)

	g.Expect(env.Delivery.InjectTransientFailure(1)).To(Succeed())

	_, err = createHermeticDeployment(t, client, token, "retry-1")
	g.Expect(err).NotTo(HaveOccurred())

	activeCtx, activeCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer activeCancel()
	g.Eventually(func(g Gomega) {
		dep, err := getDeployment(client, token, "deployments/retry-1", activeCtx)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_ACTIVE))
	}).WithContext(activeCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())

	var delivers int
	for _, c := range env.Delivery.Calls() {
		if c.Kind == fake.CallDeliver {
			delivers++
		}
	}
	g.Expect(delivers).To(BeNumerically(">=", 2), "product must retry after transient failure")
}

func TestHermeticAPI_PollCallContextCancelsBlockedIO(t *testing.T) {
	g := NewWithT(t)
	env := testenv.StartT(t)

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: "timeout-user"})
	g.Expect(err).NotTo(HaveOccurred())
	conn, err := env.DialGRPC()
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() { _ = conn.Close() })

	// Parent poll budget already expired; PollCallContext must inherit it
	// without relying on wall-clock sleeps.
	pollCtx, pollCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer pollCancel()

	callCtx, callCancel := testenv.PollCallContext(pollCtx)
	defer callCancel()
	g.Expect(callCtx.Err()).To(MatchError(context.DeadlineExceeded))

	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(
		testenv.AuthedContext(callCtx, token),
		&pb.ListDeploymentsRequest{},
	)
	g.Expect(err).To(HaveOccurred())
	g.Expect(callCtx.Err()).To(MatchError(context.DeadlineExceeded))
}

func createHermeticDeployment(
	t *testing.T,
	client pb.DeploymentServiceClient,
	token string,
	id string,
) (*pb.Deployment, error) {
	t.Helper()
	ctx, cancel := testenv.CallContext(context.Background())
	defer cancel()

	return client.CreateDeployment(
		testenv.AuthedContext(ctx, token),
		&pb.CreateDeploymentRequest{
			DeploymentId: id,
			Deployment: &pb.Deployment{
				ManifestStrategy: &pb.ManifestStrategy{
					Type: pb.ManifestStrategy_TYPE_INLINE,
					Manifests: []*pb.Manifest{{
						ManifestType: string(testenv.HermeticManifestType),
						Raw:          mustJSON(t, map[string]string{"kind": "Widget"}),
					}},
				},
				PlacementStrategy: &pb.PlacementStrategy{
					Type:      pb.PlacementStrategy_TYPE_STATIC,
					TargetIds: []string{string(testenv.HermeticTargetID)},
				},
			},
		},
	)
}

func getDeployment(
	client pb.DeploymentServiceClient,
	token string,
	name string,
	pollCtx context.Context,
) (*pb.Deployment, error) {
	callCtx, cancel := testenv.PollCallContext(pollCtx)
	defer cancel()

	return client.GetDeployment(
		testenv.AuthedContext(callCtx, token),
		&pb.GetDeploymentRequest{Name: name},
	)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	return raw
}
