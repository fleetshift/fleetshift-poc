package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"google.golang.org/grpc"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery/fake"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testenv"
)

// apiScenario is a hermetic public-API product journey fixture.
// Tests should read as a linear sequence of domain verbs; this type owns
// environment plumbing, auth, clients, polling, and Gomega.
type apiScenario struct {
	t      *testing.T
	g      Gomega
	env    *testenv.Env
	token  string
	conn   *grpc.ClientConn
	deploy pb.DeploymentServiceClient
	query  pb.ResourceQueryServiceClient
}

func newAPIScenario(t *testing.T, subject string) *apiScenario {
	t.Helper()
	g := NewWithT(t)
	start := time.Now()
	env := testenv.StartT(t)
	t.Cleanup(func() {
		env.Artifacts.RecordTestResult(t.Name(), !t.Failed(), nil, time.Since(start))
		testenv.AssertArtifactTestResult(t, env, t.Name())
	})

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: subject})
	g.Expect(err).NotTo(HaveOccurred())

	conn, err := env.DialGRPC()
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() { _ = conn.Close() })

	return &apiScenario{
		t:      t,
		g:      g,
		env:    env,
		token:  token,
		conn:   conn,
		deploy: pb.NewDeploymentServiceClient(conn),
		query:  pb.NewResourceQueryServiceClient(conn),
	}
}

func (s *apiScenario) GateDelivery() {
	s.t.Helper()
	s.g.Expect(s.env.Delivery.Gate()).To(Succeed())
}

func (s *apiScenario) ReleaseDelivery() {
	s.t.Helper()
	s.g.Expect(s.env.Delivery.Release()).To(Succeed())
}

func (s *apiScenario) ReportDeliveryProgress(message string) {
	s.t.Helper()
	s.g.Expect(s.env.Delivery.ReportProgress(message)).To(Succeed())
}

func (s *apiScenario) InjectTransientDeliveryFailure(n int) {
	s.t.Helper()
	s.g.Expect(s.env.Delivery.InjectTransientFailure(n)).To(Succeed())
}

func (s *apiScenario) CreateDeployment(id string) *pb.Deployment {
	s.t.Helper()
	ctx, cancel := testenv.CallContext(context.Background())
	defer cancel()

	created, err := s.deploy.CreateDeployment(
		testenv.AuthedContext(ctx, s.token),
		&pb.CreateDeploymentRequest{
			DeploymentId: id,
			Deployment: &pb.Deployment{
				ManifestStrategy: &pb.ManifestStrategy{
					Type: pb.ManifestStrategy_TYPE_INLINE,
					Manifests: []*pb.Manifest{{
						ManifestType: string(testenv.HermeticManifestType),
						Raw:          mustJSON(s.t, map[string]string{"kind": "Widget"}),
					}},
				},
				PlacementStrategy: &pb.PlacementStrategy{
					Type:      pb.PlacementStrategy_TYPE_STATIC,
					TargetIds: []string{string(testenv.HermeticTargetID)},
				},
			},
		},
	)
	s.g.Expect(err).NotTo(HaveOccurred())
	return created
}

func (s *apiScenario) ExpectCreating(dep *pb.Deployment) {
	s.t.Helper()
	s.g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_CREATING))
}

func (s *apiScenario) DeleteDeployment(id string) {
	s.t.Helper()
	ctx, cancel := testenv.CallContext(context.Background())
	defer cancel()

	_, err := s.deploy.DeleteDeployment(
		testenv.AuthedContext(ctx, s.token),
		&pb.DeleteDeploymentRequest{Name: deploymentName(id)},
	)
	s.g.Expect(err).NotTo(HaveOccurred())
}

func (s *apiScenario) WaitUntilCreatingAndReconciling(id string) {
	s.t.Helper()
	s.waitDeployment(id, func(g Gomega, dep *pb.Deployment) {
		g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_CREATING))
		g.Expect(dep.GetReconciling()).To(BeTrue())
		g.Expect(s.env.Delivery.Calls()).NotTo(BeEmpty())
	})
}

func (s *apiScenario) WaitUntilActive(id string) {
	s.t.Helper()
	s.waitDeployment(id, func(g Gomega, dep *pb.Deployment) {
		g.Expect(dep.GetState()).To(Equal(pb.Deployment_STATE_ACTIVE))
		g.Expect(dep.GetReconciling()).To(BeFalse())
	})
}

func (s *apiScenario) WaitUntilGone(id string) {
	s.t.Helper()
	pollCtx, pollCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer pollCancel()
	s.g.Eventually(func(g Gomega) {
		_, err := s.getDeployment(pollCtx, id)
		g.Expect(err).To(HaveOccurred())
	}).WithContext(pollCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())
}

func (s *apiScenario) WaitUntilProgressObserved(message string) {
	s.t.Helper()
	s.g.Eventually(func(g Gomega) {
		var progress int
		for _, r := range s.env.Delivery.Reports() {
			if r.Kind == fake.ReportEvent && r.Event != nil && r.Event.Message == message {
				progress++
			}
		}
		g.Expect(progress).To(BeNumerically(">=", 1))
	}).WithTimeout(testenv.DefaultEventualTimeout).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())
}

func (s *apiScenario) LabelInventory(name string, labels map[string]string) {
	s.t.Helper()
	ctx, cancel := testenv.CallContext(context.Background())
	defer cancel()
	s.g.Expect(s.env.Inventory.ReplaceLabels(ctx, domain.ResourceName(name), labels)).To(Succeed())
}

func (s *apiScenario) WaitUntilQueryFinds(filter, nameSuffix string) {
	s.t.Helper()
	pollCtx, pollCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer pollCancel()
	s.g.Eventually(func(g Gomega) {
		callCtx, cancel := testenv.PollCallContext(pollCtx)
		defer cancel()
		page, err := s.query.QueryResources(
			testenv.AuthedContext(callCtx, s.token),
			&pb.QueryResourcesRequest{
				Scope:    "-",
				Filter:   filter,
				PageSize: 10,
			},
		)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(page.GetResources()).NotTo(BeEmpty())
		g.Expect(page.GetResources()[0].GetName()).To(HaveSuffix(nameSuffix))
		g.Expect(page.GetResources()[0].GetResourceType()).To(Equal(string(testenv.HermeticInventoryType)))
	}).WithContext(pollCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())
}

func (s *apiScenario) ExpectDeliverAttemptsAtLeast(n int) {
	s.t.Helper()
	var delivers int
	for _, c := range s.env.Delivery.Calls() {
		if c.Kind == fake.CallDeliver {
			delivers++
		}
	}
	s.g.Expect(delivers).To(BeNumerically(">=", n), "product must retry after transient failure")
}

func (s *apiScenario) waitDeployment(id string, assert func(Gomega, *pb.Deployment)) {
	s.t.Helper()
	pollCtx, pollCancel := context.WithTimeout(context.Background(), testenv.DefaultEventualTimeout)
	defer pollCancel()
	s.g.Eventually(func(g Gomega) {
		dep, err := s.getDeployment(pollCtx, id)
		g.Expect(err).NotTo(HaveOccurred())
		assert(g, dep)
	}).WithContext(pollCtx).WithPolling(testenv.DefaultEventualPoll).Should(Succeed())
}

func (s *apiScenario) getDeployment(pollCtx context.Context, id string) (*pb.Deployment, error) {
	callCtx, cancel := testenv.PollCallContext(pollCtx)
	defer cancel()
	return s.deploy.GetDeployment(
		testenv.AuthedContext(callCtx, s.token),
		&pb.GetDeploymentRequest{Name: deploymentName(id)},
	)
}

func deploymentName(id string) string {
	return "deployments/" + id
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	return raw
}
