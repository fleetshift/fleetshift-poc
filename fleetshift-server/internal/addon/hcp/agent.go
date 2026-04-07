// Package hcp implements a [domain.DeliveryAgent] for managing HCP
// (Hosted Control Plane) clusters via AWS and HyperShift. Manifests
// are interpreted as HCP cluster specifications; delivery creates or
// updates clusters, and removal deletes them.
package hcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for HCP-managed targets.
const TargetType domain.TargetType = "hcp"

// ClusterResourceType is the [domain.ResourceType] for HCP cluster
// specifications.
const ClusterResourceType domain.ResourceType = "api.hcp.cluster"

// KubernetesTargetType is the [domain.TargetType] for Kubernetes
// clusters provisioned by the HCP addon.
const KubernetesTargetType domain.TargetType = "kubernetes"

// ClusterSpec is the manifest payload accepted by the HCP delivery
// agent.
type ClusterSpec struct {
	Name                     string         `json:"name"`
	InfraID                  string         `json:"infraID,omitempty"`
	RoleARN                  string         `json:"roleArn"`
	Region                   string         `json:"region,omitempty"`
	BaseDomain               string         `json:"baseDomain,omitempty"`
	ReleaseImage             string         `json:"releaseImage,omitempty"`
	NodePools                []NodePoolSpec `json:"nodePools"`
	IDP                      *IDPSpec       `json:"idp,omitempty"`
	ControlPlaneAvailability string         `json:"controlPlaneAvailability,omitempty"`
	InfraAvailability        string         `json:"infraAvailability,omitempty"`
}

// NodePoolSpec describes a node pool within an HCP cluster.
type NodePoolSpec struct {
	Name         string   `json:"name"`
	Replicas     int32    `json:"replicas"`
	InstanceType string   `json:"instanceType,omitempty"`
	Arch         string   `json:"arch,omitempty"`
	Zones        []string `json:"zones,omitempty"`
}

// IDPSpec configures an identity provider for the HCP cluster.
type IDPSpec struct {
	Name   string `json:"name"`
	Issuer string `json:"issuer"`
}

// validateManifests parses and validates a slice of manifests as HCP
// cluster specs. It applies defaults for optional fields.
func validateManifests(manifests []domain.Manifest) ([]ClusterSpec, error) {
	specs := make([]ClusterSpec, len(manifests))
	for i, m := range manifests {
		if err := json.Unmarshal(m.Raw, &specs[i]); err != nil {
			return nil, fmt.Errorf("unmarshal hcp cluster spec: %w", err)
		}
		if specs[i].Name == "" {
			return nil, fmt.Errorf("%w: hcp cluster spec requires a name", domain.ErrInvalidArgument)
		}
		if specs[i].RoleARN == "" {
			return nil, fmt.Errorf("%w: hcp cluster spec requires a roleArn", domain.ErrInvalidArgument)
		}
		if specs[i].BaseDomain == "" {
			return nil, fmt.Errorf("%w: hcp cluster spec requires a baseDomain", domain.ErrInvalidArgument)
		}
		if len(specs[i].NodePools) == 0 {
			return nil, fmt.Errorf("%w: hcp cluster spec requires at least one nodePool", domain.ErrInvalidArgument)
		}

		// Apply defaults.
		if specs[i].ControlPlaneAvailability == "" {
			specs[i].ControlPlaneAvailability = "HighlyAvailable"
		}
		if specs[i].InfraAvailability == "" {
			specs[i].InfraAvailability = "HighlyAvailable"
		}
		for j := range specs[i].NodePools {
			if specs[i].NodePools[j].InstanceType == "" {
				specs[i].NodePools[j].InstanceType = "m6i.xlarge"
			}
			if specs[i].NodePools[j].Arch == "" {
				specs[i].NodePools[j].Arch = "amd64"
			}
		}
	}
	return specs, nil
}

// AgentConfig holds non-secret configuration for the HCP delivery agent.
type AgentConfig struct {
	MgmtKubeconfig []byte // admin kubeconfig for the HyperShift management cluster
	PullSecret     []byte // container image pull secret
	S3Bucket       string // S3 bucket for OIDC discovery documents
	AWSRegion      string // default AWS region
}

// Agent implements [domain.DeliveryAgent] for HCP clusters.
type Agent struct {
	config   AgentConfig
	ec2      EC2API
	iam      IAMAPI
	route53  Route53API
	observer AgentObserver
	verifier *attestation.Verifier
	mgmt     mgmtCluster
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithObserver sets the [AgentObserver] for delivery lifecycle events.
func WithObserver(o AgentObserver) AgentOption {
	return func(a *Agent) { a.observer = o }
}

// WithAttestationVerifier configures optional attestation verification.
func WithAttestationVerifier(v *attestation.Verifier) AgentOption {
	return func(a *Agent) { a.verifier = v }
}

// withMgmtCluster overrides the management cluster client (for testing).
func withMgmtCluster(m mgmtCluster) AgentOption {
	return func(a *Agent) { a.mgmt = m }
}

// NewAgent returns an Agent configured with the given AWS clients and options.
func NewAgent(config AgentConfig, ec2Client EC2API, iamClient IAMAPI, r53Client Route53API, opts ...AgentOption) *Agent {
	a := &Agent{
		config:  config,
		ec2:     ec2Client,
		iam:     iamClient,
		route53: r53Client,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

func (a *Agent) agentObserver() AgentObserver {
	if a.observer != nil {
		return a.observer
	}
	return NoOpAgentObserver{}
}

// Deliver validates manifests synchronously, optionally verifies
// attestation, then returns [domain.DeliveryStateAccepted] and performs
// the actual provisioning in a background goroutine.
func (a *Agent) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	specs, err := validateManifests(manifests)
	if err != nil {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed}, err
	}

	if att != nil && a.verifier != nil {
		if err := a.verifier.Verify(ctx, att); err != nil {
			return domain.DeliveryResult{
				State:   domain.DeliveryStateAuthFailed,
				Message: fmt.Sprintf("attestation verification failed: %v", err),
			}, nil
		}
	}

	asyncCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 1*time.Hour)
	go func() {
		defer cancel()
		a.deliverAsync(asyncCtx, specs, signaler)
	}()

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// Remove deletes HyperShift resources described by the manifests from
// the management cluster. Destroy functions for AWS infra/IAM are
// called if available (added by Task 11).
func (a *Agent) Remove(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	specs, err := validateManifests(manifests)
	if err != nil {
		return fmt.Errorf("validate manifests: %w", err)
	}

	mc, err := a.mgmtClient()
	if err != nil {
		return fmt.Errorf("management cluster client: %w", err)
	}

	for _, spec := range specs {
		if err := mc.deleteNodePools(ctx, spec); err != nil {
			return fmt.Errorf("delete node pools for %q: %w", spec.Name, err)
		}
		if err := mc.deleteHostedCluster(ctx, spec.Name); err != nil {
			return fmt.Errorf("delete hosted cluster %q: %w", spec.Name, err)
		}

		// Wait for the HyperShift operator to finish cleaning up cloud
		// resources (load balancers, ENIs, etc.) before destroying the VPC.
		if err := mc.waitForDeletion(ctx, spec.Name); err != nil {
			return fmt.Errorf("wait for hosted cluster %q deletion: %w", spec.Name, err)
		}

		// Clean up AWS IAM resources (roles, instance profile, OIDC provider).
		infraID := spec.InfraID
		if infraID == "" {
			infraID = spec.Name
		}
		if err := DestroyIAM(ctx, a.iam, infraID, nil); err != nil {
			return fmt.Errorf("destroy IAM for %q: %w", spec.Name, err)
		}

		// Clean up AWS infrastructure (VPC, subnets, NAT gateways, etc.)
		// via tag-based resource discovery.
		if err := DestroyInfra(ctx, a.ec2, a.route53, infraID, nil); err != nil {
			return fmt.Errorf("destroy infrastructure for %q: %w", spec.Name, err)
		}

		// TODO: clean up vault secrets for target hcp-{name} (requires secret store access)
	}
	return nil
}

func (a *Agent) deliverAsync(ctx context.Context, specs []ClusterSpec, signaler *domain.DeliverySignaler) {
	defer func() {
		if r := recover(); r != nil {
			failDelivery(ctx, signaler, "internal error: %v", r)
		}
	}()

	var outputs []ClusterOutput

	for _, spec := range specs {
		out, ok := a.deliverCluster(ctx, spec, signaler)
		if !ok {
			return
		}
		if out != nil {
			outputs = append(outputs, *out)
		}
	}

	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	for _, out := range outputs {
		result.ProvisionedTargets = append(result.ProvisionedTargets, out.Target())
		result.ProducedSecrets = append(result.ProducedSecrets, out.Secrets()...)
	}
	signaler.Done(ctx, result)
}

func (a *Agent) deliverCluster(ctx context.Context, spec ClusterSpec, signaler *domain.DeliverySignaler) (*ClusterOutput, bool) {
	// Apply defaults that other functions depend on.
	if spec.InfraID == "" {
		spec.InfraID = spec.Name
	}
	if spec.Region == "" {
		spec.Region = a.config.AWSRegion
	}

	ctx, probe := a.agentObserver().ClusterDeliverStarted(ctx, spec.Name)
	defer probe.End()

	// 1. Create AWS infrastructure.
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Creating AWS infrastructure for %q", spec.Name),
	})
	infraSpec := InfraSpec{
		Name:       spec.Name,
		InfraID:    spec.InfraID,
		Region:     spec.Region,
		BaseDomain: spec.BaseDomain,
		Zones:      collectZones(spec),
	}
	infra, err := CreateInfra(ctx, a.ec2, a.route53, infraSpec)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "create infrastructure for %q: %v", spec.Name, err)
		return nil, false
	}
	probe.InfraCreated(infra.VPCID)

	// 2. Create IAM roles and OIDC provider.
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Creating IAM roles for %q", spec.Name),
	})
	iamParams := IAMParams{
		InfraID:  spec.InfraID,
		Region:   spec.Region,
		S3Bucket: a.config.S3Bucket,
	}
	iamOut, err := CreateIAM(ctx, a.iam, iamParams)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "create IAM for %q: %v", spec.Name, err)
		return nil, false
	}
	probe.IAMCreated(iamOut.OIDCProviderArn)

	// 3. Build HyperShift resources.
	platformCfg := PlatformConfig{PullSecret: a.config.PullSecret}
	hc := BuildHostedCluster(spec, *infra, *iamOut, platformCfg)
	nodePools := BuildNodePools(spec, *infra)
	secrets, err := BuildSecrets(spec, platformCfg)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "build secrets for %q: %v", spec.Name, err)
		return nil, false
	}

	// 4. Apply to management cluster.
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Applying HyperShift resources to management cluster for %q", spec.Name),
	})
	mc, err := a.mgmtClient()
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "management cluster client for %q: %v", spec.Name, err)
		return nil, false
	}
	if err := mc.applyResources(ctx, hc, nodePools, secrets); err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "apply HyperShift resources for %q: %v", spec.Name, err)
		return nil, false
	}
	probe.CRDsApplied()

	// 5. Wait for HostedCluster to become available.
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Waiting for HostedCluster %q to become available", spec.Name),
	})
	if err := mc.waitForAvailable(ctx, spec.Name); err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "wait for HostedCluster %q: %v", spec.Name, err)
		return nil, false
	}

	// 6. Read admin kubeconfig from control plane namespace.
	guestKubeconfig, err := mc.getAdminKubeconfig(ctx, spec.Name)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "get admin kubeconfig for %q: %v", spec.Name, err)
		return nil, false
	}

	apiServer, caCert, err := extractClusterConnInfo(guestKubeconfig)
	if err != nil {
		probe.Error(err)
		failDelivery(ctx, signaler, "extract connection info for %q: %v", spec.Name, err)
		return nil, false
	}
	probe.HostedClusterAvailable(apiServer)

	// 7. Bootstrap platform SA on the guest cluster.
	targetID := domain.TargetID("hcp-" + spec.Name)
	out := ClusterOutput{
		TargetID:  targetID,
		Name:      spec.Name,
		APIServer: apiServer,
		CACert:    caCert,
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Bootstrapping platform ServiceAccount on %q", spec.Name),
	})
	ref, token, saErr := bootstrapPlatformSA(ctx, guestKubeconfig, targetID)
	if saErr != nil {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventWarning,
			Message: fmt.Sprintf("platform SA bootstrap on %q: %v (attested delivery will not work)", spec.Name, saErr),
		})
	} else {
		out.SATokenRef = ref
		out.SAToken = token
	}

	probe.TargetRegistered(string(targetID))
	return &out, true
}

func failDelivery(ctx context.Context, signaler *domain.DeliverySignaler, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	signaler.Emit(ctx, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventError,
		Message: msg,
	})
	signaler.Done(ctx, domain.DeliveryResult{
		State:   domain.DeliveryStateFailed,
		Message: msg,
	})
}

// collectZones gathers unique availability zones from all node pools in
// the spec. If none are specified, returns a single default zone.
func collectZones(spec ClusterSpec) []string {
	seen := make(map[string]struct{})
	var zones []string
	for _, np := range spec.NodePools {
		for _, z := range np.Zones {
			if _, ok := seen[z]; !ok {
				seen[z] = struct{}{}
				zones = append(zones, z)
			}
		}
	}
	if len(zones) == 0 {
		return []string{spec.Region + "a"}
	}
	return zones
}

// extractClusterConnInfo parses a kubeconfig to extract API server and CA cert.
func extractClusterConnInfo(kubeconfig []byte) (string, []byte, error) {
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return "", nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	for _, cluster := range cfg.Clusters {
		return cluster.Server, cluster.CertificateAuthorityData, nil
	}
	return "", nil, fmt.Errorf("kubeconfig contains no clusters")
}

func (a *Agent) mgmtClient() (mgmtCluster, error) {
	if a.mgmt != nil {
		return a.mgmt, nil
	}
	return newKubeMgmtCluster(a.config.MgmtKubeconfig)
}
