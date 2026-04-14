package ocp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	fleetshiftv1 "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Agent implements [domain.DeliveryAgent] for OCP clusters. It shells
// out to ocp-engine for provisioning, receives completion callbacks via
// gRPC, and bootstraps platform credentials on the new cluster.
type Agent struct {
	engineBinary string
	callbackAddr string
	vault        domain.Vault
	credentials  CredentialProvider
	oidcConfig   OIDCProviderConfig
	observer     AgentObserver
	tokenSigner  *CallbackTokenSigner
	provisions   sync.Map // clusterID → *provisionState
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithEngineBinary sets the path to the ocp-engine binary.
func WithEngineBinary(path string) AgentOption {
	return func(a *Agent) { a.engineBinary = path }
}

// WithCallbackAddr sets the gRPC callback address that ocp-engine
// will use to report completion/failure (e.g. "localhost:9443").
func WithCallbackAddr(addr string) AgentOption {
	return func(a *Agent) { a.callbackAddr = addr }
}

// WithVault sets the [domain.Vault] used for storing cluster secrets.
func WithVault(v domain.Vault) AgentOption {
	return func(a *Agent) { a.vault = v }
}

// WithCredentialProvider sets the [CredentialProvider] used for
// resolving AWS credentials and pull secrets.
func WithCredentialProvider(p CredentialProvider) AgentOption {
	return func(a *Agent) { a.credentials = p }
}

// WithOIDCConfig sets the OIDC provider configuration used for
// generating authentication manifests on provisioned clusters.
func WithOIDCConfig(cfg OIDCProviderConfig) AgentOption {
	return func(a *Agent) { a.oidcConfig = cfg }
}

// WithObserver sets the [AgentObserver] for delivery lifecycle events.
func WithObserver(o AgentObserver) AgentOption {
	return func(a *Agent) { a.observer = o }
}

// WithTokenSigner sets the [CallbackTokenSigner] used for minting and
// verifying callback JWTs.
func WithTokenSigner(s *CallbackTokenSigner) AgentOption {
	return func(a *Agent) { a.tokenSigner = s }
}

// NewAgent returns an Agent configured with the given options.
// Default engineBinary is "ocp-engine"; default observer is
// [NoOpAgentObserver].
func NewAgent(opts ...AgentOption) *Agent {
	a := &Agent{
		engineBinary: "ocp-engine",
		observer:     NoOpAgentObserver{},
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// CallbackServer returns the gRPC callback service implementation that
// ocp-engine subprocesses report to. The returned server shares the
// provisions map with the agent, so callbacks signal the correct
// in-flight delivery.
func (a *Agent) CallbackServer() fleetshiftv1.OCPEngineCallbackServiceServer {
	return &callbackServer{
		provisions:    &a.provisions,
		tokenVerifier: a.tokenSigner,
	}
}

// Deliver implements [domain.DeliveryAgent.Deliver]. It parses the
// cluster spec from manifests, resolves credentials, writes cluster.yaml,
// and launches ocp-engine as a subprocess. The actual result is delivered
// asynchronously via the callback server and [domain.DeliverySignaler].
func (a *Agent) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	_ domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	signaler *domain.DeliverySignaler,
) (domain.DeliveryResult, error) {
	// 1. Parse ClusterSpec from manifests
	spec, err := ParseClusterSpec(manifests)
	if err != nil {
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("parse cluster spec: %v", err),
		}, nil
	}

	clusterID := spec.Name

	// 2. Start observer probe
	ctx, probe := a.observer.ClusterDeliverStarted(ctx, clusterID)
	defer probe.End()

	// 3. Read region and role_arn from target.Properties
	region := target.Properties["region"]
	roleARN := target.Properties["role_arn"]
	if region == "" {
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: "target property 'region' is required",
		}, nil
	}
	if roleARN == "" {
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: "target property 'role_arn' is required",
		}, nil
	}

	// 4. Resolve AWS credentials
	awsCreds, err := a.credentials.ResolveAWS(ctx, AWSCredentialRequest{
		Region:  region,
		RoleARN: roleARN,
		Auth:    auth,
	})
	if err != nil {
		probe.Error(err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("resolve AWS credentials: %v", err),
		}, nil
	}
	probe.CredentialsResolved("aws")

	// 5. Resolve pull secret
	pullSecret, err := a.credentials.ResolvePullSecret(ctx, PullSecretRequest{
		Auth: auth,
	})
	if err != nil {
		probe.Error(err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("resolve pull secret: %v", err),
		}, nil
	}

	// 6. Generate SSH key
	sshPublicKey, sshPrivateKey, err := GenerateSSHKey()
	if err != nil {
		probe.Error(err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("generate SSH key: %v", err),
		}, nil
	}

	// 7. Prepare work directory with pull secret and cluster.yaml
	configPath, workDir, err := prepareWorkDir(clusterID, spec, region, pullSecret, sshPublicKey)
	if err != nil {
		probe.Error(err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: err.Error(),
		}, nil
	}

	// 8. Register provisionState
	state := &provisionState{done: make(chan struct{})}
	a.provisions.Store(clusterID, state)

	// 9. Launch background goroutine
	go a.deliverAsync(ctx, clusterID, configPath, workDir, awsCreds, sshPrivateKey, auth, signaler, state)

	// 10. Return accepted
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// deliverAsync runs ocp-engine and waits for the callback or process exit.
func (a *Agent) deliverAsync(
	ctx context.Context,
	clusterID string,
	configPath string,
	workDir string,
	awsCreds *AWSCredentials,
	sshPrivateKey []byte,
	auth domain.DeliveryAuth,
	signaler *domain.DeliverySignaler,
	state *provisionState,
) {
	defer a.provisions.Delete(clusterID)
	defer os.RemoveAll(workDir)

	// Generate callback JWT
	token, err := a.tokenSigner.Sign(clusterID, provisionTimeout())
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("generate callback token: %v", err),
		})
		return
	}

	timeout := "2h"

	// Build and run ocp-engine subprocess
	cmd := exec.CommandContext(ctx, a.engineBinary,
		"provision",
		"--config", configPath,
		"--timeout", timeout,
		"--callback-url", a.callbackAddr,
		"--cluster-id", clusterID,
	)
	cmd.Env = append(os.Environ(), awsCreds.Env()...)
	cmd.Env = append(cmd.Env, "OCP_CALLBACK_TOKEN="+token)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	// Start the subprocess
	if err := cmd.Start(); err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("start ocp-engine: %v", err),
		})
		return
	}

	// Wait for process exit in a separate goroutine
	processDone := make(chan error, 1)
	go func() {
		processDone <- cmd.Wait()
	}()

	// Wait for either callback or process exit
	select {
	case <-state.done:
		// Callback received — check completion vs failure
	case err := <-processDone:
		// Process exited before callback — check if callback already fired
		select {
		case <-state.done:
			// Callback fired just before process exit, proceed normally
		default:
			// Process crashed without callback
			msg := "ocp-engine exited without reporting completion"
			if err != nil {
				msg = fmt.Sprintf("ocp-engine exited with error: %v", err)
			}
			signaler.Done(ctx, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: msg,
			})
			return
		}
	case <-ctx.Done():
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("context cancelled: %v", ctx.Err()),
		})
		return
	}

	// Process callback result
	if state.failure != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("ocp-engine failed in phase %s: %s", state.failure.GetPhase(), state.failure.GetFailureMessage()),
		})
		return
	}

	if state.completion == nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: "callback received but no completion data",
		})
		return
	}

	// Handle successful completion
	output, err := a.handleCompletion(ctx, clusterID, state.completion, sshPrivateKey, auth)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("post-provision bootstrap failed: %v", err),
		})
		return
	}

	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		Message:            fmt.Sprintf("OCP cluster %s provisioned successfully", clusterID),
		ProvisionedTargets: []domain.ProvisionedTarget{output.Target()},
		ProducedSecrets:    output.Secrets(),
	}

	signaler.Done(ctx, result)
}

// handleCompletion performs post-provision bootstrap and builds the
// [ClusterOutput] from the completion callback data.
func (a *Agent) handleCompletion(
	ctx context.Context,
	clusterName string,
	completion *fleetshiftv1.OCPEngineCompletionRequest,
	sshPrivateKey []byte,
	auth domain.DeliveryAuth,
) (*ClusterOutput, error) {
	targetID := domain.TargetID("k8s-" + completion.GetInfraId())

	// Determine issuer URL from OIDC config
	var issuerURL domain.IssuerURL
	if a.oidcConfig.IssuerURL != "" {
		issuerURL = domain.IssuerURL(a.oidcConfig.IssuerURL)
	}

	// Bootstrap the cluster: create namespace, SA, RBAC, generate token
	bootstrapResult, err := BootstrapCluster(
		ctx,
		completion.GetKubeconfig(),
		targetID,
		auth.Caller,
		issuerURL,
	)
	if err != nil {
		return nil, fmt.Errorf("bootstrap cluster: %w", err)
	}

	// Build ClusterOutput
	output := &ClusterOutput{
		TargetID:      targetID,
		Name:          clusterName,
		APIServer:     completion.GetApiServer(),
		CACert:        completion.GetCaCert(),
		InfraID:       completion.GetInfraId(),
		ClusterID:     completion.GetClusterUuid(),
		Region:        completion.GetRegion(),
		SATokenRef:    bootstrapResult.SATokenRef,
		SAToken:       bootstrapResult.SAToken,
		KubeconfigRef: bootstrapResult.KubeconfigRef,
		Kubeconfig:    completion.GetKubeconfig(),
		SSHKeyRef:     bootstrapResult.SSHKeyRef,
		SSHPrivateKey: sshPrivateKey,
	}

	return output, nil
}

// Remove implements [domain.DeliveryAgent.Remove]. It destroys the OCP
// cluster via ocp-engine destroy, cleans up ccoctl IAM resources, and
// removes vault entries.
func (a *Agent) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	_ domain.DeliveryID,
	_ []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	_ *domain.DeliverySignaler,
) error {
	// 1. Read infra_id, region, role_arn from target.Properties
	infraID := target.Properties["infra_id"]
	region := target.Properties["region"]
	roleARN := target.Properties["role_arn"]
	clusterID := target.Properties["cluster_id"]

	if infraID == "" {
		return fmt.Errorf("target property 'infra_id' is required for removal")
	}
	if region == "" {
		return fmt.Errorf("target property 'region' is required for removal")
	}
	if roleARN == "" {
		return fmt.Errorf("target property 'role_arn' is required for removal")
	}

	// 2. Resolve AWS credentials (1h session for destroy)
	awsCreds, err := a.credentials.ResolveAWS(ctx, AWSCredentialRequest{
		Region:  region,
		RoleARN: roleARN,
		Auth:    auth,
	})
	if err != nil {
		return fmt.Errorf("resolve AWS credentials for destroy: %w", err)
	}

	// 3. Create temp work dir
	workDir, err := os.MkdirTemp("", fmt.Sprintf("ocp-destroy-%s-", infraID))
	if err != nil {
		return fmt.Errorf("create work directory: %w", err)
	}
	defer os.RemoveAll(workDir)

	// 4. Write reconstructed metadata.json from target properties
	if err := writeDestroyMetadata(workDir, infraID, clusterID, region); err != nil {
		return err
	}

	// 5. Run ocp-engine destroy
	cmd := exec.CommandContext(ctx, a.engineBinary,
		"destroy",
		"--work-dir", workDir,
	)
	cmd.Env = append(os.Environ(), awsCreds.Env()...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ocp-engine destroy failed: %w", err)
	}

	// 6. Run ccoctl delete to clean up IAM resources (log warning if fails)
	clusterName := target.Name
	if clusterName == "" {
		clusterName = infraID
	}
	cco := &CCOctlOrchestrator{
		WorkDir: workDir,
		AWSEnv:  awsCreds.Env(),
	}
	// ccoctl binary path may not be available during destroy; attempt
	// deletion only if ccoctl is on PATH.
	ccoPath, ccoErr := exec.LookPath("ccoctl")
	if ccoErr == nil {
		cco.BinaryPath = ccoPath
		if err := cco.Delete(ctx, clusterName, region); err != nil {
			slog.Warn("ccoctl delete failed (IAM resources may need manual cleanup)",
				"error", err,
				"cluster", clusterName,
				"region", region,
			)
		}
	}

	// 7. Clean up vault entries
	targetID := domain.TargetID("k8s-" + infraID)
	if a.vault != nil {
		vaultRefs := []domain.SecretRef{
			domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID)),
			domain.SecretRef(fmt.Sprintf("targets/%s/kubeconfig", targetID)),
			domain.SecretRef(fmt.Sprintf("targets/%s/ssh-key", targetID)),
		}
		for _, ref := range vaultRefs {
			if err := a.vault.Delete(ctx, ref); err != nil {
				slog.Warn("failed to delete vault secret",
					"ref", ref,
					"error", err,
				)
			}
		}
	}

	return nil
}

// provisionTimeout returns the default provision timeout as a duration.
func provisionTimeout() time.Duration {
	return defaultProvisionSTSDuration
}

// prepareWorkDir creates a temp directory containing the pull secret and
// cluster.yaml config. Returns the config file path and work directory.
// The caller is responsible for cleaning up the work directory.
func prepareWorkDir(clusterID string, spec *ClusterSpec, region string, pullSecret, sshPublicKey []byte) (configPath, workDir string, err error) {
	workDir, err = os.MkdirTemp("", "ocp-provision-"+clusterID+"-")
	if err != nil {
		return "", "", fmt.Errorf("create work directory: %w", err)
	}

	pullSecretFile := filepath.Join(workDir, "pull-secret.json")
	if err := os.WriteFile(pullSecretFile, pullSecret, 0600); err != nil {
		os.RemoveAll(workDir)
		return "", "", fmt.Errorf("write pull secret: %w", err)
	}

	clusterYAML, err := BuildClusterYAML(spec, region, pullSecretFile, strings.TrimSpace(string(sshPublicKey)))
	if err != nil {
		os.RemoveAll(workDir)
		return "", "", fmt.Errorf("build cluster.yaml: %w", err)
	}

	configPath = filepath.Join(workDir, "cluster.yaml")
	if err := os.WriteFile(configPath, clusterYAML, 0600); err != nil {
		os.RemoveAll(workDir)
		return "", "", fmt.Errorf("write cluster.yaml: %w", err)
	}

	return configPath, workDir, nil
}

// writeDestroyMetadata writes a reconstructed metadata.json to the work
// directory for ocp-engine destroy.
func writeDestroyMetadata(workDir, infraID, clusterID, region string) error {
	metadata := map[string]any{
		"infraID":   infraID,
		"clusterID": clusterID,
		"aws": map[string]any{
			"region":     region,
			"identifier": []map[string]string{{"infraID": infraID}},
		},
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata.json: %w", err)
	}
	return os.WriteFile(filepath.Join(workDir, "metadata.json"), metadataJSON, 0600)
}
