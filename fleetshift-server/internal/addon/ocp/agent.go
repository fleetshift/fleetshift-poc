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

	"google.golang.org/grpc"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Agent implements [domain.DeliveryAgent] for OCP clusters. It shells
// out to ocp-engine for provisioning, receives completion callbacks via
// gRPC, and bootstraps platform credentials on the new cluster.
type Agent struct {
	engineBinary     string
	callbackAddr     string
	vault            domain.Vault
	credentials      CredentialProvider
	observer         AgentObserver
	tokenSigner      *CallbackTokenSigner
	provisionTimeout time.Duration
	provisions       sync.Map // clusterID → *provisionState
	grpcServer       *grpc.Server
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithVault sets the [domain.Vault] used for storing cluster secrets.
func WithVault(v domain.Vault) AgentOption {
	return func(a *Agent) { a.vault = v }
}

// WithCredentialProvider sets the [CredentialProvider] used for
// resolving AWS credentials and pull secrets.
func WithCredentialProvider(p CredentialProvider) AgentOption {
	return func(a *Agent) { a.credentials = p }
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
//
// Defaults read from environment (overridable via options):
//   - OCP_ENGINE_BINARY: path to ocp-engine binary (default: "ocp-engine")
//   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN:
//     used by the default PassthroughCredentialProvider
//   - OCP_PULL_SECRET_FILE: path to pull secret JSON file
//
// The callback token signer is created automatically if not provided
// via WithTokenSigner.
func NewAgent(opts ...AgentOption) *Agent {
	engineBinary := os.Getenv("OCP_ENGINE_BINARY")
	if engineBinary == "" {
		engineBinary = "ocp-engine"
	}
	a := &Agent{
		engineBinary: engineBinary,
		observer:     NoOpAgentObserver{},
	}
	for _, o := range opts {
		o(a)
	}

	// Default credential provider: if static AWS keys are present in the
	// environment, use passthrough. Otherwise use SSO (STS
	// AssumeRoleWithWebIdentity from the caller's OIDC token).
	if a.credentials == nil {
		pullSecret := loadPullSecret()
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			a.credentials = &PassthroughCredentialProvider{
				AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
				AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
				AWSSessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
				PullSecret:         pullSecret,
			}
		} else {
			a.credentials = &SSOCredentialProvider{
				PullSecret: pullSecret,
			}
		}
	}

	// Default callback token signer if not set via option
	if a.tokenSigner == nil {
		signer, err := NewCallbackTokenSigner()
		if err != nil {
			slog.Error("failed to create callback token signer", "error", err)
		} else {
			a.tokenSigner = signer
		}
	}

	return a
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

	// 3. Read region and role_arn from cluster spec (validated in ParseClusterSpec)
	region := spec.Region
	roleARN := spec.RoleARN

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

	// Validate credential/mode coupling
	if err := validateCredentialModeCoupling(awsCreds, spec.EffectiveCCOSTSMode()); err != nil {
		probe.Error(err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: err.Error(),
		}, nil
	}

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
	configPath, workDir, err := prepareWorkDir(clusterID, spec, region, pullSecret, sshPublicKey, auth)
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
	req := provisionRequest{
		clusterID:     clusterID,
		configPath:    configPath,
		workDir:       workDir,
		awsCreds:      awsCreds,
		sshPrivateKey: sshPrivateKey,
		auth:          auth,
		roleARN:       roleARN,
		releaseImage:  spec.ReleaseImage,
		ccostsMode:    true, // always STS mode for OCP agent
	}
	go a.deliverAsync(ctx, req, signaler, state)

	// 10. Return accepted
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// provisionRequest bundles the parameters for an async provision.
type provisionRequest struct {
	clusterID     string
	configPath    string
	workDir       string
	awsCreds      *AWSCredentials
	sshPrivateKey []byte
	auth          domain.DeliveryAuth
	roleARN       string
	releaseImage  string
	ccostsMode    bool
}

// cleanupProvision removes the work directory and provisions entry.
func (a *Agent) cleanupProvision(clusterID, workDir string) {
	os.RemoveAll(workDir)
	a.provisions.Delete(clusterID)
}

// retainOrCleanup checks whether AWS infrastructure may have been created
// (metadata.json exists) and either retains the work directory for later
// cleanup by Remove(), or cleans up immediately.
func (a *Agent) retainOrCleanup(state *provisionState, clusterID, workDir string) {
	metadataPath := filepath.Join(workDir, "metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		a.cleanupProvision(clusterID, workDir)
	} else {
		state.mu.Lock()
		state.workDir = workDir
		state.mu.Unlock()
	}
}

// deliverAsync runs ocp-engine and waits for the callback or process exit.
func (a *Agent) deliverAsync(
	ctx context.Context,
	req provisionRequest,
	signaler *domain.DeliverySignaler,
	state *provisionState,
) {
	// Generate callback JWT
	provTimeout := a.effectiveProvisionTimeout()
	token, err := a.tokenSigner.Sign(req.clusterID, provTimeout)
	if err != nil {
		a.cleanupProvision(req.clusterID, req.workDir)
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("generate callback token: %v", err),
		})
		return
	}

	timeout := fmt.Sprintf("%ds", int(provTimeout.Seconds()))

	// Build and run ocp-engine subprocess
	cmd := exec.CommandContext(ctx, a.engineBinary,
		"provision",
		"--config", req.configPath,
		"--timeout", timeout,
		"--callback-url", a.callbackAddr,
		"--cluster-id", req.clusterID,
	)
	cmd.Env = append(os.Environ(), req.awsCreds.Env()...)
	cmd.Env = append(cmd.Env, "OCP_CALLBACK_TOKEN="+token)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	// Start the subprocess
	if err := cmd.Start(); err != nil {
		a.cleanupProvision(req.clusterID, req.workDir)
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
			a.retainOrCleanup(state, req.clusterID, req.workDir)
			signaler.Done(ctx, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: msg,
			})
			return
		}
	case <-ctx.Done():
		a.retainOrCleanup(state, req.clusterID, req.workDir)
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("context cancelled: %v", ctx.Err()),
		})
		return
	}

	// Process callback result (lock protects against concurrent
	// completion/failure writes from the callback server).
	state.mu.Lock()
	failure := state.failure
	completion := state.completion
	state.mu.Unlock()

	if failure != nil {
		if failure.GetRequiresDestroy() {
			state.mu.Lock()
			state.workDir = req.workDir
			state.mu.Unlock()
		} else {
			a.cleanupProvision(req.clusterID, req.workDir)
		}
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("ocp-engine failed in phase %s: %s", failure.GetPhase(), failure.GetFailureMessage()),
		})
		return
	}

	if completion == nil {
		a.cleanupProvision(req.clusterID, req.workDir)
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: "callback received but no completion data",
		})
		return
	}

	// Handle successful completion
	output, err := a.handleCompletion(ctx, req.clusterID, completion, req.sshPrivateKey, req.auth, req.roleARN, req.releaseImage, req.ccostsMode)
	if err != nil {
		state.mu.Lock()
		state.workDir = req.workDir
		state.mu.Unlock()
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("post-provision bootstrap failed: %v", err),
		})
		return
	}

	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		Message:            fmt.Sprintf("OCP cluster %s provisioned successfully", req.clusterID),
		ProvisionedTargets: []domain.ProvisionedTarget{output.Target()},
		ProducedSecrets:    output.Secrets(),
	}

	a.cleanupProvision(req.clusterID, req.workDir)
	signaler.Done(ctx, result)
}

// handleCompletion performs post-provision bootstrap and builds the
// [ClusterOutput] from the completion callback data.
func (a *Agent) handleCompletion(
	ctx context.Context,
	clusterName string,
	completion *ocpv1.CompletionRequest,
	sshPrivateKey []byte,
	auth domain.DeliveryAuth,
	roleARN string,
	releaseImage string,
	ccostsMode bool,
) (*ClusterOutput, error) {
	targetID := domain.TargetID("k8s-" + completion.GetInfraId())

	// Bootstrap the cluster: create namespace, SA, RBAC, generate token
	bootstrapResult, err := BootstrapCluster(
		ctx,
		completion.GetKubeconfig(),
		targetID,
		auth.Caller,
		"", // OIDC issuer URL — configured externally when needed
		0,  // use default token expiry
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
		RoleARN:       roleARN,
		ReleaseImage:  releaseImage,
		CCOSTSMode:    ccostsMode,
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
// cluster via ocp-engine destroy (which handles ccoctl IAM cleanup) and
// removes vault entries.
//
// Remove first checks for a retained failed provision — either in-memory
// (provisions map) or on disk (deterministic work dir). If found, it
// destroys using that work directory directly. Otherwise it falls through
// to the normal target-property-based destroy path.
func (a *Agent) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	_ domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	_ *domain.DeliverySignaler,
) error {
	// Try to get cluster name from manifests for work dir lookup
	spec, specErr := ParseClusterSpec(manifests)
	if specErr != nil {
		// Can't parse spec — fall through to target-property destroy
		return a.removeByTargetProperties(ctx, target, auth)
	}
	clusterID := spec.Name

	// 1. Check for retained failed provision (in-memory)
	workDir := ""
	if val, ok := a.provisions.Load(clusterID); ok {
		state := val.(*provisionState)
		state.mu.Lock()
		workDir = state.workDir
		state.mu.Unlock()
	}

	// 2. Fallback: check deterministic path on disk (survives server restart)
	if workDir == "" {
		candidate := provisionWorkDirPath(clusterID)
		if _, err := os.Stat(candidate); err == nil {
			workDir = candidate
		}
	}

	// 3. If we found a retained work dir, destroy using it
	if workDir != "" {
		awsCreds, err := a.credentials.ResolveAWS(ctx, AWSCredentialRequest{
			Region:  spec.Region,
			RoleARN: spec.RoleARN,
			Auth:    auth,
		})
		if err != nil {
			return fmt.Errorf("resolve AWS credentials for destroy: %w", err)
		}
		if err := a.runDestroy(ctx, workDir, awsCreds); err != nil {
			return err
		}
		os.RemoveAll(workDir)
		a.provisions.Delete(clusterID)
		return nil
	}

	// 4. Normal destroy path (successfully provisioned clusters)
	return a.removeByTargetProperties(ctx, target, auth)
}

// removeByTargetProperties destroys a successfully provisioned cluster
// using infra_id/region/role_arn stored in target.Properties. It creates a
// temporary work directory with reconstructed metadata.json.
func (a *Agent) removeByTargetProperties(ctx context.Context, target domain.TargetInfo, auth domain.DeliveryAuth) error {
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

	// 5. Write cluster.yaml so ocp-engine knows about STS mode
	ccostsMode := target.Properties["cco_sts_mode"] == "true"
	releaseImage := target.Properties["release_image"]
	clusterName := target.Name

	// 6. Run ocp-engine destroy
	if err := a.runDestroy(ctx, workDir, awsCreds); err != nil {
		return err
	}

	// 7. Clean up ccoctl resources (OIDC provider, IAM roles, S3 bucket)
	if ccostsMode && clusterName != "" {
		ccoctlPath, lookErr := exec.LookPath("ccoctl")
		if lookErr != nil && releaseImage != "" {
			// Extract ccoctl from release image
			slog.Info("ccoctl not on PATH, extracting from release image...", "release_image", releaseImage)
			pullSecretFile := filepath.Join(workDir, "pull-secret.json")
			if _, statErr := os.Stat(pullSecretFile); os.IsNotExist(statErr) {
				if ps, psErr := a.credentials.ResolvePullSecret(ctx, PullSecretRequest{Auth: auth}); psErr == nil {
					os.WriteFile(pullSecretFile, ps, 0600)
				}
			}
			extractCmd := exec.CommandContext(ctx, "oc", "adm", "release", "extract",
				"--command=ccoctl", "--to", workDir,
				"--registry-config", pullSecretFile, releaseImage)
			extractCmd.Env = append(os.Environ(), awsCreds.Env()...)
			if extractErr := extractCmd.Run(); extractErr != nil {
				slog.Warn("failed to extract ccoctl for cleanup", "error", extractErr)
			} else {
				ccoctlPath = filepath.Join(workDir, "ccoctl")
			}
		}

		if ccoctlPath != "" {
			deleteCmd := exec.CommandContext(ctx, ccoctlPath,
				"aws", "delete", "--name", clusterName, "--region", region)
			deleteCmd.Env = append(os.Environ(), awsCreds.Env()...)
			deleteCmd.Stdout = os.Stderr
			deleteCmd.Stderr = os.Stderr
			if deleteErr := deleteCmd.Run(); deleteErr != nil {
				slog.Warn("ccoctl aws delete failed (IAM resources may need manual cleanup)",
					"error", deleteErr, "cluster", clusterName, "region", region)
			}
		} else {
			slog.Warn("ccoctl binary not available, skipping IAM cleanup",
				"cluster", clusterName, "region", region)
		}
	}

	// 8. Clean up vault entries
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

// effectiveProvisionTimeout returns the configured provision timeout,
// falling back to defaultProvisionSTSDuration (2h).
func (a *Agent) effectiveProvisionTimeout() time.Duration {
	if a.provisionTimeout > 0 {
		return a.provisionTimeout
	}
	return defaultProvisionSTSDuration
}

// validateCredentialModeCoupling ensures STS temporary credentials are
// not used with mint mode. STS creds expire — if CCO uses them as root
// creds for minting, the cluster degrades after the session expires.
func validateCredentialModeCoupling(creds *AWSCredentials, ccostsMode bool) error {
	if !ccostsMode && creds.SessionToken != "" {
		return fmt.Errorf(
			"STS temporary credentials cannot be used with mint mode (cco_sts_mode: false); " +
				"use long-lived IAM keys or enable cco_sts_mode")
	}
	return nil
}

// runDestroy calls ocp-engine destroy against a work directory.
func (a *Agent) runDestroy(ctx context.Context, workDir string, awsCreds *AWSCredentials) error {
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
	return nil
}

// provisionWorkDirPath returns the deterministic work directory path for a cluster.
func provisionWorkDirPath(clusterID string) string {
	return filepath.Join(os.TempDir(), "ocp-provision-"+clusterID)
}

// prepareWorkDir creates a temp directory containing the pull secret and
// cluster.yaml config. Returns the config file path and work directory.
// The caller is responsible for cleaning up the work directory.
func prepareWorkDir(clusterID string, spec *ClusterSpec, region string, pullSecret, sshPublicKey []byte, auth domain.DeliveryAuth) (configPath, workDir string, err error) {
	workDir = provisionWorkDirPath(clusterID)
	if err = os.MkdirAll(workDir, 0755); err != nil {
		return "", "", fmt.Errorf("create work directory: %w", err)
	}
	defer func() {
		if err != nil {
			os.RemoveAll(workDir)
		}
	}()

	pullSecretFile := filepath.Join(workDir, "pull-secret.json")
	if err = os.WriteFile(pullSecretFile, pullSecret, 0600); err != nil {
		return "", "", fmt.Errorf("write pull secret: %w", err)
	}

	clusterYAML, err := BuildClusterYAML(spec, region, pullSecretFile, strings.TrimSpace(string(sshPublicKey)))
	if err != nil {
		return "", "", fmt.Errorf("build cluster.yaml: %w", err)
	}

	configPath = filepath.Join(workDir, "cluster.yaml")
	if err = os.WriteFile(configPath, clusterYAML, 0600); err != nil {
		return "", "", fmt.Errorf("write cluster.yaml: %w", err)
	}

	// Write OIDC authentication manifests for injection during the manifests phase.
	// Derive the OIDC provider config from the caller's auth context — the issuer
	// URL comes from the caller's federated identity, the audience from the
	// delivery auth.
	if auth.Caller != nil && auth.Caller.Issuer != "" {
		var audiences []string
		for _, aud := range auth.Audience {
			audiences = append(audiences, string(aud))
		}
		clientID := ""
		if len(audiences) > 0 {
			clientID = audiences[0]
		}
		oidcCfg := OIDCProviderConfig{
			IssuerURL:   string(auth.Caller.Issuer),
			Audiences:   audiences,
			CLIClientID: clientID,
		}
		extraDir := filepath.Join(workDir, "extra-manifests")
		if err = os.MkdirAll(extraDir, 0755); err != nil {
			return "", "", fmt.Errorf("create extra-manifests dir: %w", err)
		}
		manifests, genErr := GenerateOIDCManifests(oidcCfg)
		if genErr != nil {
			return "", "", fmt.Errorf("generate OIDC manifests: %w", genErr)
		}
		for filename, content := range manifests {
			if err = os.WriteFile(filepath.Join(extraDir, filename), content, 0600); err != nil {
				return "", "", fmt.Errorf("write OIDC manifest %s: %w", filename, err)
			}
		}
	}

	return configPath, workDir, nil
}

// loadPullSecret reads the pull secret from OCP_PULL_SECRET_FILE if set.
func loadPullSecret() []byte {
	ps := os.Getenv("OCP_PULL_SECRET_FILE")
	if ps == "" {
		return nil
	}
	data, err := os.ReadFile(ps)
	if err != nil {
		slog.Warn("failed to read OCP pull secret file", "path", ps, "error", err)
		return nil
	}
	return data
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
