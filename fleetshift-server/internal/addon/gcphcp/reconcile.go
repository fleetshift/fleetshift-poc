package gcphcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var (
	guestBootstrapMaxAttempts       = 10
	guestBootstrapRetryDelay        = 15 * time.Second
	bootstrapGuestCluster           = BootstrapGuestCluster
	failureSnapshotTimeout          = 10 * time.Second
	newBrokerAuth                   = func(cfg BrokerAuthConfig) brokerAuthExchanger { return NewBrokerAuth(cfg) }
	buildCreateHypershiftWorkspace  = PrepareCreateHypershiftWorkspace
	buildDestroyHypershiftWorkspace = PrepareDestroyHypershiftWorkspace
	reconcileNodepoolsFn            = reconcileNodepools
	pollClusterReadyFn              = PollClusterReady
	completeGuestRegistrationFn     = completeGuestRegistration
	pollDesiredNodepoolsHealthyFn   = PollDesiredNodepoolsHealthy
)

type brokerAuthExchanger interface {
	Exchange(ctx context.Context, callerToken string) (BrokerAuthResult, error)
}

type reconcileInfra interface {
	unconfirmedCreateRecoveryInfra
	deleteCleanupInfra
}

// Reconciler coordinates the full cluster create/update and delete flows.
// It sequences auth, infra, client, status, and bootstrap modules to manage
// the lifecycle of GCP HCP clusters.
type Reconciler struct {
	gateway GatewayConfig
	infra   reconcileInfra
}

// NewReconciler creates a new Reconciler with the given gateway config and infra runner.
func NewReconciler(gateway GatewayConfig, infra *InfraRunner) *Reconciler {
	return &Reconciler{
		gateway: gateway,
		infra:   infra,
	}
}

func completeGuestRegistration(
	ctx context.Context,
	clsClient *CLSClient,
	clusterID string,
	brokerToken string,
	guestTargetID domain.TargetID,
	progress *deliveryProgress,
) (string, BootstrapResult, error) {
	progress.Info(ctx, "Resolving guest API endpoint")

	var bootstrapResult BootstrapResult
	var bootstrapErr error
	var guestEndpoint string
	endpointAnnounced := false
	for attempt := 1; attempt <= guestBootstrapMaxAttempts; attempt++ {
		statusData, err := clsClient.GetClusterStatus(ctx, clusterID)
		if err != nil {
			return "", BootstrapResult{}, newPostProvisionRegistrationError(
				fmt.Errorf("get cluster status: %w", err),
			)
		}

		guestEndpoint, err = ResolveGuestAPIEndpoint(statusData)
		if err != nil {
			if attempt < guestBootstrapMaxAttempts {
				progress.Warn(ctx, fmt.Sprintf("Guest API endpoint not yet available, retrying in %v: %v", guestBootstrapRetryDelay, err))
				select {
				case <-ctx.Done():
					return "", BootstrapResult{}, newPostProvisionRegistrationError(ctx.Err())
				case <-time.After(guestBootstrapRetryDelay):
				}
				continue
			}

			progress.Warn(ctx, fmt.Sprintf("Hosted cluster is ready, but guest target registration did not complete: %v", err))
			return "", BootstrapResult{}, newPostProvisionRegistrationError(
				fmt.Errorf("resolve guest API endpoint after %d attempts: %w", guestBootstrapMaxAttempts, err),
			)
		}

		if !endpointAnnounced {
			progress.Info(ctx, fmt.Sprintf("Guest API endpoint: %s", guestEndpoint))
			endpointAnnounced = true
		}

		progress.Info(ctx, fmt.Sprintf("Bootstrapping guest cluster (attempt %d/%d)", attempt, guestBootstrapMaxAttempts))

		bootstrapResult, bootstrapErr = bootstrapGuestCluster(
			ctx,
			guestEndpoint,
			brokerToken,
			guestTargetID,
		)
		if bootstrapErr == nil {
			progress.Info(ctx, "Bootstrap successful")
			return guestEndpoint, bootstrapResult, nil
		}

		if attempt < guestBootstrapMaxAttempts {
			progress.Warn(ctx, fmt.Sprintf("Bootstrap failed, retrying in %v: %v", guestBootstrapRetryDelay, bootstrapErr))
			select {
			case <-ctx.Done():
				return "", BootstrapResult{}, newPostProvisionRegistrationError(ctx.Err())
			case <-time.After(guestBootstrapRetryDelay):
			}
		}
	}

	progress.Warn(ctx, fmt.Sprintf("Hosted cluster is ready, but guest target registration did not complete: %v", bootstrapErr))
	return "", BootstrapResult{}, newPostProvisionRegistrationError(
		fmt.Errorf("bootstrap guest cluster after %d attempts: %w", guestBootstrapMaxAttempts, bootstrapErr),
	)
}

func (r *Reconciler) exchangeAndCreateClient(
	ctx context.Context,
	target TargetConfig,
	callerToken string,
	progress *deliveryProgress,
) (*CLSClient, BrokerAuthResult, error) {
	progress.Info(ctx, "Exchanging caller token for broker credentials")

	brokerAuth := newBrokerAuth(BrokerAuthConfig{
		WorkforcePool:     target.WorkforcePool,
		WorkforceProvider: target.WorkforceProvider,
		GCPProject:        target.GCPProject,
		BrokerSAEmail:     target.BrokerSAEmail,
		GatewayAudience:   r.gateway.Audience,
	})

	authResult, err := brokerAuth.Exchange(ctx, callerToken)
	if err != nil {
		return nil, BrokerAuthResult{}, fmt.Errorf("broker auth exchange: %w", err)
	}

	progress.Info(ctx, "Creating CLS client")
	clsClient := NewCLSClient(r.gateway.URL, authResult.BrokerToken, authResult.BrokerEmail, nil)
	return clsClient, authResult, nil
}

// Reconcile performs the full cluster creation flow:
func (r *Reconciler) Reconcile(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	progress *deliveryProgress,
) (_ *ClusterOutput, retErr error) {
	clsClient, authResult, err := r.exchangeAndCreateClient(ctx, target, callerToken, progress)
	if err != nil {
		return nil, err
	}
	var clusterID string
	defer func() {
		if retErr == nil {
			return
		}

		snapshotCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failureSnapshotTimeout)
		defer cancel()

		snapshotClusterID := clusterID
		if snapshotClusterID == "" {
			resolvedID, err := clsClient.ResolveClusterID(snapshotCtx, spec.Name)
			switch {
			case err == nil:
				snapshotClusterID = resolvedID
			case !errors.Is(err, ErrClusterNotFound):
				progress.Info(snapshotCtx, fmt.Sprintf("Unable to resolve cluster for failure snapshot: %v", err))
			}
		}
		if snapshotClusterID == "" {
			return
		}
		if err := emitFailureStatusSnapshot(snapshotCtx, clsClient, snapshotClusterID, spec.Name, progress); err != nil {
			progress.Info(snapshotCtx, fmt.Sprintf("Unable to emit failure snapshot: %v", err))
		}
	}()

	progress.Info(ctx, "Reconciling cluster via CLS API")

	clusterID, err = clsClient.ResolveClusterID(ctx, spec.Name)
	switch {
	case err == nil:
		observedCluster, err := clsClient.GetCluster(ctx, clusterID)
		if err != nil {
			return nil, fmt.Errorf("get existing cluster %s: %w", clusterID, err)
		}

		updateSpec, err := BuildCLSClusterUpdateSpec(spec, observedCluster)
		if err != nil {
			return nil, fmt.Errorf("build CLS cluster update spec: %w", err)
		}

		if _, err := clsClient.UpdateCluster(ctx, clusterID, updateSpec); err != nil {
			return nil, fmt.Errorf("update cluster %s: %w", spec.Name, err)
		}

		progress.Info(ctx, fmt.Sprintf("Cluster updated with ID: %s", clusterID))

	case errors.Is(err, ErrClusterNotFound):
		progress.Info(ctx, "Generating cluster keypair")

		// Generate cluster keypair
		keypair, err := GenerateClusterKeypair()
		if err != nil {
			return nil, fmt.Errorf("generate cluster keypair: %w", err)
		}

		if err := func() (retErr error) {
			progress.Info(ctx, "Preparing hypershift workspace")

			workspace, err := buildCreateHypershiftWorkspace(callerToken, target, keypair.JWKSJSON)
			if err != nil {
				return fmt.Errorf("prepare hypershift workspace: %w", err)
			}
			defer workspace.CleanupOnReturn(&retErr)

			var createdIAM bool
			var createdInfra bool
			cleanupCreateFailure := func(createErr error) error {
				if !createdIAM && !createdInfra {
					return createErr
				}
				progress.Info(ctx, "Create flow failed; cleaning up partial IAM/infra resources")
				cleanupErr := cleanupCreateResources(ctx, r.infra, spec, target, workspace.Env, createdInfra, createdIAM)
				if cleanupErr != nil {
					return errors.Join(createErr, cleanupErr)
				}
				return createErr
			}

			progress.Info(ctx, "Creating IAM resources")

			iamConfig, err := ensureIAMWithRecovery(
				ctx,
				r.infra,
				spec,
				target,
				workspace.JWKSPath,
				workspace.Env,
				progress,
			)
			if err != nil {
				return fmt.Errorf("create IAM: %w", err)
			}
			createdIAM = true

			progress.Info(ctx, "Creating infrastructure")

			infraConfig, err := ensureInfraWithRecovery(ctx, r.infra, spec, target, workspace.Env, progress)
			if err != nil {
				return fmt.Errorf("create infra: %w", err)
			}
			createdInfra = true

			progress.Info(ctx, "Building CLS cluster spec")

			clsClusterSpec, err := BuildCLSClusterSpec(spec, target, infraConfig, iamConfig, keypair.PrivateKeyPEMBase64)
			if err != nil {
				return cleanupCreateFailure(fmt.Errorf("build CLS cluster spec: %w", err))
			}

			progress.Info(ctx, "Creating cluster via CLS API")

			clusterData, err := clsClient.CreateCluster(ctx, clsClusterSpec)
			if err != nil {
				if isCLSHTTPStatus(err, http.StatusBadRequest) ||
					isCLSHTTPStatus(err, http.StatusUnauthorized) ||
					isCLSHTTPStatus(err, http.StatusForbidden) {
					return cleanupCreateFailure(fmt.Errorf("create cluster: %w", err))
				}
				clusterID, err = recoverFromUnconfirmedCreate(
					ctx,
					clsClient,
					r.infra,
					spec,
					target,
					workspace.JWKSPath,
					workspace.Env,
					createdInfra,
					createdIAM,
					fmt.Errorf("create cluster: %w", err),
					progress,
				)
				return err
			}

			var ok bool
			clusterID, ok = clusterData["id"].(string)
			if !ok || clusterID == "" {
				clusterID, err = recoverFromUnconfirmedCreate(
					ctx,
					clsClient,
					r.infra,
					spec,
					target,
					workspace.JWKSPath,
					workspace.Env,
					createdInfra,
					createdIAM,
					fmt.Errorf("cluster creation response missing id field"),
					progress,
				)
				return err
			}

			progress.Info(ctx, fmt.Sprintf("Cluster created with ID: %s", clusterID))

			return nil
		}(); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("resolve cluster ID: %w", err)
	}

	if err := reconcileNodepoolsFn(ctx, clsClient, clusterID, spec.Name, spec.Nodepools, progress); err != nil {
		return nil, err
	}

	progress.Info(ctx, "Polling for cluster ready state")

	// Poll until cluster is ready
	if err := pollClusterReadyFn(ctx, clsClient, clusterID, progress); err != nil {
		return nil, fmt.Errorf("poll cluster ready: %w", err)
	}

	emitClusterReadyTransition(ctx, progress)

	guestTargetID := GuestTargetID(spec.Name)
	guestEndpoint, bootstrapResult, err := completeGuestRegistrationFn(
		ctx,
		clsClient,
		clusterID,
		authResult.BrokerToken,
		guestTargetID,
		progress,
	)
	if err != nil {
		return nil, err
	}

	progress.Info(ctx, "Waiting for desired nodepools to become healthy")

	if err := pollDesiredNodepoolsHealthyFn(ctx, clsClient, clusterID, spec.Name, spec.Nodepools, progress); err != nil {
		return nil, fmt.Errorf("wait for desired nodepools healthy: %w", err)
	}

	progress.Info(ctx, "Desired nodepools are healthy; building cluster output")

	// Build ClusterOutput. The agent attaches trust bundles from its
	// current addon input state before registering the emitted target.
	output := &ClusterOutput{
		TargetID:   guestTargetID,
		Name:       spec.Name,
		APIServer:  guestEndpoint,
		CACert:     bootstrapResult.CACert,
		SATokenRef: bootstrapResult.SATokenRef,
		SAToken:    bootstrapResult.SAToken,
	}

	progress.Info(ctx, "Cluster provisioning complete")

	return output, nil
}

// Delete performs the full cluster deletion flow:
// 1. Exchange caller token for broker credentials
// 2. Create CLS client
// 3. Resolve cluster ID by name
// 4. Delete cluster via CLS API
// 5. Poll until deleted
// 6. Destroy infrastructure
// 7. Destroy IAM (best-effort)
func (r *Reconciler) Delete(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	progress *deliveryProgress,
) error {
	clsClient, authResult, err := r.exchangeAndCreateClient(ctx, target, callerToken, progress)
	if err != nil {
		return err
	}

	progress.Info(ctx, "Resolving cluster ID")

	// Resolve cluster ID by name
	clusterID, deletedCluster, err := deleteClusterIfPresent(ctx, clsClient, spec.Name, progress)
	if err != nil {
		return err
	}

	if deletedCluster {
		progress.Info(ctx, "Polling for cluster deletion")

		// Poll until deleted
		if err := PollClusterDeleted(ctx, clsClient, clusterID, progress); err != nil {
			return fmt.Errorf("poll cluster deleted: %w", err)
		}
	}

	progress.Info(ctx, "Preparing to destroy infrastructure")

	if err := waitForDeleteCleanupPrereqs(
		ctx,
		r.infra,
		clusterID,
		target,
		authResult.WorkforceToken,
		progress,
	); err != nil {
		return err
	}

	if err := func() (retErr error) {
		workspace, err := buildDestroyHypershiftWorkspace(callerToken, target)
		if err != nil {
			return fmt.Errorf("prepare hypershift workspace: %w", err)
		}
		defer workspace.CleanupOnReturn(&retErr)

		return cleanupDeleteResources(
			ctx,
			r.infra,
			spec,
			target,
			workspace.Env,
			progress,
		)
	}(); err != nil {
		return err
	}

	progress.Info(ctx, "Cluster deletion complete")

	return nil
}

// BuildCLSClusterSpec builds the CLS API cluster creation request body.
// It converts the addon's ClusterSpec and infrastructure outputs into the
// format expected by the CLS /api/v1/clusters endpoint.
func BuildCLSClusterSpec(
	spec ClusterSpec,
	target TargetConfig,
	infraConfig, iamConfig map[string]any,
	signingKeyBase64 string,
) (map[string]any, error) {
	// Extract infra ID
	infraID, ok := infraConfig["infraId"].(string)
	if !ok {
		return nil, fmt.Errorf("infraId not found in infra config")
	}

	// Extract network and subnet
	networkName, ok := infraConfig["networkName"].(string)
	if !ok {
		return nil, fmt.Errorf("networkName not found in infra config")
	}

	subnetName, ok := infraConfig["subnetName"].(string)
	if !ok {
		return nil, fmt.Errorf("subnetName not found in infra config")
	}

	// Convert IAM config to WIF spec
	wifSpec, err := IAMConfigToWIFSpec(iamConfig)
	if err != nil {
		return nil, fmt.Errorf("convert IAM config to WIF spec: %w", err)
	}

	clusterSpec := map[string]any{
		"infraID":                  infraID,
		"issuerURL":                fmt.Sprintf("https://hypershift-%s-oidc", infraID),
		"serviceAccountSigningKey": signingKeyBase64,
		"platform": map[string]any{
			"type": "GCP",
			"gcp": map[string]any{
				"projectID":        target.GCPProject,
				"region":           target.Region,
				"network":          networkName,
				"subnet":           subnetName,
				"endpointAccess":   spec.EndpointAccess,
				"workloadIdentity": wifSpec,
			},
		},
	}
	clusterSpec["releaseVersion"] = spec.ReleaseVersion
	clusterSpec["channelGroup"] = spec.ChannelGroup

	return map[string]any{
		"name":              spec.Name,
		"target_project_id": target.GCPProject,
		"spec":              clusterSpec,
	}, nil
}

// BuildCLSNodepoolSpec builds the CLS API nodepool creation request body.
// It converts a NodepoolSpec into the format expected by the CLS /api/v1/nodepools endpoint.
func BuildCLSNodepoolSpec(np NodepoolSpec, clusterName, clusterID string) map[string]any {
	return map[string]any{
		"name":       NodepoolName(clusterName, np.ID),
		"cluster_id": clusterID,
		"spec": map[string]any{
			"replicas": np.Replicas,
			"platform": map[string]any{
				"type": "GCP",
				"gcp": map[string]any{
					"instanceType": np.InstanceType,
					"rootVolume": map[string]any{
						"size": np.RootVolumeSize,
						"type": np.RootVolumeType,
					},
				},
			},
			"management": map[string]any{
				"autoRepair":  *np.AutoRepair,
				"upgradeType": np.UpgradeType,
			},
		},
	}
}
