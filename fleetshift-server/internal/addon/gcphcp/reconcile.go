package gcphcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var (
	guestBootstrapMaxAttempts = 10
	guestBootstrapRetryDelay  = 15 * time.Second
	bootstrapGuestCluster     = BootstrapGuestCluster
	failureSnapshotTimeout    = 10 * time.Second
	newBrokerAuth             = func(cfg BrokerAuthConfig) brokerAuthExchanger { return NewBrokerAuth(cfg) }
	buildHypershiftEnv        = PrepareHypershiftEnv
)

type brokerAuthExchanger interface {
	Exchange(ctx context.Context, callerToken string) (BrokerAuthResult, error)
}

type reconcileInfra interface {
	ambiguousCreateRecoveryInfra
	deleteCleanupInfra
}

// Reconciler coordinates the full cluster create/update and delete flows.
// It sequences auth, infra, client, status, and bootstrap modules to manage
// the lifecycle of GCP HCP clusters.
type Reconciler struct {
	gateway      GatewayConfig
	infra        reconcileInfra
	trustMu      sync.RWMutex
	trustBundles []domain.TrustBundleEntry
}

// NewReconciler creates a new Reconciler with the given gateway config and infra runner.
func NewReconciler(gateway GatewayConfig, infra *InfraRunner) *Reconciler {
	return &Reconciler{
		gateway: gateway,
		infra:   infra,
	}
}

// StoreTrustBundle appends a trust bundle entry to the reconciler's in-memory store.
// Thread-safe for concurrent access.
func (r *Reconciler) StoreTrustBundle(entry domain.TrustBundleEntry) {
	r.trustMu.Lock()
	defer r.trustMu.Unlock()
	r.trustBundles = append(r.trustBundles, entry)
}

// TrustBundles returns a copy of the current trust bundles.
// Thread-safe for concurrent access.
func (r *Reconciler) TrustBundles() []domain.TrustBundleEntry {
	r.trustMu.RLock()
	defer r.trustMu.RUnlock()
	result := make([]domain.TrustBundleEntry, len(r.trustBundles))
	copy(result, r.trustBundles)
	return result
}

func completeGuestRegistration(
	ctx context.Context,
	clsClient *CLSClient,
	clusterID string,
	brokerToken string,
	guestTargetID domain.TargetID,
	signaler *domain.DeliverySignaler,
) (string, BootstrapResult, error) {
	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Resolving guest API endpoint",
	})

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
				signaler.Emit(ctx, domain.DeliveryEvent{
					Timestamp: time.Now(),
					Kind:      domain.DeliveryEventWarning,
					Message:   fmt.Sprintf("Guest API endpoint not yet available, retrying in %v: %v", guestBootstrapRetryDelay, err),
				})
				select {
				case <-ctx.Done():
					return "", BootstrapResult{}, newPostProvisionRegistrationError(ctx.Err())
				case <-time.After(guestBootstrapRetryDelay):
				}
				continue
			}

			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventWarning,
				Message:   fmt.Sprintf("Hosted cluster is ready, but guest target registration did not complete: %v", err),
			})

			return "", BootstrapResult{}, newPostProvisionRegistrationError(
				fmt.Errorf("resolve guest API endpoint after %d attempts: %w", guestBootstrapMaxAttempts, err),
			)
		}

		if !endpointAnnounced {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   fmt.Sprintf("Guest API endpoint: %s", guestEndpoint),
			})
			endpointAnnounced = true
		}

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   fmt.Sprintf("Bootstrapping guest cluster (attempt %d/%d)", attempt, guestBootstrapMaxAttempts),
		})

		bootstrapResult, bootstrapErr = bootstrapGuestCluster(
			ctx,
			guestEndpoint,
			brokerToken,
			guestTargetID,
		)
		if bootstrapErr == nil {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   "Bootstrap successful",
			})
			return guestEndpoint, bootstrapResult, nil
		}

		if attempt < guestBootstrapMaxAttempts {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventWarning,
				Message:   fmt.Sprintf("Bootstrap failed, retrying in %v: %v", guestBootstrapRetryDelay, bootstrapErr),
			})
			select {
			case <-ctx.Done():
				return "", BootstrapResult{}, newPostProvisionRegistrationError(ctx.Err())
			case <-time.After(guestBootstrapRetryDelay):
			}
		}
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventWarning,
		Message:   fmt.Sprintf("Hosted cluster is ready, but guest target registration did not complete: %v", bootstrapErr),
	})

	return "", BootstrapResult{}, newPostProvisionRegistrationError(
		fmt.Errorf("bootstrap guest cluster after %d attempts: %w", guestBootstrapMaxAttempts, bootstrapErr),
	)
}

// Reconcile performs the full cluster creation flow:
func (r *Reconciler) Reconcile(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	signaler *domain.DeliverySignaler,
) (_ *ClusterOutput, retErr error) {
	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Exchanging caller token for broker credentials",
	})

	// Exchange caller token for broker credentials
	brokerAuth := newBrokerAuth(BrokerAuthConfig{
		WorkforcePool:     target.WorkforcePool,
		WorkforceProvider: target.WorkforceProvider,
		GCPProject:        target.GCPProject,
		BrokerSAEmail:     target.BrokerSAEmail,
		GatewayAudience:   r.gateway.Audience,
	})

	authResult, err := brokerAuth.Exchange(ctx, callerToken)
	if err != nil {
		return nil, fmt.Errorf("broker auth exchange: %w", err)
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Creating CLS client",
	})

	// Create CLS client
	clsClient := NewCLSClient(r.gateway.URL, authResult.BrokerToken, authResult.BrokerEmail, nil)
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
				emitProgress(signaler, snapshotCtx, fmt.Sprintf("Unable to resolve cluster for failure snapshot: %v", err))
			}
		}
		if snapshotClusterID == "" {
			return
		}
		if err := emitFailureStatusSnapshot(snapshotCtx, clsClient, snapshotClusterID, spec.Name, signaler); err != nil {
			emitProgress(signaler, snapshotCtx, fmt.Sprintf("Unable to emit failure snapshot: %v", err))
		}
	}()

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Reconciling cluster via CLS API",
	})

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

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   fmt.Sprintf("Cluster updated with ID: %s", clusterID),
		})

	case errors.Is(err, ErrClusterNotFound):
		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Generating cluster keypair",
		})

		// Create temp directory for workspace
		tempDir, err := os.MkdirTemp("", "gcphcp-*")
		if err != nil {
			return nil, fmt.Errorf("create temp dir: %w", err)
		}
		defer os.RemoveAll(tempDir)

		// Generate cluster keypair
		keypair, err := GenerateClusterKeypair()
		if err != nil {
			return nil, fmt.Errorf("generate cluster keypair: %w", err)
		}

		// Write JWKS to temp file
		jwksPath := filepath.Join(tempDir, "jwks.json")
		if err := os.WriteFile(jwksPath, keypair.JWKSJSON, 0600); err != nil {
			return nil, fmt.Errorf("write JWKS file: %w", err)
		}

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Preparing hypershift environment",
		})

		// Prepare hypershift environment
		hypershiftEnv, err := buildHypershiftEnv(callerToken, target, tempDir)
		if err != nil {
			return nil, fmt.Errorf("prepare hypershift env: %w", err)
		}

		var createdIAM bool
		var createdInfra bool
		cleanupCreateFailure := func(createErr error) error {
			if !createdIAM && !createdInfra {
				return createErr
			}
			emitProgress(signaler, ctx, "Create flow failed; cleaning up partial IAM/infra resources")
			cleanupErr := cleanupCreateResources(ctx, r.infra, spec, target, hypershiftEnv, createdInfra, createdIAM)
			if cleanupErr != nil {
				return errors.Join(createErr, cleanupErr)
			}
			return createErr
		}

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Creating IAM resources",
		})

		// Create IAM
		iamConfig, err := ensureIAMWithRecovery(ctx, r.infra, spec, target, jwksPath, hypershiftEnv, signaler)
		if err != nil {
			return nil, fmt.Errorf("create IAM: %w", err)
		}
		createdIAM = true

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Creating infrastructure",
		})

		// Create infrastructure
		infraConfig, err := ensureInfraWithRecovery(ctx, r.infra, spec, target, hypershiftEnv, signaler)
		if err != nil {
			return nil, fmt.Errorf("create infra: %w", err)
		}
		createdInfra = true

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Building CLS cluster spec",
		})

		// Build CLS cluster spec
		clsClusterSpec, err := BuildCLSClusterSpec(spec, target, infraConfig, iamConfig, keypair.PrivateKeyPEMBase64)
		if err != nil {
			return nil, cleanupCreateFailure(fmt.Errorf("build CLS cluster spec: %w", err))
		}

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Creating cluster via CLS API",
		})

		// Create cluster
		clusterData, err := clsClient.CreateCluster(ctx, clsClusterSpec)
		if err != nil {
			clusterID, err = recoverFromAmbiguousCreateFailure(
				ctx,
				clsClient,
				r.infra,
				spec,
				target,
				jwksPath,
				hypershiftEnv,
				createdInfra,
				createdIAM,
				fmt.Errorf("create cluster: %w", err),
				signaler,
			)
			if err != nil {
				return nil, err
			}
			break
		}

		var ok bool
		clusterID, ok = clusterData["id"].(string)
		if !ok || clusterID == "" {
			clusterID, err = recoverFromAmbiguousCreateFailure(
				ctx,
				clsClient,
				r.infra,
				spec,
				target,
				jwksPath,
				hypershiftEnv,
				createdInfra,
				createdIAM,
				fmt.Errorf("cluster creation response missing id field"),
				signaler,
			)
			if err != nil {
				return nil, err
			}
			break
		}

		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   fmt.Sprintf("Cluster created with ID: %s", clusterID),
		})

	default:
		return nil, fmt.Errorf("resolve cluster ID: %w", err)
	}

	if err := reconcileNodepools(ctx, clsClient, clusterID, spec.Nodepools, signaler); err != nil {
		return nil, err
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Polling for cluster ready state",
	})

	// Poll until cluster is ready
	if err := PollClusterReady(ctx, clsClient, clusterID, signaler); err != nil {
		return nil, fmt.Errorf("poll cluster ready: %w", err)
	}

	emitClusterReadyTransition(ctx, signaler)

	guestTargetID := GuestTargetID(spec.Name)
	guestEndpoint, bootstrapResult, err := completeGuestRegistration(
		ctx,
		clsClient,
		clusterID,
		authResult.BrokerToken,
		guestTargetID,
		signaler,
	)
	if err != nil {
		return nil, err
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Waiting for desired nodepools to become healthy",
	})

	if err := PollDesiredNodepoolsHealthy(ctx, clsClient, clusterID, spec.Nodepools, signaler); err != nil {
		return nil, fmt.Errorf("wait for desired nodepools healthy: %w", err)
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Desired nodepools are healthy; building cluster output",
	})

	// Build ClusterOutput with trust bundles
	output := &ClusterOutput{
		TargetID:     guestTargetID,
		Name:         spec.Name,
		APIServer:    guestEndpoint,
		CACert:       bootstrapResult.CACert,
		SATokenRef:   bootstrapResult.SATokenRef,
		SAToken:      bootstrapResult.SAToken,
		TrustBundles: r.TrustBundles(),
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Cluster provisioning complete",
	})

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
	signaler *domain.DeliverySignaler,
) error {
	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Exchanging caller token for broker credentials",
	})

	// Exchange caller token for broker credentials
	brokerAuth := newBrokerAuth(BrokerAuthConfig{
		WorkforcePool:     target.WorkforcePool,
		WorkforceProvider: target.WorkforceProvider,
		GCPProject:        target.GCPProject,
		BrokerSAEmail:     target.BrokerSAEmail,
		GatewayAudience:   r.gateway.Audience,
	})

	authResult, err := brokerAuth.Exchange(ctx, callerToken)
	if err != nil {
		return fmt.Errorf("broker auth exchange: %w", err)
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Creating CLS client",
	})

	// Create CLS client
	clsClient := NewCLSClient(r.gateway.URL, authResult.BrokerToken, authResult.BrokerEmail, nil)

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Resolving cluster ID",
	})

	// Resolve cluster ID by name
	clusterID, deletedCluster, err := deleteClusterIfPresent(ctx, clsClient, spec.Name, signaler)
	if err != nil {
		return err
	}

	if deletedCluster {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   "Polling for cluster deletion",
		})

		// Poll until deleted
		if err := PollClusterDeleted(ctx, clsClient, clusterID, signaler); err != nil {
			return fmt.Errorf("poll cluster deleted: %w", err)
		}
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Preparing to destroy infrastructure",
	})

	// Create temp directory and prepare hypershift env for destroy operations
	tempDir, err := os.MkdirTemp("", "gcphcp-destroy-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	hypershiftEnv, err := buildHypershiftEnv(callerToken, target, tempDir)
	if err != nil {
		return fmt.Errorf("prepare hypershift env: %w", err)
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Destroying infrastructure",
	})

	if err := cleanupDeleteResources(
		ctx,
		r.infra,
		clusterID,
		spec,
		target,
		authResult.WorkforceToken,
		hypershiftEnv,
		signaler,
	); err != nil {
		return err
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Cluster deletion complete",
	})

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
func BuildCLSNodepoolSpec(np NodepoolSpec, clusterID string) map[string]any {
	return map[string]any{
		"name":       np.Name,
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
