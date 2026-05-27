package gcphcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type nodepoolReconcileClient interface {
	ListNodepools(ctx context.Context, clusterID string) ([]map[string]any, error)
	CreateNodepool(ctx context.Context, spec map[string]any) (map[string]any, error)
	UpdateNodepool(ctx context.Context, nodepoolID string, spec map[string]any) (map[string]any, error)
	DeleteNodepool(ctx context.Context, nodepoolID string) error
}

type createCleanupInfra interface {
	DestroyInfra(ctx context.Context, infraID, projectID, region string, env []string) error
	DestroyIAM(ctx context.Context, infraID, projectID string, env []string) error
}

type deleteCleanupInfra interface {
	createCleanupInfra
	WaitForPSCCleanup(
		ctx context.Context,
		clusterID, projectID, region, workforceToken string,
		progress *deliveryProgress,
	) error
}

type clusterResolveClient interface {
	ResolveClusterID(ctx context.Context, clusterName string) (string, error)
}

type clusterDeleteClient interface {
	clusterResolveClient
	DeleteCluster(ctx context.Context, clusterID string) error
}

type prereqRecoveryInfra interface {
	CreateIAM(ctx context.Context, infraID, projectID, jwksFile string, env []string) (map[string]any, error)
	CreateInfra(ctx context.Context, infraID, projectID, region string, env []string) (map[string]any, error)
}

type unconfirmedCreateRecoveryClient interface {
	clusterResolveClient
	GetCluster(ctx context.Context, clusterID string) (map[string]any, error)
	UpdateCluster(ctx context.Context, clusterID string, spec map[string]any) (map[string]any, error)
}

type unconfirmedCreateRecoveryInfra interface {
	prereqRecoveryInfra
	createCleanupInfra
}

var (
	unconfirmedCreateProbeInterval = 2 * time.Second
	unconfirmedCreateProbeTimeout  = 30 * time.Second
	unconfirmedPrereqRetryInterval = 2 * time.Second
	unconfirmedPrereqMaxAttempts   = 3
)

// BuildCLSClusterUpdateSpec preserves observed bootstrap/infra fields while
// overlaying the desired mutable cluster fields from the addon spec.
func BuildCLSClusterUpdateSpec(spec ClusterSpec, observed map[string]any) (map[string]any, error) {
	targetProjectID, ok := observed["target_project_id"].(string)
	if !ok || targetProjectID == "" {
		return nil, fmt.Errorf("observed cluster missing target_project_id")
	}

	observedSpec, ok := observed["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("observed cluster missing spec object")
	}

	clonedSpec, err := cloneAnyMap(observedSpec)
	if err != nil {
		return nil, fmt.Errorf("clone observed cluster spec: %w", err)
	}

	platformMap, ok := clonedSpec["platform"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("observed cluster spec missing platform object")
	}
	gcpMap, ok := platformMap["gcp"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("observed cluster platform missing gcp object")
	}

	gcpMap["endpointAccess"] = spec.EndpointAccess
	clonedSpec["releaseVersion"] = spec.ReleaseVersion
	clonedSpec["channelGroup"] = spec.ChannelGroup

	return map[string]any{
		"name":              spec.Name,
		"target_project_id": targetProjectID,
		"spec":              clonedSpec,
	}, nil
}

func reconcileNodepools(
	ctx context.Context,
	client nodepoolReconcileClient,
	clusterID string,
	clusterName string,
	desired []NodepoolSpec,
	progress *deliveryProgress,
) error {
	observed, err := client.ListNodepools(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("list nodepools: %w", err)
	}

	observedByName := make(map[string]string, len(observed))
	for _, nodepool := range observed {
		name, _ := nodepool["name"].(string)
		if name == "" {
			return fmt.Errorf("observed nodepool missing name")
		}
		id, _ := nodepool["id"].(string)
		if id == "" {
			return fmt.Errorf("observed nodepool %q missing id", name)
		}
		if _, exists := observedByName[name]; exists {
			return fmt.Errorf("duplicate observed nodepool name %q", name)
		}
		observedByName[name] = id
	}

	seenIDs := make(map[string]struct{}, len(desired))
	for _, np := range desired {
		if _, exists := seenIDs[np.ID]; exists {
			return fmt.Errorf("duplicate desired nodepool id %q", np.ID)
		}
		seenIDs[np.ID] = struct{}{}
	}

	derivedNames := make(map[string]struct{}, len(desired))
	for _, np := range desired {
		derivedName := NodepoolName(clusterName, np.ID)
		derivedNames[derivedName] = struct{}{}
		if _, seen := observedByName[derivedName]; !seen {
			progress.Info(ctx, fmt.Sprintf("Creating nodepool: %s", derivedName))
			if _, err := client.CreateNodepool(ctx, BuildCLSNodepoolSpec(np, clusterName, clusterID)); err != nil {
				return fmt.Errorf("create nodepool %s: %w", derivedName, err)
			}
			continue
		}
	}

	for _, np := range desired {
		derivedName := NodepoolName(clusterName, np.ID)
		if nodepoolID, ok := observedByName[derivedName]; ok {
			progress.Info(ctx, fmt.Sprintf("Updating nodepool: %s", derivedName))
			if _, err := client.UpdateNodepool(ctx, nodepoolID, BuildCLSNodepoolSpec(np, clusterName, clusterID)); err != nil {
				return fmt.Errorf("update nodepool %s: %w", derivedName, err)
			}
		}
	}

	var removed []string
	for name := range observedByName {
		if _, ok := derivedNames[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)

	for _, name := range removed {
		progress.Info(ctx, fmt.Sprintf("Deleting removed nodepool: %s", name))
		if err := client.DeleteNodepool(ctx, observedByName[name]); err != nil {
			return fmt.Errorf("delete nodepool %s: %w", name, err)
		}
	}

	return nil
}

func cleanupCreateResources(
	ctx context.Context,
	infra createCleanupInfra,
	spec ClusterSpec,
	target TargetConfig,
	hypershiftEnv []string,
	createdInfra bool,
	createdIAM bool,
) error {
	var cleanupErr error

	if createdInfra {
		if err := infra.DestroyInfra(ctx, spec.Name, target.GCPProject, target.Region, hypershiftEnv); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("destroy infra: %w", err))
		}
	}
	if createdIAM {
		if err := infra.DestroyIAM(ctx, spec.Name, target.GCPProject, hypershiftEnv); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("destroy IAM: %w", err))
		}
	}

	return cleanupErr
}

func waitForDeleteCleanupPrereqs(
	ctx context.Context,
	infra deleteCleanupInfra,
	clusterID string,
	target TargetConfig,
	workforceToken string,
	progress *deliveryProgress,
) error {
	if clusterID != "" {
		if err := infra.WaitForPSCCleanup(
			ctx,
			clusterID,
			target.GCPProject,
			target.Region,
			workforceToken,
			progress,
		); err != nil {
			return fmt.Errorf("wait for PSC cleanup: %w", err)
		}
	}
	return nil
}

func cleanupDeleteResources(
	ctx context.Context,
	infra createCleanupInfra,
	spec ClusterSpec,
	target TargetConfig,
	hypershiftEnv []string,
	progress *deliveryProgress,
) error {
	progress.Info(ctx, "Destroying infrastructure")
	if err := infra.DestroyInfra(ctx, spec.Name, target.GCPProject, target.Region, hypershiftEnv); err != nil {
		return fmt.Errorf("destroy infra: %w", err)
	}
	progress.Info(ctx, "Destroying IAM resources")
	if err := infra.DestroyIAM(ctx, spec.Name, target.GCPProject, hypershiftEnv); err != nil {
		return fmt.Errorf(
			"cluster deletion succeeded and infrastructure cleanup completed, but IAM cleanup failed: %w",
			err,
		)
	}
	return nil
}

func ensureIAMWithRecovery(
	ctx context.Context,
	infra prereqRecoveryInfra,
	spec ClusterSpec,
	target TargetConfig,
	jwksPath string,
	hypershiftEnv []string,
	progress *deliveryProgress,
) (map[string]any, error) {
	return retryUnconfirmedPrereqCreate(ctx, progress, "IAM resource creation", func() (map[string]any, error) {
		return infra.CreateIAM(ctx, spec.Name, target.GCPProject, jwksPath, hypershiftEnv)
	})
}

func ensureInfraWithRecovery(
	ctx context.Context,
	infra prereqRecoveryInfra,
	spec ClusterSpec,
	target TargetConfig,
	hypershiftEnv []string,
	progress *deliveryProgress,
) (map[string]any, error) {
	return retryUnconfirmedPrereqCreate(ctx, progress, "infrastructure creation", func() (map[string]any, error) {
		return infra.CreateInfra(ctx, spec.Name, target.GCPProject, target.Region, hypershiftEnv)
	})
}

func retryUnconfirmedPrereqCreate(
	ctx context.Context,
	progress *deliveryProgress,
	resourceName string,
	create func() (map[string]any, error),
) (map[string]any, error) {
	totalAttempts := max(unconfirmedPrereqMaxAttempts, 1)

	attemptErrs := make([]error, 0, totalAttempts)
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(append(attemptErrs, err)...)
		}

		result, err := create()
		if err == nil {
			if attempt > 1 {
				progress.Info(ctx, fmt.Sprintf("Recovered %s on attempt %d", resourceName, attempt))
			}
			return result, nil
		}

		attemptErrs = append(attemptErrs, fmt.Errorf("attempt %d: %w", attempt, err))
		if attempt == totalAttempts {
			attemptErrs = append(attemptErrs, fmt.Errorf("%s remained unconfirmed after %d attempts", resourceName, totalAttempts))
			return nil, errors.Join(attemptErrs...)
		}

		progress.Info(ctx, fmt.Sprintf("%s may have partially succeeded; retrying (%d/%d)", resourceName, attempt+1, totalAttempts))
		if unconfirmedPrereqRetryInterval <= 0 {
			continue
		}

		timer := time.NewTimer(unconfirmedPrereqRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, errors.Join(append(attemptErrs, ctx.Err())...)
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("unreachable unconfirmed prereq recovery state")
}

func recoverFromUnconfirmedCreate(
	ctx context.Context,
	client unconfirmedCreateRecoveryClient,
	infra unconfirmedCreateRecoveryInfra,
	spec ClusterSpec,
	target TargetConfig,
	jwksPath string,
	hypershiftEnv []string,
	createdInfra bool,
	createdIAM bool,
	createErr error,
	progress *deliveryProgress,
) (string, error) {
	clusterID, adopted, probeErr := resolveClusterAfterUnconfirmedCreate(ctx, client, spec.Name, progress)
	switch {
	case probeErr != nil:
		return "", errors.Join(createErr, probeErr)
	case adopted:
		if err := repairAdoptedClusterAfterUnconfirmedCreate(
			ctx,
			client,
			infra,
			clusterID,
			spec,
			target,
			jwksPath,
			hypershiftEnv,
			progress,
		); err != nil {
			return "", errors.Join(createErr, err)
		}
		return clusterID, nil
	default:
		progress.Info(ctx, "Unconfirmed create did not surface a cluster; cleaning up partial IAM/infra resources")
		cleanupErr := cleanupCreateResources(ctx, infra, spec, target, hypershiftEnv, createdInfra, createdIAM)
		if cleanupErr != nil {
			return "", errors.Join(createErr, cleanupErr)
		}
		return "", createErr
	}
}

func repairAdoptedClusterAfterUnconfirmedCreate(
	ctx context.Context,
	client unconfirmedCreateRecoveryClient,
	infra unconfirmedCreateRecoveryInfra,
	clusterID string,
	spec ClusterSpec,
	target TargetConfig,
	jwksPath string,
	hypershiftEnv []string,
	progress *deliveryProgress,
) error {
	progress.Info(ctx, fmt.Sprintf("Re-ensuring IAM resources for adopted cluster %s", clusterID))
	if _, err := ensureIAMWithRecovery(ctx, infra, spec, target, jwksPath, hypershiftEnv, progress); err != nil {
		return fmt.Errorf("re-ensure IAM for adopted cluster: %w", err)
	}

	progress.Info(ctx, fmt.Sprintf("Re-ensuring infrastructure for adopted cluster %s", clusterID))
	if _, err := ensureInfraWithRecovery(ctx, infra, spec, target, hypershiftEnv, progress); err != nil {
		return fmt.Errorf("re-ensure infrastructure for adopted cluster: %w", err)
	}

	progress.Info(ctx, fmt.Sprintf("Refreshing adopted cluster %s before update", clusterID))
	observedCluster, err := client.GetCluster(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("get adopted cluster %s: %w", clusterID, err)
	}

	updateSpec, err := BuildCLSClusterUpdateSpec(spec, observedCluster)
	if err != nil {
		return fmt.Errorf("build adopted cluster update spec: %w", err)
	}

	if _, err := client.UpdateCluster(ctx, clusterID, updateSpec); err != nil {
		return fmt.Errorf("update adopted cluster %s: %w", clusterID, err)
	}

	progress.Info(ctx, fmt.Sprintf("Adopted cluster %s repaired and updated", clusterID))
	return nil
}

func resolveClusterAfterUnconfirmedCreate(
	ctx context.Context,
	client clusterResolveClient,
	clusterName string,
	progress *deliveryProgress,
) (string, bool, error) {
	progress.Info(ctx, fmt.Sprintf("Create result for cluster %s was unconfirmed; probing for adoption", clusterName))

	ticker := time.NewTicker(unconfirmedCreateProbeInterval)
	defer ticker.Stop()

	timeout := time.After(unconfirmedCreateProbeTimeout)

	for {
		clusterID, err := client.ResolveClusterID(ctx, clusterName)
		switch {
		case err == nil:
			progress.Info(ctx, fmt.Sprintf("Adopted cluster %s after unconfirmed create", clusterID))
			return clusterID, true, nil
		case !errors.Is(err, ErrClusterNotFound):
			return "", false, fmt.Errorf("probe for cluster after unconfirmed create: %w", err)
		}

		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-timeout:
			return "", false, nil
		case <-ticker.C:
		}
	}
}

func deleteClusterIfPresent(
	ctx context.Context,
	client clusterDeleteClient,
	clusterName string,
	progress *deliveryProgress,
) (string, bool, error) {
	clusterID, err := client.ResolveClusterID(ctx, clusterName)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			progress.Info(ctx, fmt.Sprintf("Cluster %s already absent; continuing cleanup", clusterName))
			return "", false, nil
		}
		return "", false, fmt.Errorf("resolve cluster ID: %w", err)
	}

	progress.Info(ctx, fmt.Sprintf("Deleting cluster %s (ID: %s)", clusterName, clusterID))
	if err := client.DeleteCluster(ctx, clusterID); err != nil {
		if isCLSHTTPStatus(err, http.StatusNotFound) {
			progress.Info(ctx, fmt.Sprintf("Cluster %s already absent after resolve; continuing cleanup", clusterName))
			return clusterID, false, nil
		}
		return "", false, fmt.Errorf("delete cluster: %w", err)
	}

	return clusterID, true, nil
}

func cloneAnyMap(in map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

