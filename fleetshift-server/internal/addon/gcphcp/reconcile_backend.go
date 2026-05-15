package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type nodepoolReconcileClient interface {
	ListNodepools(ctx context.Context, clusterID string) ([]map[string]any, error)
	CreateNodepool(ctx context.Context, spec map[string]any) (map[string]any, error)
	UpdateNodepool(ctx context.Context, nodepoolID string, spec map[string]any) (map[string]any, error)
	DeleteNodepool(ctx context.Context, nodepoolID string) error
}

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
	setOptionalString(clonedSpec, "releaseVersion", spec.ReleaseVersion)
	setOptionalString(clonedSpec, "channelGroup", spec.ChannelGroup)

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
	desired []NodepoolSpec,
	signaler *domain.DeliverySignaler,
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

	seenDesired := make(map[string]struct{}, len(desired))
	for _, np := range desired {
		if _, exists := seenDesired[np.Name]; exists {
			return fmt.Errorf("duplicate desired nodepool name %q", np.Name)
		}
		seenDesired[np.Name] = struct{}{}
	}

	for _, np := range desired {
		if _, seen := observedByName[np.Name]; !seen {
			emitProgress(signaler, ctx, fmt.Sprintf("Creating nodepool: %s", np.Name))
			if _, err := client.CreateNodepool(ctx, BuildCLSNodepoolSpec(np, clusterID)); err != nil {
				return fmt.Errorf("create nodepool %s: %w", np.Name, err)
			}
			continue
		}
	}

	for _, np := range desired {
		if nodepoolID, ok := observedByName[np.Name]; ok {
			emitProgress(signaler, ctx, fmt.Sprintf("Updating nodepool: %s", np.Name))
			if _, err := client.UpdateNodepool(ctx, nodepoolID, BuildCLSNodepoolSpec(np, clusterID)); err != nil {
				return fmt.Errorf("update nodepool %s: %w", np.Name, err)
			}
		}
	}

	var removed []string
	for name := range observedByName {
		if _, ok := seenDesired[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)

	for _, name := range removed {
		emitProgress(signaler, ctx, fmt.Sprintf("Deleting removed nodepool: %s", name))
		if err := client.DeleteNodepool(ctx, observedByName[name]); err != nil {
			return fmt.Errorf("delete nodepool %s: %w", name, err)
		}
	}

	return nil
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

func setOptionalString(dst map[string]any, key, value string) {
	if value == "" {
		delete(dst, key)
		return
	}
	dst[key] = value
}

func emitProgress(signaler *domain.DeliverySignaler, ctx context.Context, message string) {
	if signaler == nil {
		return
	}
	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   message,
	})
}
