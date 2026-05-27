package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	defaultClusterPollInterval = 15 * time.Second
	defaultClusterPollTimeout  = 20 * time.Minute
)

var (
	clusterPollInterval  = defaultClusterPollInterval
	clusterPollTimeout   = defaultClusterPollTimeout
	nodepoolPollInterval = defaultClusterPollInterval
	nodepoolPollTimeout  = defaultClusterPollTimeout
)

type nodepoolStatusClient interface {
	ListNodepools(ctx context.Context, clusterID string) ([]map[string]any, error)
	GetNodepoolStatus(ctx context.Context, nodepoolID string) (map[string]any, error)
}

type resourceStatusSummary struct {
	Phase   string
	Reason  string
	Message string
}

type failureConditionSnapshot struct {
	Controller string `json:"controller,omitempty"`
	Type       string `json:"type"`
	Status     string `json:"status,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
}

type failureResourceSnapshot struct {
	Phase             string                     `json:"phase,omitempty"`
	Reason            string                     `json:"reason,omitempty"`
	Message           string                     `json:"message,omitempty"`
	APIServerPresent  bool                       `json:"api_server_present"`
	ProblemConditions []failureConditionSnapshot `json:"problem_conditions,omitempty"`
}

type failureNodepoolSnapshot struct {
	ID                string                     `json:"id,omitempty"`
	Name              string                     `json:"name,omitempty"`
	Phase             string                     `json:"phase,omitempty"`
	Reason            string                     `json:"reason,omitempty"`
	Message           string                     `json:"message,omitempty"`
	ProblemConditions []failureConditionSnapshot `json:"problem_conditions,omitempty"`
}

type failureStatusSnapshot struct {
	ClusterID      string                    `json:"cluster_id"`
	ClusterName    string                    `json:"cluster_name,omitempty"`
	ReleaseVersion string                    `json:"release_version,omitempty"`
	Cluster        failureResourceSnapshot   `json:"cluster"`
	Nodepools      []failureNodepoolSnapshot `json:"nodepools,omitempty"`
}

func parseResourceStatusSummary(resourceData map[string]any) resourceStatusSummary {
	summary := resourceStatusSummary{
		Phase:  "Unknown",
		Reason: "Unknown",
	}

	status, ok := resourceData["status"].(map[string]any)
	if !ok {
		return summary
	}
	if phase, ok := status["phase"].(string); ok && phase != "" {
		summary.Phase = phase
	}
	if reason, ok := status["reason"].(string); ok && reason != "" {
		summary.Reason = reason
	}
	if message, ok := status["message"].(string); ok {
		summary.Message = message
	}
	return summary
}

func formatResourceStatusMessage(resource string, resourceData map[string]any) string {
	summary := parseResourceStatusSummary(resourceData)
	return fmt.Sprintf(
		`%s status: phase=%s reason=%s message=%q`,
		resource,
		summary.Phase,
		summary.Reason,
		summary.Message,
	)
}

func emitResourceStatusProgress(
	ctx context.Context,
	progress *deliveryProgress,
	resource string,
	resourceData map[string]any,
) {
	progress.Event(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   formatResourceStatusMessage(resource, resourceData),
	})
}

func emitClusterReadyTransition(ctx context.Context, progress *deliveryProgress) {
	progress.Event(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Cluster readiness satisfied; proceeding with guest bootstrap and desired nodepool health checks",
	})
}

func emitFailureStatusSnapshot(
	ctx context.Context,
	client *CLSClient,
	clusterID string,
	clusterName string,
	progress *deliveryProgress,
) error {
	if progress == nil {
		return nil
	}

	snapshot, err := collectFailureStatusSnapshot(ctx, client, clusterID, clusterName)
	if err != nil {
		return err
	}

	detail, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal failure snapshot: %w", err)
	}

	progress.Event(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventWarning,
		Message:   "Redacted failure snapshot: " + string(detail),
		Detail:    detail,
	})
	return nil
}

func collectFailureStatusSnapshot(
	ctx context.Context,
	client *CLSClient,
	clusterID string,
	clusterName string,
) (failureStatusSnapshot, error) {
	snapshot := failureStatusSnapshot{
		ClusterID:   clusterID,
		ClusterName: clusterName,
	}
	if clusterID == "" {
		return snapshot, fmt.Errorf("cluster ID is required for failure snapshot")
	}

	clusterData, err := client.GetCluster(ctx, clusterID)
	if err != nil {
		return snapshot, fmt.Errorf("get cluster: %w", err)
	}
	if name, _ := clusterData["name"].(string); name != "" {
		snapshot.ClusterName = name
	}
	snapshot.ReleaseVersion = extractReleaseVersion(clusterData)

	clusterStatusData, err := client.GetClusterStatus(ctx, clusterID)
	if err != nil {
		return snapshot, fmt.Errorf("get cluster status: %w", err)
	}

	clusterStatus := parseResourceStatusSummary(clusterStatusData)
	snapshot.Cluster = failureResourceSnapshot{
		Phase:             clusterStatus.Phase,
		Reason:            clusterStatus.Reason,
		Message:           clusterStatus.Message,
		APIServerPresent:  hasGuestAPIEndpoint(clusterStatusData),
		ProblemConditions: extractProblemConditions(clusterStatusData),
	}

	nodepools, err := client.ListNodepools(ctx, clusterID)
	if err != nil {
		return snapshot, fmt.Errorf("list nodepools: %w", err)
	}
	sort.Slice(nodepools, func(i, j int) bool {
		left, _ := nodepools[i]["name"].(string)
		right, _ := nodepools[j]["name"].(string)
		return left < right
	})

	snapshot.Nodepools = make([]failureNodepoolSnapshot, 0, len(nodepools))
	for _, nodepool := range nodepools {
		nodepoolID, _ := nodepool["id"].(string)
		nodepoolName, _ := nodepool["name"].(string)

		statusData, err := client.GetNodepoolStatus(ctx, nodepoolID)
		if err != nil {
			return snapshot, fmt.Errorf("get nodepool status for %s: %w", nodepoolName, err)
		}

		nodepoolStatus := parseResourceStatusSummary(statusData)
		snapshot.Nodepools = append(snapshot.Nodepools, failureNodepoolSnapshot{
			ID:                nodepoolID,
			Name:              nodepoolName,
			Phase:             nodepoolStatus.Phase,
			Reason:            nodepoolStatus.Reason,
			Message:           nodepoolStatus.Message,
			ProblemConditions: extractProblemConditions(statusData),
		})
	}

	return snapshot, nil
}

func extractReleaseVersion(clusterData map[string]any) string {
	spec, ok := clusterData["spec"].(map[string]any)
	if !ok {
		return ""
	}
	if release, ok := spec["release"].(map[string]any); ok {
		if version, ok := release["version"].(string); ok && version != "" {
			return version
		}
	}
	if version, ok := spec["releaseVersion"].(string); ok {
		return version
	}
	return ""
}

func hasGuestAPIEndpoint(statusData map[string]any) bool {
	_, err := ResolveGuestAPIEndpoint(statusData)
	return err == nil
}

func extractProblemConditions(resourceData map[string]any) []failureConditionSnapshot {
	var result []failureConditionSnapshot

	if controllerStatus, ok := resourceData["controller_status"].([]any); ok {
		for _, rawController := range controllerStatus {
			controllerMap, ok := rawController.(map[string]any)
			if !ok {
				continue
			}
			controllerName, _ := controllerMap["controller_name"].(string)
			result = appendProblemConditions(result, controllerName, controllerMap["conditions"])
		}
	}
	if len(result) > 0 {
		return result
	}

	status, ok := resourceData["status"].(map[string]any)
	if !ok {
		return nil
	}
	return appendProblemConditions(nil, "", status["conditions"])
}

func appendProblemConditions(
	existing []failureConditionSnapshot,
	controller string,
	rawConditions any,
) []failureConditionSnapshot {
	conditions, ok := rawConditions.([]any)
	if !ok {
		return existing
	}

	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if !ok || !shouldIncludeProblemCondition(condition) {
			continue
		}

		snapshot := failureConditionSnapshot{
			Controller: controller,
			Type:       stringField(condition, "type"),
			Status:     stringField(condition, "status"),
			Reason:     stringField(condition, "reason"),
			Message:    stringField(condition, "message"),
		}
		existing = append(existing, snapshot)
	}
	return existing
}

func shouldIncludeProblemCondition(condition map[string]any) bool {
	condStatus := stringField(condition, "status")
	condType := stringField(condition, "type")

	switch condStatus {
	case "False", "Unknown":
		return true
	case "True":
		switch condType {
		case "Degraded", "Failed", "Failing", "Progressing", "Deleting":
			return true
		}
	}
	return false
}

func stringField(m map[string]any, key string) string {
	value, _ := m[key].(string)
	return value
}

// ResolveGuestAPIEndpoint resolves the guest API endpoint from cluster status data.
// It first scans controller_status[].conditions[] for a condition with type="APIServer"
// whose message starts with "https://". If not found, it falls back to the api_endpoint field.
// Returns an error if neither source provides a valid endpoint.
func ResolveGuestAPIEndpoint(statusData map[string]any) (string, error) {
	// Primary: scan controller_status for APIServer condition
	if controllerStatus, ok := statusData["controller_status"].([]any); ok {
		for _, ctrl := range controllerStatus {
			ctrlMap, ok := ctrl.(map[string]any)
			if !ok {
				continue
			}
			conditions, ok := ctrlMap["conditions"].([]any)
			if !ok {
				continue
			}
			for _, cond := range conditions {
				condMap, ok := cond.(map[string]any)
				if !ok {
					continue
				}
				condType, _ := condMap["type"].(string)
				if condType != "APIServer" {
					continue
				}
				message, _ := condMap["message"].(string)
				if strings.HasPrefix(message, "https://") {
					return message, nil
				}
			}
		}
	}

	// Fallback: check api_endpoint field
	if apiEndpoint, ok := statusData["api_endpoint"].(string); ok && apiEndpoint != "" {
		return apiEndpoint, nil
	}

	return "", fmt.Errorf("guest API endpoint not found in cluster status")
}

// PollDesiredNodepoolsHealthy waits until every desired nodepool is present and
// reports Ready. Failed nodepools terminate the reconcile immediately.
func PollDesiredNodepoolsHealthy(
	ctx context.Context,
	client nodepoolStatusClient,
	clusterID string,
	clusterName string,
	desired []NodepoolSpec,
	progress *deliveryProgress,
) error {
	if len(desired) == 0 {
		return nil
	}

	ticker := time.NewTicker(nodepoolPollInterval)
	defer ticker.Stop()

	timeout := time.After(nodepoolPollTimeout)

	for {
		allReady, err := checkDesiredNodepoolsHealthy(ctx, client, clusterID, clusterName, desired, progress)
		if err != nil {
			return err
		}
		if allReady {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for desired nodepools to become healthy")
		case <-ticker.C:
		}
	}
}

// PollClusterReady polls the cluster status until it reaches "Ready" phase or fails.
// It polls every 15 seconds for up to 20 minutes.
// Returns nil when phase="Ready", error when phase="Failed", or error on timeout.
func PollClusterReady(ctx context.Context, client *CLSClient, clusterID string, progress *deliveryProgress) error {
	ticker := time.NewTicker(clusterPollInterval)
	defer ticker.Stop()

	timeout := time.After(clusterPollTimeout)

	var clusterStatus resourceStatusSummary
	for first := true; ; first = false {
		if !first {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timeout:
				return fmt.Errorf("timeout waiting for cluster to become ready (phase=%s)", clusterStatus.Phase)
			case <-ticker.C:
			}
		}

		clusterData, err := client.GetCluster(ctx, clusterID)
		if err != nil {
			return fmt.Errorf("get cluster: %w", err)
		}

		clusterStatus = parseResourceStatusSummary(clusterData)
		emitResourceStatusProgress(ctx, progress, "Cluster", clusterData)

		if clusterStatus.Phase == "Ready" {
			return nil
		}
		if clusterStatus.Phase == "Failed" {
			return fmt.Errorf("cluster provisioning failed: %s", clusterStatus.Message)
		}
	}
}

// PollClusterDeleted polls until the cluster is deleted (404 response).
// It polls every 15 seconds for up to 20 minutes.
// Returns nil when cluster returns 404, error on timeout.
func PollClusterDeleted(ctx context.Context, client *CLSClient, clusterID string, progress *deliveryProgress) error {
	ticker := time.NewTicker(clusterPollInterval)
	defer ticker.Stop()

	timeout := time.After(clusterPollTimeout)

	for first := true; ; first = false {
		if !first {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timeout:
				return fmt.Errorf("timeout waiting for cluster deletion")
			case <-ticker.C:
			}
		}

		_, err := client.GetCluster(ctx, clusterID)
		if err != nil {
			if isCLSHTTPStatus(err, http.StatusNotFound) {
				progress.Info(ctx, "Cluster deleted")
				return nil
			}
			return fmt.Errorf("get cluster: %w", err)
		}

		progress.Info(ctx, "Waiting for cluster deletion")
	}
}

func checkDesiredNodepoolsHealthy(
	ctx context.Context,
	client nodepoolStatusClient,
	clusterID string,
	clusterName string,
	desired []NodepoolSpec,
	progress *deliveryProgress,
) (bool, error) {
	observed, err := client.ListNodepools(ctx, clusterID)
	if err != nil {
		return false, fmt.Errorf("list nodepools: %w", err)
	}

	observedByName := make(map[string]string, len(observed))
	for _, nodepool := range observed {
		name, _ := nodepool["name"].(string)
		id, _ := nodepool["id"].(string)
		if name == "" || id == "" {
			continue
		}
		observedByName[name] = id
	}

	desiredNames := make([]string, 0, len(desired))
	for _, np := range desired {
		desiredNames = append(desiredNames, NodepoolName(clusterName, np.ID))
	}
	sort.Strings(desiredNames)

	allReady := true
	for _, name := range desiredNames {
		nodepoolID, ok := observedByName[name]
		if !ok {
			progress.Event(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   fmt.Sprintf("Nodepool %s not yet visible", name),
			})
			allReady = false
			continue
		}

		statusData, err := client.GetNodepoolStatus(ctx, nodepoolID)
		if err != nil {
			return false, fmt.Errorf("get nodepool status for %s: %w", name, err)
		}

		nodepoolStatus := parseResourceStatusSummary(statusData)
		emitResourceStatusProgress(ctx, progress, fmt.Sprintf("Nodepool %s", name), statusData)

		if nodepoolStatus.Phase == "Failed" {
			statusMsg := nodepoolStatus.Message
			if statusMsg == "" {
				statusMsg = "unknown failure"
			}
			return false, fmt.Errorf("nodepool %s failed: %s", name, statusMsg)
		}
		if nodepoolStatus.Phase != "Ready" {
			allReady = false
		}
	}

	return allReady, nil
}
