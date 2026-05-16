package gcphcp

import (
	"context"
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

// ParseClusterPhase extracts the status.phase field from cluster data.
// Returns "Unknown" if the field is not found or not a string.
func ParseClusterPhase(clusterData map[string]any) string {
	status, ok := clusterData["status"].(map[string]any)
	if !ok {
		return "Unknown"
	}
	phase, ok := status["phase"].(string)
	if !ok {
		return "Unknown"
	}
	return phase
}

// ParseNodepoolPhase extracts status.phase from a nodepool status response.
// Returns "Unknown" if the field is missing or malformed.
func ParseNodepoolPhase(nodepoolStatus map[string]any) string {
	status, ok := nodepoolStatus["status"].(map[string]any)
	if !ok {
		return "Unknown"
	}
	phase, ok := status["phase"].(string)
	if !ok {
		return "Unknown"
	}
	return phase
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
	desired []NodepoolSpec,
	signaler *domain.DeliverySignaler,
) error {
	if len(desired) == 0 {
		return nil
	}

	ticker := time.NewTicker(nodepoolPollInterval)
	defer ticker.Stop()

	timeout := time.After(nodepoolPollTimeout)

	for {
		allReady, err := checkDesiredNodepoolsHealthy(ctx, client, clusterID, desired, signaler)
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
func PollClusterReady(ctx context.Context, client *CLSClient, clusterID string, signaler *domain.DeliverySignaler) error {
	ticker := time.NewTicker(clusterPollInterval)
	defer ticker.Stop()

	timeout := time.After(clusterPollTimeout)

	// Check immediately first
	clusterData, err := client.GetCluster(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}

	phase := ParseClusterPhase(clusterData)
	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   fmt.Sprintf("Cluster phase: %s", phase),
	})

	if phase == "Ready" {
		return nil
	}
	if phase == "Failed" {
		statusMsg := ""
		if status, ok := clusterData["status"].(map[string]any); ok {
			if msg, ok := status["message"].(string); ok {
				statusMsg = msg
			}
		}
		return fmt.Errorf("cluster provisioning failed: %s", statusMsg)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for cluster to become ready (phase=%s)", phase)
		case <-ticker.C:
			clusterData, err := client.GetCluster(ctx, clusterID)
			if err != nil {
				return fmt.Errorf("get cluster: %w", err)
			}

			phase = ParseClusterPhase(clusterData)
			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   fmt.Sprintf("Cluster phase: %s", phase),
			})

			if phase == "Ready" {
				return nil
			}
			if phase == "Failed" {
				statusMsg := ""
				if status, ok := clusterData["status"].(map[string]any); ok {
					if msg, ok := status["message"].(string); ok {
						statusMsg = msg
					}
				}
				return fmt.Errorf("cluster provisioning failed: %s", statusMsg)
			}
		}
	}
}

// PollClusterDeleted polls until the cluster is deleted (404 response).
// It polls every 15 seconds for up to 20 minutes.
// Returns nil when cluster returns 404, error on timeout.
func PollClusterDeleted(ctx context.Context, client *CLSClient, clusterID string, signaler *domain.DeliverySignaler) error {
	ticker := time.NewTicker(clusterPollInterval)
	defer ticker.Stop()

	timeout := time.After(clusterPollTimeout)

	// Check immediately first
	_, err := client.GetCluster(ctx, clusterID)
	if err != nil {
		if isCLSHTTPStatus(err, http.StatusNotFound) {
			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   "Cluster deleted",
			})
			return nil
		}
		return fmt.Errorf("get cluster: %w", err)
	}

	signaler.Emit(ctx, domain.DeliveryEvent{
		Timestamp: time.Now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   "Waiting for cluster deletion",
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for cluster deletion")
		case <-ticker.C:
			_, err := client.GetCluster(ctx, clusterID)
			if err != nil {
				if isCLSHTTPStatus(err, http.StatusNotFound) {
					signaler.Emit(ctx, domain.DeliveryEvent{
						Timestamp: time.Now(),
						Kind:      domain.DeliveryEventProgress,
						Message:   "Cluster deleted",
					})
					return nil
				}
				return fmt.Errorf("get cluster: %w", err)
			}

			signaler.Emit(ctx, domain.DeliveryEvent{
				Timestamp: time.Now(),
				Kind:      domain.DeliveryEventProgress,
				Message:   "Waiting for cluster deletion",
			})
		}
	}
}

func checkDesiredNodepoolsHealthy(
	ctx context.Context,
	client nodepoolStatusClient,
	clusterID string,
	desired []NodepoolSpec,
	signaler *domain.DeliverySignaler,
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
		desiredNames = append(desiredNames, np.Name)
	}
	sort.Strings(desiredNames)

	allReady := true
	for _, name := range desiredNames {
		nodepoolID, ok := observedByName[name]
		if !ok {
			signaler.Emit(ctx, domain.DeliveryEvent{
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

		phase := ParseNodepoolPhase(statusData)
		statusMsg := ""
		if status, ok := statusData["status"].(map[string]any); ok {
			if msg, ok := status["message"].(string); ok {
				statusMsg = msg
			}
		}

		message := fmt.Sprintf("Nodepool %s phase: %s", name, phase)
		if statusMsg != "" {
			message += " — " + statusMsg
		}
		signaler.Emit(ctx, domain.DeliveryEvent{
			Timestamp: time.Now(),
			Kind:      domain.DeliveryEventProgress,
			Message:   message,
		})

		if phase == "Failed" {
			if statusMsg == "" {
				statusMsg = "unknown failure"
			}
			return false, fmt.Errorf("nodepool %s failed: %s", name, statusMsg)
		}
		if phase != "Ready" {
			allReady = false
		}
	}

	return allReady, nil
}
