package gcphcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrClusterNotFound = errors.New("cluster not found")

const defaultCLSHTTPTimeout = 30 * time.Second

type CLSClient struct {
	baseURL    string
	token      string
	userEmail  string
	httpClient *http.Client
}

type clsHTTPError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *clsHTTPError) Error() string {
	return fmt.Sprintf("CLS API %s %s failed (HTTP %d): %s", e.Method, e.Path, e.StatusCode, e.Body)
}

func NewCLSClient(baseURL, brokerToken, brokerEmail string, httpClient *http.Client) *CLSClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultCLSHTTPTimeout}
	}
	return &CLSClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		token:      brokerToken,
		userEmail:  brokerEmail,
		httpClient: httpClient,
	}
}

func (c *CLSClient) CreateCluster(ctx context.Context, spec map[string]any) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodPost, "/api/v1/clusters", spec)
}

func (c *CLSClient) GetCluster(ctx context.Context, clusterID string) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodGet, "/api/v1/clusters/"+clusterID, nil)
}

func (c *CLSClient) UpdateCluster(ctx context.Context, clusterID string, spec map[string]any) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodPut, "/api/v1/clusters/"+clusterID, spec)
}

func (c *CLSClient) GetClusterStatus(ctx context.Context, clusterID string) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodGet, "/api/v1/clusters/"+clusterID+"/status", nil)
}

func (c *CLSClient) GetNodepoolStatus(ctx context.Context, nodepoolID string) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodGet, "/api/v1/nodepools/"+nodepoolID+"/status", nil)
}

func (c *CLSClient) ListClusters(ctx context.Context) ([]map[string]any, error) {
	result, err := c.doJSON(ctx, http.MethodGet, "/api/v1/clusters", nil)
	if err != nil {
		return nil, err
	}
	return requiredObjectListField(result, "clusters")
}

func (c *CLSClient) DeleteCluster(ctx context.Context, clusterID string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, "/api/v1/clusters/"+clusterID+"?force=true", nil)
	return err
}

func (c *CLSClient) CreateNodepool(ctx context.Context, spec map[string]any) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodPost, "/api/v1/nodepools", spec)
}

func (c *CLSClient) UpdateNodepool(ctx context.Context, nodepoolID string, spec map[string]any) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodPut, "/api/v1/nodepools/"+nodepoolID, spec)
}

func (c *CLSClient) ListNodepools(ctx context.Context, clusterID string) ([]map[string]any, error) {
	query := url.Values{}
	query.Set("clusterId", clusterID)

	result, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodepools?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	return requiredObjectListField(result, "nodepools")
}

func (c *CLSClient) DeleteNodepool(ctx context.Context, nodepoolID string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, "/api/v1/nodepools/"+nodepoolID, nil)
	return err
}

// ResolveClusterID finds a cluster by name and returns its backend ID.
func (c *CLSClient) ResolveClusterID(ctx context.Context, clusterName string) (string, error) {
	clusters, err := c.ListClusters(ctx)
	if err != nil {
		return "", fmt.Errorf("list clusters: %w", err)
	}
	for _, cl := range clusters {
		if name, _ := cl["name"].(string); name == clusterName {
			if id, _ := cl["id"].(string); id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("%w: %q", ErrClusterNotFound, clusterName)
}

func (c *CLSClient) doJSON(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-User-Email", c.userEmail)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read CLS response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, &clsHTTPError{
			Method:     method,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}

	if len(respBody) == 0 {
		return nil, nil
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse CLS response: %w", err)
	}
	return result, nil
}

func requiredObjectListField(result map[string]any, field string) ([]map[string]any, error) {
	if result == nil {
		return nil, fmt.Errorf("CLS response missing %s field", field)
	}

	raw, ok := result[field]
	if !ok {
		return nil, fmt.Errorf("CLS response missing %s field", field)
	}
	if raw == nil {
		return []map[string]any{}, nil
	}

	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("CLS response field %q has unexpected type %T", field, raw)
	}

	out := make([]map[string]any, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("CLS response field %q item %d has unexpected type %T", field, i, item)
		}
		out = append(out, m)
	}
	return out, nil
}

func isCLSHTTPStatus(err error, statusCode int) bool {
	var httpErr *clsHTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == statusCode
}
