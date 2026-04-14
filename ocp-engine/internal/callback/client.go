// Package callback provides a gRPC client for reporting provision
// progress back to fleetshift-server's OCPEngineCallbackService.
package callback

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// CompletionData holds the artifacts produced by a successful provision.
type CompletionData struct {
	InfraID, ClusterUUID, APIServer, Region string
	Kubeconfig, CACert, SSHPrivateKey, SSHPublicKey, MetadataJSON []byte
	RecoveryAttempted                                              bool
	ElapsedSeconds, Attempt                                        int32
}

// FailureData describes a terminal provision failure.
type FailureData struct {
	Phase, FailureReason, FailureMessage, LogTail string
	RequiresDestroy, RecoveryAttempted             bool
	Attempt                                        int32
}

// Client wraps the generated OCPEngineCallbackServiceClient with convenience
// methods that inject the cluster ID and bearer token automatically.
type Client struct {
	conn      *grpc.ClientConn
	client    OCPEngineCallbackServiceClient
	clusterID string
	token     string
}

// New dials the callback server at addr and returns a ready Client.
// The connection uses insecure (plaintext) transport credentials.
func New(addr, clusterID, token string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("callback: dial %s: %w", addr, err)
	}
	return &Client{
		conn:      conn,
		client:    NewOCPEngineCallbackServiceClient(conn),
		clusterID: clusterID,
		token:     token,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// withAuth returns a context carrying the bearer token as gRPC metadata.
func (c *Client) withAuth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// ReportPhaseResult reports the outcome of a single provision phase.
func (c *Client) ReportPhaseResult(ctx context.Context, phase, status string, elapsed int32, errMsg string, attempt int32) error {
	_, err := c.client.ReportPhaseResult(c.withAuth(ctx), &OCPEnginePhaseResultRequest{
		ClusterId:      c.clusterID,
		Phase:          phase,
		Status:         status,
		ElapsedSeconds: elapsed,
		Error:          errMsg,
		Attempt:        attempt,
	})
	return err
}

// ReportMilestone reports a notable event during provisioning.
func (c *Client) ReportMilestone(ctx context.Context, event string, elapsed, attempt int32) error {
	_, err := c.client.ReportMilestone(c.withAuth(ctx), &OCPEngineMilestoneRequest{
		ClusterId:      c.clusterID,
		Event:          event,
		ElapsedSeconds: elapsed,
		Attempt:        attempt,
	})
	return err
}

// ReportCompletion reports a successful provision along with all produced artifacts.
func (c *Client) ReportCompletion(ctx context.Context, data CompletionData) error {
	_, err := c.client.ReportCompletion(c.withAuth(ctx), &OCPEngineCompletionRequest{
		ClusterId:         c.clusterID,
		InfraId:           data.InfraID,
		ClusterUuid:       data.ClusterUUID,
		ApiServer:         data.APIServer,
		Kubeconfig:        data.Kubeconfig,
		CaCert:            data.CACert,
		SshPrivateKey:     data.SSHPrivateKey,
		SshPublicKey:      data.SSHPublicKey,
		Region:            data.Region,
		MetadataJson:      data.MetadataJSON,
		RecoveryAttempted: data.RecoveryAttempted,
		ElapsedSeconds:    data.ElapsedSeconds,
		Attempt:           data.Attempt,
	})
	return err
}

// ReportFailure reports a terminal provision failure.
func (c *Client) ReportFailure(ctx context.Context, data FailureData) error {
	_, err := c.client.ReportFailure(c.withAuth(ctx), &OCPEngineFailureRequest{
		ClusterId:         c.clusterID,
		Phase:             data.Phase,
		FailureReason:     data.FailureReason,
		FailureMessage:    data.FailureMessage,
		LogTail:           data.LogTail,
		RequiresDestroy:   data.RequiresDestroy,
		RecoveryAttempted: data.RecoveryAttempted,
		Attempt:           data.Attempt,
	})
	return err
}
