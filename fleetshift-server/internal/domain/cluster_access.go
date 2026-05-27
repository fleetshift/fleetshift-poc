package domain

import (
	"context"
	"time"
)

// ClusterAccessProvider mints short-lived credentials for accessing a
// provisioned guest cluster. Each addon that produces kubernetes targets
// implements this interface with its own auth exchange logic.
type ClusterAccessProvider interface {
	MintCredential(ctx context.Context, callerToken string, target TargetInfo) (*ClusterCredential, error)
}

// ClusterCredential is a short-lived bearer token for a guest cluster.
type ClusterCredential struct {
	Token      string
	Expiration time.Time
}
