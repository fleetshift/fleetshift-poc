package cmd

import (
	"fmt"
	"os"

	"github.com/ocp-engine/internal/callback"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ocp-engine",
	Short: "Standalone OCP cluster provisioning CLI",
	Long:  "A stateless CLI tool that wraps openshift-install for provisioning and deprovisioning OpenShift 4.20 clusters on AWS.",
}

var (
	callbackURL   string
	clusterID     string
	callbackToken string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&callbackURL, "callback-url", "", "gRPC address for callback reporting (optional)")
	rootCmd.PersistentFlags().StringVar(&clusterID, "cluster-id", "", "Cluster ID for callback reporting (required with --callback-url)")
	rootCmd.PersistentFlags().StringVar(&callbackToken, "callback-token", "", "Bearer token for callback authentication (required with --callback-url)")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// newCallbackClient creates a callback client when --callback-url is set.
// Returns (nil, nil) when callback is not configured (stdout-only mode).
func newCallbackClient() (*callback.Client, error) {
	if callbackURL == "" {
		return nil, nil // not configured — stdout JSON mode
	}
	if clusterID == "" {
		return nil, fmt.Errorf("--cluster-id is required when --callback-url is set")
	}

	// Read token from env var first, fall back to CLI flag
	token := os.Getenv("OCP_CALLBACK_TOKEN")
	if token == "" {
		token = callbackToken
	}
	if token == "" {
		return nil, fmt.Errorf("OCP_CALLBACK_TOKEN env var or --callback-token flag is required when --callback-url is set")
	}
	return callback.New(callbackURL, clusterID, token)
}
