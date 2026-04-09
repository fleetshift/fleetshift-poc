package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/credentials"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var destroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "Destroy an existing OpenShift cluster",
	Long:  "Tears down a cluster using the metadata and installer binary from the work directory",
	RunE:  runDestroy,
}

var (
	destroyWorkDir    string
	destroyConfigPath string
)

func init() {
	destroyCmd.Flags().StringVar(&destroyWorkDir, "work-dir", "", "Path to work directory (required)")
	destroyCmd.Flags().StringVar(&destroyConfigPath, "config", "", "Path to cluster configuration file (optional, for AWS credentials)")
	destroyCmd.MarkFlagRequired("work-dir")
	rootCmd.AddCommand(destroyCmd)
}

func runDestroy(cmd *cobra.Command, args []string) error {
	wd, err := workdir.Open(destroyWorkDir)
	if err != nil {
		return output.WriteError(os.Stdout,"workdir_error", err, false)
	}

	if !wd.HasMetadata() {
		return output.WriteError(os.Stdout,"workdir_error", fmt.Errorf("metadata.json not found in work-dir; cannot destroy cluster without metadata"), false)
	}

	if !wd.HasInstaller() {
		return output.WriteError(os.Stdout,"workdir_error", fmt.Errorf("openshift-install binary not found in work-dir; cannot destroy cluster"), false)
	}

	if err := wd.Lock(); err != nil {
		return output.WriteError(os.Stdout,"already_running", err, false)
	}
	defer wd.Unlock()

	infraID, err := wd.InfraID()
	if err != nil {
		return output.WriteError(os.Stdout,"workdir_error", fmt.Errorf("failed to read infra ID from metadata.json: %w", err), false)
	}

	// Resolve AWS credentials from config (if provided) or use ambient
	var awsEnv map[string]string
	if destroyConfigPath != "" {
		cfg, err := config.LoadConfig(destroyConfigPath)
		if err != nil {
			return output.WriteError(os.Stdout,"config_error", err, false)
		}
		awsEnv, err = credentials.ResolveFromConfig(&cfg.Platform.AWS.Credentials)
		if err != nil {
			return output.WriteError(os.Stdout,"config_error", fmt.Errorf("failed to resolve AWS credentials: %w", err), false)
		}
	}

	inst := &installer.Installer{
		WorkDir:       wd.Path,
		InstallerPath: wd.InstallerPath(),
		AWSEnv:        awsEnv,
	}

	logPath := wd.LogPath()
	start := time.Now()
	err = inst.DestroyCluster(logPath)
	elapsed := int(time.Since(start).Seconds())

	if err != nil {
		output.WriteDestroyResult(os.Stdout, output.DestroyResult{
			Action:         "destroy",
			Status:         "failed",
			InfraID:        infraID,
			Error:          err.Error(),
			LogTail:        wd.LogTail(50),
			ElapsedSeconds: elapsed,
		})
		return err
	}

	output.WriteDestroyResult(os.Stdout, output.DestroyResult{
		Action:         "destroy",
		Status:         "succeeded",
		InfraID:        infraID,
		ElapsedSeconds: elapsed,
	})

	return nil
}
