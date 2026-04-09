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
	// Step 1: Open work directory
	wd, err := workdir.Open(destroyWorkDir)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 2: Check HasMetadata
	if !wd.HasMetadata() {
		err := fmt.Errorf("metadata.json not found in work-dir; cannot destroy cluster without metadata")
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 3: Check HasInstaller
	if !wd.HasInstaller() {
		err := fmt.Errorf("openshift-install binary not found in work-dir; cannot destroy cluster")
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 4: Lock work directory
	if err := wd.Lock(); err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "already_running",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}
	defer wd.Unlock()

	// Step 5: Get infraID
	infraID, err := wd.InfraID()
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         fmt.Sprintf("failed to read infra ID from metadata.json: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 6: Resolve AWS credentials from config (if provided) or use ambient
	var awsEnv map[string]string
	if destroyConfigPath != "" {
		cfg, err := config.LoadConfig(destroyConfigPath)
		if err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "config_error",
				Message:         err.Error(),
				RequiresDestroy: false,
			})
			return err
		}
		awsEnv, err = credentials.Resolve(credentials.AWSCredentials{
			AccessKeyID:     cfg.Platform.AWS.Credentials.AccessKeyID,
			SecretAccessKey: cfg.Platform.AWS.Credentials.SecretAccessKey,
			CredentialsFile: cfg.Platform.AWS.Credentials.CredentialsFile,
			Profile:         cfg.Platform.AWS.Credentials.Profile,
			RoleARN:         cfg.Platform.AWS.Credentials.RoleARN,
		})
		if err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "config_error",
				Message:         fmt.Sprintf("failed to resolve AWS credentials: %v", err),
				RequiresDestroy: false,
			})
			return err
		}
	}

	// Step 7: Create Installer instance
	inst := &installer.Installer{
		WorkDir:       wd.Path,
		InstallerPath: wd.InstallerPath(),
		AWSEnv:        awsEnv,
	}

	// Step 8: Time the destroy and call inst.DestroyCluster
	logPath := wd.LogPath()
	start := time.Now()
	err = inst.DestroyCluster(logPath)
	elapsed := int(time.Since(start).Seconds())

	// Step 9: Write DestroyResult
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
