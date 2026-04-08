package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/credentials"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/prereq"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var provisionCmd = &cobra.Command{
	Use:   "provision",
	Short: "Provision a new OpenShift cluster",
	Long:  "Executes the complete cluster provisioning workflow through all phases (extract, install-config, manifests, ignition, cluster)",
	RunE:  runProvision,
}

var (
	provisionConfigPath string
	provisionWorkDir    string
)

func init() {
	provisionCmd.Flags().StringVar(&provisionConfigPath, "config", "", "Path to cluster configuration file (required)")
	provisionCmd.Flags().StringVar(&provisionWorkDir, "work-dir", "", "Path to work directory (required)")
	provisionCmd.MarkFlagRequired("config")
	provisionCmd.MarkFlagRequired("work-dir")
	rootCmd.AddCommand(provisionCmd)
}

func runProvision(cmd *cobra.Command, args []string) error {
	// Step 1: Validate prerequisites
	if err := prereq.Validate(); err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "prereq_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 2: Load config
	cfg, err := config.LoadConfig(provisionConfigPath)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 3: Init work directory
	wd, err := workdir.Init(provisionWorkDir)
	if err != nil {
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

	// Step 5: Copy cluster.yaml to work-dir
	clusterYAMLPath := filepath.Join(wd.Path, "cluster.yaml")
	clusterYAMLData, err := os.ReadFile(provisionConfigPath)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         fmt.Sprintf("failed to read config file for copying: %v", err),
			RequiresDestroy: false,
		})
		return err
	}
	if err := os.WriteFile(clusterYAMLPath, clusterYAMLData, 0644); err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         fmt.Sprintf("failed to copy cluster.yaml to work-dir: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 6: Resolve AWS credentials
	awsEnv, err := credentials.Resolve(credentials.AWSCredentials{
		AccessKeyID:     cfg.Platform.AWS.Credentials.AccessKeyID,
		SecretAccessKey: cfg.Platform.AWS.Credentials.SecretAccessKey,
		CredentialsFile: cfg.Platform.AWS.Credentials.CredentialsFile,
		Profile:         cfg.Platform.AWS.Credentials.Profile,
		RoleARN:         cfg.Platform.AWS.Credentials.STSRoleARN,
	})
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         fmt.Sprintf("failed to resolve AWS credentials: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 7: Determine release image
	releaseImage := cfg.ReleaseImage
	if releaseImage == "" {
		releaseImage = "quay.io/openshift-release-dev/ocp-release:4.20.18-multi"
	}

	// Step 8: Create Installer instance
	inst := &installer.Installer{
		WorkDir:        wd.Path,
		InstallerPath:  wd.InstallerPath(),
		ReleaseImage:   releaseImage,
		PullSecretFile: cfg.PullSecretFile,
		AWSEnv:         awsEnv,
	}

	logPath := wd.LogPath()
	phases := phase.AllPhases()

	// Step 9: Run phases sequentially
	// Phase 0: extract
	if !wd.IsPhaseComplete(phases[0].Name) {
		if err := phase.RunPhase(phases[0], func() error {
			return inst.Extract(logPath)
		}, os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           phases[0].Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				RequiresDestroy: false,
			})
			return err
		}
		wd.MarkPhaseComplete(phases[0].Name)
	}

	// Phase 1: install-config
	if !wd.IsPhaseComplete(phases[1].Name) {
		if err := phase.RunPhase(phases[1], func() error {
			installConfigData, err := config.GenerateInstallConfig(cfg)
			if err != nil {
				return fmt.Errorf("generate install-config: %w", err)
			}
			return os.WriteFile(wd.InstallConfigPath(), installConfigData, 0644)
		}, os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           phases[1].Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				RequiresDestroy: false,
			})
			return err
		}
		wd.MarkPhaseComplete(phases[1].Name)
	}

	// Phase 2: manifests
	if !wd.IsPhaseComplete(phases[2].Name) {
		if err := phase.RunPhase(phases[2], func() error {
			return inst.CreateManifests(logPath)
		}, os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           phases[2].Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				RequiresDestroy: false,
			})
			return err
		}
		wd.MarkPhaseComplete(phases[2].Name)
	}

	// Phase 3: ignition
	if !wd.IsPhaseComplete(phases[3].Name) {
		if err := phase.RunPhase(phases[3], func() error {
			return inst.CreateIgnitionConfigs(logPath)
		}, os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           phases[3].Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				RequiresDestroy: false,
			})
			return err
		}
		wd.MarkPhaseComplete(phases[3].Name)
	}

	// Phase 4: cluster
	if !wd.IsPhaseComplete(phases[4].Name) {
		if err := phase.RunPhase(phases[4], func() error {
			return inst.CreateCluster(logPath)
		}, os.Stdout); err != nil {
			// Phase 4 failure requires destroy
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           phases[4].Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				HasMetadata:     wd.HasMetadata(),
				RequiresDestroy: true,
			})
			return err
		}
		wd.MarkPhaseComplete(phases[4].Name)
	}

	// Step 10: Get infra ID and output success
	infraID, _ := wd.InfraID()

	output.WritePhaseResult(os.Stdout, output.PhaseResult{
		Phase:  "provision",
		Status: "succeeded",
	})

	// Also output infra_id in a structured way (using error result structure for JSON output)
	fmt.Fprintf(os.Stdout, `{"status":"succeeded","infra_id":"%s"}`+"\n", infraID)

	return nil
}
