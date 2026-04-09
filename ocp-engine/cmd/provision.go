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

	// Step 9: Build phase function map and run phases sequentially
	phaseFns := map[string]func() error{
		"extract": func() error {
			return inst.Extract(logPath)
		},
		"install-config": func() error {
			installConfigData, err := config.GenerateInstallConfig(cfg)
			if err != nil {
				return fmt.Errorf("generate install-config: %w", err)
			}
			return os.WriteFile(wd.InstallConfigPath(), installConfigData, 0600)
		},
		"manifests": func() error {
			return inst.CreateManifests(logPath)
		},
		"ignition": func() error {
			return inst.CreateIgnitionConfigs(logPath)
		},
		"cluster": func() error {
			return inst.CreateCluster(logPath)
		},
	}

	for _, p := range phases {
		if wd.IsPhaseComplete(p.Name) {
			continue
		}
		fn := phaseFns[p.Name]
		if err := phase.RunPhase(p, fn, os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           p.Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				HasMetadata:     wd.HasMetadata(),
				RequiresDestroy: p.RequiresDestroyOnFailure,
			})
			return err
		}
		wd.MarkPhaseComplete(p.Name)
	}

	// Step 10: Output success
	infraID, _ := wd.InfraID()

	output.WriteProvisionResult(os.Stdout, output.ProvisionResult{
		Status:  "succeeded",
		InfraID: infraID,
	})

	return nil
}
