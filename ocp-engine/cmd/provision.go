package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ocp-engine/internal/artifacts"
	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/logpipeline"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/preflight"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var provisionCmd = &cobra.Command{
	Use:   "provision",
	Short: "Provision a new OpenShift cluster",
	Long:  "Executes the complete cluster provisioning workflow through all phases (extract, install-config, manifests, ignition, cluster). The directory containing the config file is used as the work directory.",
	RunE:  runProvision,
}

var provisionConfigPath string
var provisionAttempt int
var provisionTimeout time.Duration

func init() {
	provisionCmd.Flags().StringVar(&provisionConfigPath, "config", "", "Path to cluster.yaml (required). Parent directory is used as work directory.")
	provisionCmd.MarkFlagRequired("config")
	provisionCmd.Flags().IntVar(&provisionAttempt, "attempt", 1, "Attempt number for retry tracking (metadata only, no engine behavior change)")
	provisionCmd.Flags().DurationVar(&provisionTimeout, "timeout", 3*time.Hour, "Total timeout for all provision phases")
	rootCmd.AddCommand(provisionCmd)
}

func runProvision(cmd *cobra.Command, args []string) error {
	provisionStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
	defer cancel()
	var recoveryAttempted bool
	var pipeline *logpipeline.Pipeline

	cfg, err := config.LoadConfig(provisionConfigPath)
	if err != nil {
		return output.WriteError(os.Stdout, "config_error", err, false)
	}

	wd, err := workdir.Open(filepath.Dir(provisionConfigPath))
	if err != nil {
		return output.WriteError(os.Stdout, "workdir_error", err, false)
	}

	if err := wd.Lock(); err != nil {
		return output.WriteError(os.Stdout, "already_running", err, false)
	}
	defer wd.Unlock()

	// Copy cluster.yaml to work-dir so destroy can find it later
	if err := wd.CopyConfig(provisionConfigPath); err != nil {
		return output.WriteError(os.Stdout, "workdir_error", err, false)
	}

	// Run preflight checks (validates config, files, credentials, DNS)
	awsEnv, err := preflight.RunPreflight(cfg, os.Stdout, provisionAttempt)
	if err != nil {
		return output.WriteError(os.Stdout, "prereq_error", err, false)
	}
	wd.MarkPhaseComplete("preflight")

	releaseImage := cfg.Engine.ReleaseImage
	if releaseImage == "" {
		releaseImage = "quay.io/openshift-release-dev/ocp-release:4.20.18-multi"
	}

	inst := &installer.Installer{
		WorkDir:        wd.Path,
		InstallerPath:  wd.InstallerPath(),
		ReleaseImage:   releaseImage,
		PullSecretFile: cfg.Engine.PullSecretFile,
		AWSEnv:         awsEnv,
	}

	logPath := wd.LogPath()

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
			if err := wd.BackupInstallConfig(); err != nil {
				return fmt.Errorf("backup install-config: %w", err)
			}
			return inst.CreateManifests(logPath)
		},
		"ignition": func() error {
			return inst.CreateIgnitionConfigs(logPath)
		},
		"cluster": func() error {
			pipeline = logpipeline.NewPipeline(logPath, os.Stdout, os.Stderr, provisionAttempt)
			pipeline.Start()
			defer pipeline.Stop()
			return inst.CreateClusterWithContext(ctx, logPath)
		},
	}

	for _, p := range phase.AllPhases() {
		if p.Name == "preflight" {
			continue // already ran above
		}
		if wd.IsPhaseComplete(p.Name) {
			continue
		}
		fn, ok := phaseFns[p.Name]
		if !ok {
			continue
		}
		if err := phase.RunPhase(p, fn, os.Stdout, provisionAttempt); err != nil {
			// For cluster phase: attempt bootstrap-aware recovery
			if p.Name == "cluster" && pipeline != nil && pipeline.BootstrapComplete() {
				fmt.Fprintln(os.Stderr, "Bootstrap complete — attempting recovery with wait-for install-complete")
				recoveryErr := inst.WaitForInstallComplete(ctx, logPath)
				if recoveryErr == nil {
					// Recovery succeeded — mark phase complete and continue
					wd.MarkPhaseComplete(p.Name)
					recoveryAttempted = true
					continue
				}
				fmt.Fprintf(os.Stderr, "Recovery failed: %v\n", recoveryErr)
			}

			errResult := output.ErrorResult{
				Category:          "phase_error",
				Phase:             p.Name,
				Message:           err.Error(),
				LogTail:           wd.LogTail(50),
				HasMetadata:       wd.HasMetadata(),
				RequiresDestroy:   p.RequiresDestroyOnFailure,
				RecoveryAttempted: pipeline != nil && pipeline.BootstrapComplete(),
				Attempt:           provisionAttempt,
			}
			// For cluster phase, parse failure reason from logs
			if p.Name == "cluster" {
				fullLog := readFullLog(logPath)
				errResult.FailureReason, errResult.FailureMessage = logpipeline.ParseFailureReason(fullLog)
			}
			output.WriteErrorResult(os.Stdout, errResult)
			return err
		}
		wd.MarkPhaseComplete(p.Name)
	}

	// Post-install artifact validation
	artifactResult, err := artifacts.Validate(wd.Path)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "artifact_error",
			Message:         fmt.Sprintf("install succeeded but artifact validation failed: %s", err),
			RequiresDestroy: true,
			Attempt:         provisionAttempt,
		})
		return err
	}

	elapsed := int(time.Since(provisionStart).Seconds())
	output.WriteProvisionResult(os.Stdout, output.ProvisionResult{
		Status:            "succeeded",
		InfraID:           artifactResult.InfraID,
		ClusterID:         artifactResult.ClusterID,
		HasKubeconfig:     artifactResult.HasKubeconfig,
		RecoveryAttempted: recoveryAttempted,
		ElapsedSeconds:    elapsed,
		Attempt:           provisionAttempt,
	})

	return nil
}

func readFullLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}
