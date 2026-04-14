package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ocp-engine/internal/artifacts"
	"github.com/ocp-engine/internal/callback"
	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/logpipeline"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/preflight"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
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
	provisionCmd.Flags().DurationVar(&provisionTimeout, "timeout", 2*time.Hour, "Total timeout for all provision phases")
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

	// Create callback client (nil when --callback-url is not set)
	cb, err := newCallbackClient()
	if err != nil {
		return output.WriteError(os.Stdout, "callback_error", err, false)
	}
	if cb != nil {
		defer cb.Close()
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
		phaseStart := time.Now()
		if err := phase.RunPhase(p, fn, os.Stdout, provisionAttempt); err != nil {
			phaseElapsed := int32(time.Since(phaseStart).Seconds())

			// For cluster phase: attempt bootstrap-aware recovery
			if p.Name == "cluster" && pipeline != nil && pipeline.BootstrapComplete() {
				fmt.Fprintln(os.Stderr, "Bootstrap complete — attempting recovery with wait-for install-complete")
				recoveryErr := inst.WaitForInstallComplete(ctx, logPath)
				if recoveryErr == nil {
					// Recovery succeeded — mark phase complete and continue
					wd.MarkPhaseComplete(p.Name)
					recoveryAttempted = true
					reportPhaseResult(cb, ctx, p.Name, "complete", phaseElapsed, "", int32(provisionAttempt))
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

			// Report phase failure and terminal failure via callback
			reportPhaseResult(cb, ctx, p.Name, "failed", phaseElapsed, err.Error(), int32(provisionAttempt))
			reportFailure(cb, ctx, callback.FailureData{
				Phase:             p.Name,
				FailureReason:     errResult.FailureReason,
				FailureMessage:    errResult.FailureMessage,
				LogTail:           errResult.LogTail,
				RequiresDestroy:   p.RequiresDestroyOnFailure,
				RecoveryAttempted: pipeline != nil && pipeline.BootstrapComplete(),
				Attempt:           int32(provisionAttempt),
			})
			return err
		}
		wd.MarkPhaseComplete(p.Name)
		reportPhaseResult(cb, ctx, p.Name, "complete", int32(time.Since(phaseStart).Seconds()), "", int32(provisionAttempt))
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
		reportFailure(cb, ctx, callback.FailureData{
			Phase:           "validation",
			FailureReason:   "artifact_validation_failed",
			FailureMessage:  err.Error(),
			RequiresDestroy: true,
			Attempt:         int32(provisionAttempt),
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

	// Report completion via callback with artifact data
	if cb != nil {
		region := extractRegion(cfg)
		apiServer, caCert := extractKubeconfigData(filepath.Join(wd.Path, "auth", "kubeconfig"))
		kubeconfig, _ := os.ReadFile(filepath.Join(wd.Path, "auth", "kubeconfig"))
		metadataJSON, _ := os.ReadFile(filepath.Join(wd.Path, "metadata.json"))
		sshPrivKey, _ := os.ReadFile(filepath.Join(wd.Path, "auth", "ssh-privatekey"))
		sshPubKey, _ := readSSHPublicKey(cfg)

		reportCompletion(cb, ctx, callback.CompletionData{
			InfraID:           artifactResult.InfraID,
			ClusterUUID:       artifactResult.ClusterID,
			APIServer:         apiServer,
			Region:            region,
			Kubeconfig:        kubeconfig,
			CACert:            caCert,
			SSHPrivateKey:     sshPrivKey,
			SSHPublicKey:      sshPubKey,
			MetadataJSON:      metadataJSON,
			RecoveryAttempted: recoveryAttempted,
			ElapsedSeconds:    int32(elapsed),
			Attempt:           int32(provisionAttempt),
		})
	}

	return nil
}

func readFullLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

// extractRegion pulls the AWS region from the parsed cluster config.
func extractRegion(cfg *config.ClusterConfig) string {
	platform, ok := cfg.InstallConfig["platform"].(map[string]any)
	if !ok {
		return ""
	}
	aws, ok := platform["aws"].(map[string]any)
	if !ok {
		return ""
	}
	region, _ := aws["region"].(string)
	return region
}

// extractKubeconfigData reads a kubeconfig file and extracts the API server
// URL and CA certificate (PEM bytes). Returns empty values on any error.
func extractKubeconfigData(kubeconfigPath string) (apiServer string, caCert []byte) {
	data, err := os.ReadFile(kubeconfigPath)
	if err != nil {
		return "", nil
	}

	// Parse kubeconfig YAML to extract cluster server and CA data
	var kc struct {
		Clusters []struct {
			Cluster struct {
				Server                   string `yaml:"server"`
				CertificateAuthorityData string `yaml:"certificate-authority-data"`
			} `yaml:"cluster"`
		} `yaml:"clusters"`
	}

	if err := yaml.Unmarshal(data, &kc); err != nil {
		return "", nil
	}

	if len(kc.Clusters) == 0 {
		return "", nil
	}

	apiServer = kc.Clusters[0].Cluster.Server

	if kc.Clusters[0].Cluster.CertificateAuthorityData != "" {
		decoded, err := base64.StdEncoding.DecodeString(kc.Clusters[0].Cluster.CertificateAuthorityData)
		if err == nil {
			caCert = decoded
		}
	}

	return apiServer, caCert
}

// readSSHPublicKey reads the SSH public key from the config's referenced file.
func readSSHPublicKey(cfg *config.ClusterConfig) ([]byte, error) {
	if cfg.Engine.SSHPublicKeyFile == "" {
		return nil, nil
	}
	return os.ReadFile(cfg.Engine.SSHPublicKeyFile)
}

// reportPhaseResult is a nil-safe wrapper that reports a phase result via callback.
func reportPhaseResult(cb *callback.Client, ctx context.Context, phaseName, status string, elapsed int32, errMsg string, attempt int32) {
	if cb == nil {
		return
	}
	if err := cb.ReportPhaseResult(ctx, phaseName, status, elapsed, errMsg, attempt); err != nil {
		fmt.Fprintf(os.Stderr, "callback: ReportPhaseResult(%s): %v\n", phaseName, err)
	}
}

// reportFailure is a nil-safe wrapper that reports a terminal failure via callback.
func reportFailure(cb *callback.Client, ctx context.Context, data callback.FailureData) {
	if cb == nil {
		return
	}
	if err := cb.ReportFailure(ctx, data); err != nil {
		fmt.Fprintf(os.Stderr, "callback: ReportFailure: %v\n", err)
	}
}

// reportCompletion is a nil-safe wrapper that reports completion via callback.
func reportCompletion(cb *callback.Client, ctx context.Context, data callback.CompletionData) {
	if cb == nil {
		return
	}
	if err := cb.ReportCompletion(ctx, data); err != nil {
		fmt.Fprintf(os.Stderr, "callback: ReportCompletion: %v\n", err)
	}
}
