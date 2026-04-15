package installer

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type Installer struct {
	WorkDir        string
	InstallerPath  string
	ReleaseImage   string
	PullSecretFile string
	AWSEnv         map[string]string
}

func (i *Installer) buildExtractArgs() []string {
	args := []string{
		"adm", "release", "extract",
		"--command=openshift-install",
		"--to=" + i.WorkDir,
	}
	if i.PullSecretFile != "" {
		args = append(args, "--registry-config="+i.PullSecretFile)
	}
	args = append(args, i.ReleaseImage)
	return args
}

func (i *Installer) buildInstallerArgs(subcommand ...string) []string {
	args := append([]string{}, subcommand...)
	args = append(args, "--dir="+i.WorkDir)
	return args
}

func (i *Installer) buildEnv() []string {
	env := os.Environ()
	for k, v := range i.AWSEnv {
		env = append(env, k+"="+v)
	}
	return env
}

// BuildEnv returns the installer's environment variables for use by
// external commands that need the same AWS credentials.
func (i *Installer) BuildEnv() []string {
	return i.buildEnv()
}

func (i *Installer) Extract(logPath string) error {
	return RunCommand("oc", i.buildExtractArgs(), i.buildEnv(), logPath)
}

func (i *Installer) CreateManifests(logPath string) error {
	return RunCommand(i.InstallerPath, i.buildInstallerArgs("create", "manifests"), i.buildEnv(), logPath)
}

func (i *Installer) CreateIgnitionConfigs(logPath string) error {
	return RunCommand(i.InstallerPath, i.buildInstallerArgs("create", "ignition-configs"), i.buildEnv(), logPath)
}

func (i *Installer) CreateCluster(logPath string) error {
	return RunCommand(i.InstallerPath, i.buildInstallerArgs("create", "cluster"), i.buildEnv(), logPath)
}

// CreateClusterQuiet runs create cluster writing only to the log file.
// Use when a log pipeline is handling stderr output.
func (i *Installer) CreateClusterQuiet(logPath string) error {
	return RunCommandQuiet(i.InstallerPath, i.buildInstallerArgs("create", "cluster"), i.buildEnv(), logPath)
}

func (i *Installer) DestroyCluster(logPath string) error {
	return RunCommand(i.InstallerPath, i.buildInstallerArgs("destroy", "cluster"), i.buildEnv(), logPath)
}

func RunCommand(binary string, args []string, env []string, logPath string) error {
	cmd := exec.Command(binary, args...)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer logFile.Close()

	// Stream output to both the log file and stderr so the user sees progress
	cmd.Stdout = io.MultiWriter(logFile, os.Stderr)
	cmd.Stderr = io.MultiWriter(logFile, os.Stderr)

	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command %s failed: %w", binary, err)
	}
	return nil
}

// RunCommandQuiet runs a command writing output only to the log file, not stderr.
// Use this when a log pipeline is handling stderr output separately.
func RunCommandQuiet(binary string, args []string, env []string, logPath string) error {
	cmd := exec.Command(binary, args...)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command %s failed: %w", binary, err)
	}
	return nil
}

// RunCommandWithContext runs a command with context-based timeout support.
// On context cancellation, sends SIGTERM, waits 30s, then SIGKILL.
func RunCommandWithContext(ctx context.Context, binary string, args []string, env []string, logPath string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if env != nil {
		cmd.Env = env
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", binary, err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("command %s failed: %w", binary, err)
		}
		return nil
	case <-ctx.Done():
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		return fmt.Errorf("command %s killed: deadline exceeded", binary)
	}
}

func (i *Installer) CreateClusterWithContext(ctx context.Context, logPath string) error {
	return RunCommandWithContext(ctx, i.InstallerPath, i.buildInstallerArgs("create", "cluster"), i.buildEnv(), logPath)
}

func (i *Installer) DestroyClusterWithContext(ctx context.Context, logPath string) error {
	return RunCommandWithContext(ctx, i.InstallerPath, i.buildInstallerArgs("destroy", "cluster"), i.buildEnv(), logPath)
}

func (i *Installer) WaitForInstallComplete(ctx context.Context, logPath string) error {
	return RunCommandWithContext(ctx, i.InstallerPath, i.buildInstallerArgs("wait-for", "install-complete"), i.buildEnv(), logPath)
}
