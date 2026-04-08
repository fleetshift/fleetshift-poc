package installer

import (
	"fmt"
	"os"
	"os/exec"
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
