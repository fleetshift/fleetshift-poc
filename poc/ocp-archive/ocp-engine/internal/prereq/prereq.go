package prereq

import (
	"fmt"
	"os/exec"
)

func CheckBinary(name string) error {
	_, err := exec.LookPath(name)
	if err != nil {
		return fmt.Errorf("%s not found in PATH", name)
	}
	return nil
}

func CheckContainerRuntime() error {
	if err := CheckBinary("podman"); err == nil {
		return nil
	}
	if err := CheckBinary("docker"); err == nil {
		return nil
	}
	return fmt.Errorf("neither podman nor docker found in PATH")
}

func Validate() error {
	if err := CheckBinary("oc"); err != nil {
		return fmt.Errorf("prerequisite check failed: oc: %w", err)
	}
	if err := CheckContainerRuntime(); err != nil {
		return fmt.Errorf("prerequisite check failed: container-runtime: %w", err)
	}
	return nil
}
