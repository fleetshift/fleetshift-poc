package prereq

import (
	"fmt"
	"os/exec"
)

type CheckResult struct {
	Name    string
	Passed  bool
	Message string
}

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

func CheckAll() []CheckResult {
	var results []CheckResult
	ocErr := CheckBinary("oc")
	results = append(results, CheckResult{
		Name:    "oc",
		Passed:  ocErr == nil,
		Message: errMsg(ocErr, "oc CLI found"),
	})
	rtErr := CheckContainerRuntime()
	results = append(results, CheckResult{
		Name:    "container-runtime",
		Passed:  rtErr == nil,
		Message: errMsg(rtErr, "container runtime found"),
	})
	return results
}

func errMsg(err error, successMsg string) string {
	if err != nil {
		return err.Error()
	}
	return successMsg
}

func Validate() error {
	results := CheckAll()
	for _, r := range results {
		if !r.Passed {
			return fmt.Errorf("prerequisite check failed: %s: %s", r.Name, r.Message)
		}
	}
	return nil
}
