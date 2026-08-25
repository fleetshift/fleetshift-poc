package steps

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestRunStep_Runs(t *testing.T) {
	t.Parallel()
	var ran bool
	RunStep(t, "do", func(t *testing.T) {
		ran = true
	})
	if !ran {
		t.Fatal("step did not run")
	}
}

func TestRunStep_SkipsAfterFailure(t *testing.T) {
	if os.Getenv("TEST_RUNSTEP_INNER") == "1" {
		RunStep(t, "fail", func(t *testing.T) {
			t.Error("boom")
		})
		RunStep(t, "later", func(t *testing.T) {
			t.Error("later ran")
		})
		return
	}

	t.Parallel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRunStep_SkipsAfterFailure$", "-test.v=true")
	cmd.Env = append(os.Environ(), "TEST_RUNSTEP_INNER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("inner test should fail:\n%s", out)
	}
	got := string(out)
	if !strings.Contains(got, "earlier step failed") {
		t.Fatalf("later step was not skipped:\n%s", got)
	}
	if strings.Contains(got, "later ran") {
		t.Fatalf("later step fn ran:\n%s", got)
	}
}
