// Package test locks AIO packaging contracts that live outside Go types.
//
// CI tests here compare this repo's Dockerfile.fleetshift with ci-operator
// and Prow YAML in github.com/openshift/release (not this tree). A failure
// is cross-repo drift or a live GitHub fetch error:
//
//   - TestDockerfileFleetshiftOpenShiftRewriteContract: local Dockerfile
//     FROM text vs OpenShift rewrite rules. Patch Dockerfile.fleetshift,
//     then matching from:/inputs.*.as in openshift/release.
//   - TestOpenShiftReleaseAIOImageGraphContract: HTTP GET of
//     openshift/release master. Inspect the GET URL in the failure (network,
//     404, or openshift/release drift).
//
// YAML structs are a trimmed subset of the remote schema; unknown fields
// are dropped, so a renamed remote field looks like a missing image/job.
//
// TestS6ServiceGraph locks the in-image s6 graph and UID split under
// deploy/aio/s6, not OpenShift CI.
package test

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// releaseRawBase is github.com/openshift/release@master as raw GitHub
	// content. These files are not in fleetshift-poc.
	releaseRawBase = "https://raw.githubusercontent.com/openshift/release/master/"
	// ciConfigPath is the ci-operator config that owns the AIO image graph.
	// Generated Prow jobs must stay in sync with this file.
	ciConfigPath = "ci-operator/config/fleetshift/fleetshift-poc/fleetshift-fleetshift-poc-main.yaml"
	// presubmitsPath is the generated PR jobs for that config.
	presubmitsPath = "ci-operator/jobs/fleetshift/fleetshift-poc/fleetshift-fleetshift-poc-main-presubmits.yaml"
	// postsubmitsPath is the generated merge jobs for that config.
	postsubmitsPath = "ci-operator/jobs/fleetshift/fleetshift-poc/fleetshift-fleetshift-poc-main-postsubmits.yaml"

	// webFromArg is the Dockerfile FROM text ci-operator must match in
	// inputs.fleetshift-web.as (exact string, before ARG expansion).
	webFromArg = "${WEB_IMAGE}"
	// serverFromArg is the last Dockerfile FROM. ci-operator from: rewrites
	// that instruction to the pipeline image named in fleetshift.from.
	serverFromArg = "${SERVER_IMAGE}"
)

// TestDockerfileFleetshiftOpenShiftRewriteContract locks FROM names in this
// repo's Dockerfile.fleetshift as OpenShift's builder sees them, before ARG
// expansion.
//
// ci-operator rewrites the last FROM via from: (any text works) and
// intermediate FROMs via inputs.*.as (exact string match). Local
// `nx run image:aio` still passes --build-arg for both ARGs, so the names
// must stay ARG references.
//
// A failure is this repo's Dockerfile. Changing FROM text requires a matching
// openshift/release change to from: / inputs.*.as or the live graph test
// fails next.
func TestDockerfileFleetshiftOpenShiftRewriteContract(t *testing.T) {
	froms := dockerfileFROMImages(t, readDockerfileFleetshift(t))
	if len(froms) < 2 {
		t.Fatalf("Dockerfile.fleetshift: need at least web + server FROM, got %q", froms)
	}
	if !contains(froms, webFromArg) {
		t.Fatalf("Dockerfile.fleetshift: missing FROM %s (ci-operator inputs.fleetshift-web.as must match this text; got %q)", webFromArg, froms)
	}
	if got := froms[len(froms)-1]; got != serverFromArg {
		t.Fatalf("Dockerfile.fleetshift: last FROM must be %s so it stays the runtime base `from:` rewrites; got %q", serverFromArg, got)
	}
}

// TestOpenShiftReleaseAIOImageGraphContract fetches ci-operator
// config and generated jobs from openshift/release@master and checks them
// against this repo's Dockerfile FROM list.
//
// It does not re-check last-FROM / ${SERVER_IMAGE} (that is
// TestDockerfileFleetshiftOpenShiftRewriteContract) and it does not verify
// a git SHA; pipeline image names imply this-SHA builds.
//
// A failure is a GET error (network, 404) or openshift/release drift.
func TestOpenShiftReleaseAIOImageGraphContract(t *testing.T) {
	froms := dockerfileFROMImages(t, readDockerfileFleetshift(t))
	cfg := fetchOpenShiftReleaseYAML[ciOperatorConfig](t, releaseRawBase+ciConfigPath)
	presubmits := fetchOpenShiftReleaseYAML[presubmitsFile](t, releaseRawBase+presubmitsPath)
	postsubmits := fetchOpenShiftReleaseYAML[postsubmitsFile](t, releaseRawBase+postsubmitsPath)
	assertAIOReleaseContract(t, froms, cfg, presubmits, postsubmits)
}

// assertAIOReleaseContract reports every AIO image-graph problem against
// already-parsed YAML. froms is Dockerfile FROM text in order.
//
// Owned by openshift/release ci-operator config unless noted:
//   - images.to includes fleetshift-server, fleetshift-server-local,
//     fleetshift-web, and fleetshift (one pipeline)
//   - those images.dockerfile_path values are Dockerfile, Dockerfile.local,
//     Dockerfile.web, and Dockerfile.fleetshift
//   - fleetshift-server-local.from is fleetshift-server (this-SHA server)
//   - fleetshift.from is fleetshift-server-local (from: rewrites last FROM)
//   - fleetshift.inputs.fleetshift-web.as matches a Dockerfile FROM string
//     (web substitution + wait). Prefer ${WEB_IMAGE}; if that FROM is
//     missing, compare against froms[0] so a Dockerfile edit still fails
//     as an as: mismatch rather than a skipped check
//   - pr-image-mirror-* and pr-merge-image-mirror-* tests set
//     SOURCE_IMAGE_REF and IMAGE_REPO to those pipeline images (no rebuild
//     from Quay, no push to the wrong repo)
//   - pr-merge-* are postsubmit; pr-image-mirror-* are not
//   - generated presubmits ci/prow/images and ci/prow/pr-image-mirror-*
//     always_run on every PR
//   - generated postsubmits branch-ci-…-pr-merge-image-mirror-* always_run
//     on every merge
func assertAIOReleaseContract(t *testing.T, froms []string, cfg ciOperatorConfig, presubmits presubmitsFile, postsubmits postsubmitsFile) {
	t.Helper()
	if len(froms) == 0 {
		t.Error("no FROM instructions")
		return
	}
	webFROM := webFromArg
	if !contains(froms, webFROM) {
		webFROM = froms[0]
	}

	byTo := map[string]ciImage{}
	for _, img := range cfg.Images.Items {
		byTo[img.To] = img
	}
	for _, w := range []struct{ to, path string }{
		{"fleetshift-server", "Dockerfile"},
		{"fleetshift-server-local", "Dockerfile.local"},
		{"fleetshift-web", "Dockerfile.web"},
		{"fleetshift", "Dockerfile.fleetshift"},
	} {
		t.Run("images/"+w.to, func(t *testing.T) {
			img, ok := byTo[w.to]
			if !ok {
				t.Errorf("ci-operator images: missing %s (AIO graph must build all four in one pipeline)", w.to)
				return
			}
			if img.DockerfilePath != w.path {
				t.Errorf("%s dockerfile_path = %q, want %q", w.to, img.DockerfilePath, w.path)
			}
		})
	}
	if img, ok := byTo["fleetshift-server-local"]; ok {
		t.Run("fleetshift-server-local.from", func(t *testing.T) {
			if img.From != "fleetshift-server" {
				t.Errorf("fleetshift-server-local.from = %q, want fleetshift-server", img.From)
			}
		})
	}
	aio, aioOK := byTo["fleetshift"]
	if aioOK {
		t.Run("fleetshift.from", func(t *testing.T) {
			if aio.From != "fleetshift-server-local" {
				t.Errorf("fleetshift.from = %q, want fleetshift-server-local (`from:` rewrites last FROM)", aio.From)
			}
		})
		t.Run("fleetshift.inputs", func(t *testing.T) {
			webIn, ok := aio.Inputs["fleetshift-web"]
			if !ok {
				t.Error("fleetshift.inputs: missing fleetshift-web (AIO would not wait for web, and would not substitute it)")
				return
			}
			if !contains(webIn.As, webFROM) {
				t.Errorf("fleetshift.inputs.fleetshift-web.as = %q, want %q (must match Dockerfile FROM text before ARG expansion)", webIn.As, webFROM)
			}
		})
	}

	tests := map[string]ciTest{}
	for _, test := range cfg.Tests {
		tests[test.As] = test
	}
	for _, w := range []struct{ name, image string }{
		{"pr-image-mirror-server", "fleetshift-server"},
		{"pr-image-mirror-local", "fleetshift-server-local"},
		{"pr-image-mirror-web", "fleetshift-web"},
		{"pr-image-mirror-aio", "fleetshift"},
		{"pr-merge-image-mirror-server", "fleetshift-server"},
		{"pr-merge-image-mirror-local", "fleetshift-server-local"},
		{"pr-merge-image-mirror-web", "fleetshift-web"},
		{"pr-merge-image-mirror-aio", "fleetshift"},
	} {
		t.Run("tests/"+w.name, func(t *testing.T) {
			test, ok := tests[w.name]
			if !ok {
				t.Errorf("ci-operator tests: missing %s", w.name)
				return
			}
			if test.Steps == nil || test.Steps.Dependencies["SOURCE_IMAGE_REF"] != w.image {
				t.Errorf("%s SOURCE_IMAGE_REF: want %s (mirror must reuse the pipeline image, not rebuild from Quay)", w.name, w.image)
			}
			if test.Steps == nil || test.Steps.Env["IMAGE_REPO"] != w.image {
				t.Errorf("%s IMAGE_REPO: want %s (mirror must push the pipeline image to the matching repo)", w.name, w.image)
			}
			wantPost := strings.HasPrefix(w.name, "pr-merge-")
			if test.Postsubmit != wantPost {
				t.Errorf("%s postsubmit=%v, want %v", w.name, test.Postsubmit, wantPost)
			}
		})
	}

	jobs := presubmits.Presubmits["fleetshift/fleetshift-poc"]
	for _, ctx := range []string{
		"ci/prow/images",
		"ci/prow/pr-image-mirror-server",
		"ci/prow/pr-image-mirror-local",
		"ci/prow/pr-image-mirror-web",
		"ci/prow/pr-image-mirror-aio",
	} {
		t.Run("presubmits/"+ctx, func(t *testing.T) {
			job := findProwJobByContext(jobs, ctx)
			if job == nil {
				t.Errorf("presubmits: missing %s", ctx)
				return
			}
			if !job.AlwaysRun {
				t.Errorf("%s always_run=false; AIO/product images must build on every PR", ctx)
			}
		})
	}

	post := postsubmits.Postsubmits["fleetshift/fleetshift-poc"]
	for _, name := range []string{
		"branch-ci-fleetshift-fleetshift-poc-main-pr-merge-image-mirror-server",
		"branch-ci-fleetshift-fleetshift-poc-main-pr-merge-image-mirror-local",
		"branch-ci-fleetshift-fleetshift-poc-main-pr-merge-image-mirror-web",
		"branch-ci-fleetshift-fleetshift-poc-main-pr-merge-image-mirror-aio",
	} {
		t.Run("postsubmits/"+name, func(t *testing.T) {
			job := findProwJobByName(post, name)
			if job == nil {
				t.Errorf("postsubmits: missing %s", name)
				return
			}
			if !job.AlwaysRun {
				t.Errorf("%s always_run=false; AIO/product images must build on every merge", name)
			}
		})
	}
}

// dockerfileFROMImages returns FROM image tokens in order, skipping blanks
// and comment lines. The token is the pre-ARG-expansion text OpenShift
// matches against inputs.*.as.
func dockerfileFROMImages(t *testing.T, dockerfile string) []string {
	t.Helper()
	var froms []string
	for line := range strings.SplitSeq(dockerfile, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.EqualFold(fields[0], "FROM") {
			froms = append(froms, fields[1])
		}
	}
	return froms
}

// readDockerfileFleetshift reads repo-root Dockerfile.fleetshift (the AIO
// assembly file), resolving the root by walking from the test cwd.
func readDockerfileFleetshift(t *testing.T) string {
	t.Helper()
	root := findRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "Dockerfile.fleetshift"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// fetchOpenShiftReleaseYAML GETs url (raw.githubusercontent.com
// openshift/release) and unmarshals it. Timeout is 20s. Non-200 is fatal
// with status and body; that is a fetch failure, not a graph regression.
func fetchOpenShiftReleaseYAML[T any](t *testing.T, url string) T {
	t.Helper()
	client := &http.Client{Timeout: 20 * time.Second}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "fleetshift-poc-aio-ci-contract-test")
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %s\n%s", url, res.Status, body)
	}
	return mustYAML[T](t, string(body))
}

// mustYAML unmarshals raw YAML or fatals. Extra fields are ignored.
func mustYAML[T any](t *testing.T, raw string) T {
	t.Helper()
	var out T
	if err := yaml.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	return out
}

// findProwJobByContext returns the presubmit whose context: matches, or nil.
func findProwJobByContext(jobs []prowJob, context string) *prowJob {
	for i := range jobs {
		if jobs[i].Context == context {
			return &jobs[i]
		}
	}
	return nil
}

// findProwJobByName returns the postsubmit whose name: matches, or nil.
func findProwJobByName(jobs []prowJob, name string) *prowJob {
	for i := range jobs {
		if jobs[i].Name == name {
			return &jobs[i]
		}
	}
	return nil
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// ciOperatorConfig is the subset of openshift/release ci-operator config
// used for the AIO graph. A renamed remote field unmarshals empty and
// fails as a missing image or test.
type ciOperatorConfig struct {
	Images struct {
		Items []ciImage `yaml:"items"`
	} `yaml:"images"`
	Tests []ciTest `yaml:"tests"`
}

// ciImage is one images.items entry (to, dockerfile_path, optional from,
// optional inputs).
type ciImage struct {
	To             string             `yaml:"to"`
	From           string             `yaml:"from"`
	DockerfilePath string             `yaml:"dockerfile_path"`
	Inputs         map[string]ciInput `yaml:"inputs"`
}

// ciInput is inputs.<pipeline-image>.as: Dockerfile FROM strings to rewrite.
type ciInput struct {
	As []string `yaml:"as"`
}

// ciTest is one tests[] entry: name, postsubmit bit, and step dependencies
// and env (SOURCE_IMAGE_REF and IMAGE_REPO for image-mirror tests).
type ciTest struct {
	As         string `yaml:"as"`
	Postsubmit bool   `yaml:"postsubmit"`
	Steps      *struct {
		Dependencies map[string]string `yaml:"dependencies"`
		Env          map[string]string `yaml:"env"`
	} `yaml:"steps"`
}

// presubmitsFile is the generated Prow presubmits YAML keyed by repo.
type presubmitsFile struct {
	Presubmits map[string][]prowJob `yaml:"presubmits"`
}

// postsubmitsFile is the generated Prow postsubmits YAML keyed by repo.
type postsubmitsFile struct {
	Postsubmits map[string][]prowJob `yaml:"postsubmits"`
}

// prowJob is always_run plus context (presubmits) or name (postsubmits).
type prowJob struct {
	AlwaysRun bool   `yaml:"always_run"`
	Context   string `yaml:"context"`
	Name      string `yaml:"name"`
}
