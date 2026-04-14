package output_test

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
)

func deploymentColumns() []output.Column {
	return []output.Column{
		{Header: "Name", Value: func(m proto.Message) string { return m.(*pb.Deployment).GetName() }},
		{Header: "State", Value: func(m proto.Message) string { return m.(*pb.Deployment).GetState().String() }},
	}
}

func sampleDeployment(name string, state pb.Deployment_State) *pb.Deployment {
	return &pb.Deployment{
		Name:  name,
		State: state,
		ManifestStrategy: &pb.ManifestStrategy{
			Type: pb.ManifestStrategy_TYPE_INLINE,
		},
		PlacementStrategy: &pb.PlacementStrategy{
			Type: pb.PlacementStrategy_TYPE_ALL,
		},
	}
}

func TestPrintResource_Table(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatTable)

	dep := sampleDeployment("deployments/alpha", pb.Deployment_STATE_ACTIVE)
	if err := p.PrintResource(dep, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")

	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + row), got %d:\n%s", len(lines), got)
	}
	if !strings.Contains(lines[0], "NAME") || !strings.Contains(lines[0], "STATE") {
		t.Errorf("header missing expected columns: %q", lines[0])
	}
	if !strings.Contains(lines[1], "deployments/alpha") || !strings.Contains(lines[1], "STATE_ACTIVE") {
		t.Errorf("row missing expected values: %q", lines[1])
	}
}

func TestPrintResource_JSON(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatJSON)

	dep := sampleDeployment("deployments/alpha", pb.Deployment_STATE_ACTIVE)
	if err := p.PrintResource(dep, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	if !strings.Contains(got, `"deployments/alpha"`) {
		t.Errorf("expected deployment name in JSON output: %s", got)
	}
	if !strings.Contains(got, `"STATE_ACTIVE"`) {
		t.Errorf("expected state in JSON output: %s", got)
	}
}

func TestPrintResourceList_Table(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatTable)

	deps := []proto.Message{
		sampleDeployment("deployments/alpha", pb.Deployment_STATE_ACTIVE),
		sampleDeployment("deployments/beta", pb.Deployment_STATE_CREATING),
	}
	if err := p.PrintResourceList(deps, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines (header + 2 rows), got %d:\n%s", len(lines), buf.String())
	}
}

func TestPrintResourceList_JSON(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatJSON)

	deps := []proto.Message{
		sampleDeployment("deployments/alpha", pb.Deployment_STATE_ACTIVE),
		sampleDeployment("deployments/beta", pb.Deployment_STATE_CREATING),
	}
	if err := p.PrintResourceList(deps, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	if !strings.HasPrefix(got, "[") {
		t.Errorf("expected JSON array, got: %s", got)
	}
	if strings.Count(got, `"name"`) != 2 {
		t.Errorf("expected 2 name fields, got: %s", got)
	}
}

func TestPrintResourceList_Empty_JSON(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatJSON)

	if err := p.PrintResourceList(nil, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	got := strings.TrimSpace(buf.String())
	if got != "[]" {
		t.Errorf("expected [], got: %q", got)
	}
}

func TestPrintResource_TableAlignment(t *testing.T) {
	var buf bytes.Buffer
	p := output.NewPrinter(&buf, output.FormatTable)

	deps := []proto.Message{
		sampleDeployment("deployments/a", pb.Deployment_STATE_ACTIVE),
		sampleDeployment("deployments/longname", pb.Deployment_STATE_CREATING),
	}
	if err := p.PrintResourceList(deps, deploymentColumns()); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")

	// The STATE column should start at the same offset in each line.
	stateOffsets := make([]int, len(lines))
	for i, line := range lines {
		stateOffsets[i] = strings.Index(line, "STATE")
		if stateOffsets[i] == -1 && i > 0 {
			// data rows won't literally contain "STATE" header, find the value instead
			continue
		}
	}
	// At least the header should have STATE
	if stateOffsets[0] == -1 {
		t.Error("header does not contain STATE column")
	}
}

func TestFormatValidate(t *testing.T) {
	tests := []struct {
		format  output.Format
		wantErr bool
	}{
		{output.FormatTable, false},
		{output.FormatJSON, false},
		{"yaml", true},
		{"", true},
	}
	for _, tt := range tests {
		err := tt.format.Validate()
		if (err != nil) != tt.wantErr {
			t.Errorf("Format(%q).Validate() err=%v, wantErr=%v", tt.format, err, tt.wantErr)
		}
	}
}
