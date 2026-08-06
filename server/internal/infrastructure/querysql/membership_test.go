package querysql_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestClassifyResourceContainer_Envelope(t *testing.T) {
	// Envelope classification needs no schema provider or Bind.
	ctx := querysql.ResolveContext{Context: context.Background()}
	tests := []struct {
		name    string
		segs    []string
		want    querysql.ContainerKind
		wantErr bool
	}{
		{name: "labels", segs: []string{"labels"}, want: querysql.ContainerKindObject},
		{name: "label value", segs: []string{"labels", "team"}, want: querysql.ContainerKindScalar},
		{name: "conditions", segs: []string{"conditions"}, want: querysql.ContainerKindObject},
		{name: "condition entry", segs: []string{"conditions", "Ready"}, want: querysql.ContainerKindObject},
		{name: "condition subfield", segs: []string{"conditions", "Ready", "status"}, want: querysql.ContainerKindScalar},
		{name: "spec root", segs: []string{"spec"}, want: querysql.ContainerKindObject},
		{name: "observation open", segs: []string{"observation", "foo"}, want: querysql.ContainerKindUnknown},
		{name: "name scalar", segs: []string{"name"}, want: querysql.ContainerKindScalar},
		{name: "unsupported", segs: []string{"bogus"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ctx.ClassifyResourceContainer(tt.segs)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Kind != tt.want {
				t.Errorf("Kind = %v, want %v", got.Kind, tt.want)
			}
		})
	}
}
