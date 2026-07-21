package querysql_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// membershipRecorder records presence and JSON-membership calls so
// orchestration tests can assert which dialect hooks ran.
type membershipRecorder struct {
	presencePaths []string
	jsonTargets   []querysql.JSONMembershipTarget
	jsonKeys      []string
	presenceErr   error
	jsonErr       error
}

func (r *membershipRecorder) Resolve(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
	return querysql.SQLExpr{SQL: path.String()}, nil
}

func (r *membershipRecorder) ResolvePresence(path querysql.FieldPath, _ querysql.ResolveContext) (string, error) {
	r.presencePaths = append(r.presencePaths, path.String())
	if r.presenceErr != nil {
		return "", r.presenceErr
	}
	return "HAS(" + path.String() + ")", nil
}

func (r *membershipRecorder) ResolveJSONMembership(target querysql.JSONMembershipTarget, key string, _ querysql.ResolveContext) (string, error) {
	r.jsonTargets = append(r.jsonTargets, target)
	r.jsonKeys = append(r.jsonKeys, key)
	if r.jsonErr != nil {
		return "", r.jsonErr
	}
	return fmt.Sprintf("JSON_MEM(%d,%v,%q)", target.Root, target.Path, key), nil
}

func TestCompileFilter_MembershipOrchestration(t *testing.T) {
	desc := nestedSpecDescriptor(t)
	schemas := staticQuerySchemas{
		testRT: {
			ResourceType:                   testRT,
			SpecDescriptor:                 desc,
			InventoryObservationDescriptor: desc,
		},
	}
	guard := `resourceType == "kind.fleetshift.io/Cluster" && `

	t.Run("resource root delegates to presence with key", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `"spec" in resource`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if pred.SQL != "HAS(resource.spec)" {
			t.Errorf("SQL = %q, want HAS(resource.spec)", pred.SQL)
		}
		if len(rec.presencePaths) != 1 || rec.presencePaths[0] != "resource.spec" {
			t.Errorf("presence = %v, want [resource.spec]", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 0 {
			t.Errorf("json membership called: %v", rec.jsonTargets)
		}
	})

	t.Run("known object delegates to presence with key appended", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `"value" in resource.observation.nested`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if !strings.Contains(pred.SQL, "HAS(resource.observation.nested.value)") {
			t.Errorf("SQL = %q, want presence of nested.value", pred.SQL)
		}
		if len(rec.presencePaths) != 1 || rec.presencePaths[0] != "resource.observation.nested.value" {
			t.Errorf("presence = %v, want [resource.observation.nested.value]", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 0 {
			t.Errorf("json membership called: %v", rec.jsonTargets)
		}
	})

	t.Run("envelope object delegates to presence", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `"team" in resource.labels`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if len(rec.presencePaths) != 1 || rec.presencePaths[0] != "resource.labels.team" {
			t.Errorf("presence = %v, want [resource.labels.team]", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 0 {
			t.Errorf("json membership called: %v", rec.jsonTargets)
		}
	})

	t.Run("list invokes ResolveJSONMembership", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `"k" in resource.spec.tags`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if len(rec.presencePaths) != 0 {
			t.Errorf("presence called: %v", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 1 {
			t.Fatalf("json targets = %v, want 1", rec.jsonTargets)
		}
		got := rec.jsonTargets[0]
		if got.Root != querysql.JSONMembershipRootSpec {
			t.Errorf("Root = %v, want Spec", got.Root)
		}
		if fmt.Sprint(got.Path) != "[tags]" {
			t.Errorf("Path = %v, want [tags]", got.Path)
		}
		if got.Kind != querysql.ContainerKindList {
			t.Errorf("Kind = %v, want List", got.Kind)
		}
		if rec.jsonKeys[0] != "k" {
			t.Errorf("key = %q, want k", rec.jsonKeys[0])
		}
	})

	t.Run("unknown open JSON invokes ResolveJSONMembership", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `"k" in resource.observation.metadata.foo`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if len(rec.presencePaths) != 0 {
			t.Errorf("presence called: %v", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 1 {
			t.Fatalf("json targets = %v, want 1", rec.jsonTargets)
		}
		got := rec.jsonTargets[0]
		if got.Root != querysql.JSONMembershipRootObservation {
			t.Errorf("Root = %v, want Observation", got.Root)
		}
		if fmt.Sprint(got.Path) != "[metadata foo]" {
			t.Errorf("Path = %v, want [metadata foo]", got.Path)
		}
		if got.Kind != querysql.ContainerKindUnknown {
			t.Errorf("Kind = %v, want Unknown", got.Kind)
		}
	})

	t.Run("scalar fails without presence or json membership", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `"k" in resource.observation.nested.value`,
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
		if len(rec.presencePaths) != 0 {
			t.Errorf("presence called: %v", rec.presencePaths)
		}
		if len(rec.jsonTargets) != 0 {
			t.Errorf("json membership called: %v", rec.jsonTargets)
		}
	})

	t.Run("object membership matches equivalent has presence path", func(t *testing.T) {
		recIn := &membershipRecorder{}
		recHas := &membershipRecorder{}
		cIn := querysql.Compiler{Fields: recIn, Params: dollarTestParams{}, Schemas: schemas}
		cHas := querysql.Compiler{Fields: recHas, Params: dollarTestParams{}, Schemas: schemas}
		inPred, err := cIn.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `"value" in resource.observation.nested`,
		})
		if err != nil {
			t.Fatalf("in: %v", err)
		}
		hasPred, err := cHas.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: guard + `has(resource.observation.nested.value)`,
		})
		if err != nil {
			t.Fatalf("has: %v", err)
		}
		if inPred.SQL != hasPred.SQL {
			t.Errorf("in SQL = %q, has SQL = %q", inPred.SQL, hasPred.SQL)
		}
		if fmt.Sprint(recIn.presencePaths) != fmt.Sprint(recHas.presencePaths) {
			t.Errorf("in presence = %v, has presence = %v", recIn.presencePaths, recHas.presencePaths)
		}
	})

	t.Run("classification error propagates", func(t *testing.T) {
		rec := &membershipRecorder{}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `"k" in resource.bogus`,
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
		if len(rec.presencePaths)+len(rec.jsonTargets) != 0 {
			t.Errorf("dialect hooks called on classification error")
		}
	})

	t.Run("presence error propagates from object membership", func(t *testing.T) {
		sentinel := fmt.Errorf("presence boom: %w", domain.ErrInvalidArgument)
		rec := &membershipRecorder{presenceErr: sentinel}
		c := querysql.Compiler{Fields: rec, Params: dollarTestParams{}, Schemas: schemas}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `"team" in resource.labels`,
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want sentinel", err)
		}
	})
}
