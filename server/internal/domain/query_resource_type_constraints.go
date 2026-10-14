package domain

import (
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
)

// ResourceTypeConstraints reports whether filter contains a top-level
// &&-reachable `resourceType == "..."` or `resourceType in [...]`
// conjunct, and the union of string literals those conjuncts name.
//
// Constrained is false when the filter is empty or has no such
// conjunct (including resourceType comparisons nested only under
// `||`). Types may be empty when Constrained is true (e.g.
// `resourceType in []`). Invalid CEL returns [ErrInvalidArgument].
//
// [ResolveQueryResourceTypeScope] uses this to reject inactive named
// types when a [QuerySchemaProvider] is present. Repository
// schema-backed field guards still require a single top-level
// equality (see querysql's hasResourceTypeGuard).
func ResourceTypeConstraints(filter string) (constrained bool, types []ResourceType, err error) {
	if filter == "" {
		return false, nil, nil
	}
	env, err := queryFilterCELEnv()
	if err != nil {
		return false, nil, fmt.Errorf("filter: create CEL environment: %w", err)
	}
	checked, issues := env.Compile(filter)
	if issues != nil && issues.Err() != nil {
		return false, nil, fmt.Errorf("filter: %w: %v", ErrInvalidArgument, issues.Err())
	}
	seen := map[ResourceType]struct{}{}
	var out []ResourceType
	collectResourceTypeConstraints(checked.NativeRep().Expr(), &constrained, seen, &out)
	return constrained, out, nil
}

var (
	queryFilterCELEnvOnce sync.Once
	queryFilterCELEnvVal  *cel.Env
	queryFilterCELEnvErr  error
)

func queryFilterCELEnv() (*cel.Env, error) {
	queryFilterCELEnvOnce.Do(func() {
		queryFilterCELEnvVal, queryFilterCELEnvErr = cel.NewEnv(
			cel.EagerlyValidateDeclarations(true),
			cel.Variable("name", cel.StringType),
			cel.Variable("resourceType", cel.StringType),
			cel.Variable("resource", cel.DynType),
		)
	})
	return queryFilterCELEnvVal, queryFilterCELEnvErr
}

func collectResourceTypeConstraints(e ast.Expr, constrained *bool, seen map[ResourceType]struct{}, out *[]ResourceType) {
	if addResourceTypeConstraintLiterals(e, seen, out) {
		*constrained = true
		return
	}
	if e.Kind() != ast.CallKind {
		return
	}
	c := e.AsCall()
	if c.FunctionName() != operators.LogicalAnd {
		return
	}
	args := c.Args()
	if len(args) != 2 {
		return
	}
	collectResourceTypeConstraints(args[0], constrained, seen, out)
	collectResourceTypeConstraints(args[1], constrained, seen, out)
}

func addResourceTypeConstraintLiterals(e ast.Expr, seen map[ResourceType]struct{}, out *[]ResourceType) bool {
	if rt, ok := resourceTypeEqualityConstraint(e); ok {
		addResourceTypeOnce(rt, seen, out)
		return true
	}
	return resourceTypeInListConstraint(e, seen, out)
}

func addResourceTypeOnce(rt ResourceType, seen map[ResourceType]struct{}, out *[]ResourceType) {
	if _, ok := seen[rt]; ok {
		return
	}
	seen[rt] = struct{}{}
	*out = append(*out, rt)
}

func resourceTypeEqualityConstraint(e ast.Expr) (ResourceType, bool) {
	if e.Kind() != ast.CallKind {
		return "", false
	}
	c := e.AsCall()
	if c.FunctionName() != operators.Equals {
		return "", false
	}
	args := c.Args()
	if len(args) != 2 {
		return "", false
	}
	if isResourceTypeIdent(args[0]) {
		if s, ok := stringLiteral(args[1]); ok {
			return ResourceType(s), true
		}
	}
	if isResourceTypeIdent(args[1]) {
		if s, ok := stringLiteral(args[0]); ok {
			return ResourceType(s), true
		}
	}
	return "", false
}

func resourceTypeInListConstraint(e ast.Expr, seen map[ResourceType]struct{}, out *[]ResourceType) bool {
	if e.Kind() != ast.CallKind {
		return false
	}
	c := e.AsCall()
	if c.FunctionName() != operators.In {
		return false
	}
	args := c.Args()
	if len(args) != 2 {
		return false
	}
	if !isResourceTypeIdent(args[0]) {
		return false
	}
	if args[1].Kind() != ast.ListKind {
		return false
	}
	for _, el := range args[1].AsList().Elements() {
		if s, ok := stringLiteral(el); ok {
			addResourceTypeOnce(ResourceType(s), seen, out)
		}
	}
	return true
}

func isResourceTypeIdent(e ast.Expr) bool {
	return e.Kind() == ast.IdentKind && e.AsIdent() == "resourceType"
}

func stringLiteral(e ast.Expr) (string, bool) {
	if e.Kind() != ast.LiteralKind {
		return "", false
	}
	s, ok := e.AsLiteral().Value().(string)
	return s, ok
}
