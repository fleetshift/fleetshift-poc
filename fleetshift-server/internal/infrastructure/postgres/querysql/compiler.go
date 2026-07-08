package querysql

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// comparisonOperators maps cel-go's canonical comparison function
// names to their SQL symbol. Equals/NotEquals are symmetric;
// the four ordered comparisons get flipped by flipComparison when the
// literal appears on the left (e.g. `4 < resource.generation`).
var comparisonOperators = map[string]string{
	operators.Equals:        "=",
	operators.NotEquals:     "!=",
	operators.Less:          "<",
	operators.LessEquals:    "<=",
	operators.Greater:       ">",
	operators.GreaterEquals: ">=",
}

func flipComparison(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op
	}
}

// compileBool compiles e -- which must be a boolean-valued CEL
// expression per the minimum supported subset documented in the
// package doc -- into a SQL boolean expression. st.guard carries
// whether (and against which type) the overall filter has a
// top-level `resource_type == "..."` conjunct (see
// hasResourceTypeGuard), which st.fields may require for
// type-specific fields.
func compileBool(e ast.Expr, st *state) (string, error) {
	switch e.Kind() {
	case ast.CallKind:
		c := e.AsCall()
		switch c.FunctionName() {
		case operators.LogicalAnd:
			return compileBinaryLogic(c, st, "AND")
		case operators.LogicalOr:
			return compileBinaryLogic(c, st, "OR")
		case operators.LogicalNot:
			args := c.Args()
			if len(args) != 1 {
				return "", unsupportedExprf("logical not")
			}
			inner, err := compileBool(args[0], st)
			if err != nil {
				return "", err
			}
			return "NOT (" + inner + ")", nil
		case operators.Equals, operators.NotEquals,
			operators.Less, operators.LessEquals,
			operators.Greater, operators.GreaterEquals:
			return compileComparison(c.FunctionName(), c.Args(), st)
		case operators.In:
			return compileIn(c.Args(), st)
		default:
			return "", fmt.Errorf("filter: %w: unsupported function %q", domain.ErrInvalidArgument, c.FunctionName())
		}
	case ast.ComprehensionKind:
		return "", fmt.Errorf("filter: %w: macros (exists/all/map/filter) are not supported", domain.ErrInvalidArgument)
	default:
		return "", unsupportedExprf("boolean")
	}
}

func compileBinaryLogic(c ast.CallExpr, st *state, joiner string) (string, error) {
	args := c.Args()
	if len(args) != 2 {
		return "", unsupportedExprf("logical " + joiner)
	}
	lhs, err := compileBool(args[0], st)
	if err != nil {
		return "", err
	}
	rhs, err := compileBool(args[1], st)
	if err != nil {
		return "", err
	}
	return "(" + lhs + ") " + joiner + " (" + rhs + ")", nil
}

// compileComparison handles field OP literal or literal OP field
// (flipping ordered operators in the latter case); anything else
// (literal OP literal, field OP field) is rejected since it either
// carries no queryable field or isn't pushable to this row-shaped
// SQL. The literal's Go type becomes the [TypeHint] passed to
// st.fields.Resolve, so a resolver backing the field with
// JSON-extracted text knows what to cast it to.
func compileComparison(fn string, args []ast.Expr, st *state) (string, error) {
	if len(args) != 2 {
		return "", unsupportedExprf("comparison")
	}
	left, err := classifyOperand(args[0])
	if err != nil {
		return "", err
	}
	right, err := classifyOperand(args[1])
	if err != nil {
		return "", err
	}

	op := comparisonOperators[fn]

	var path FieldPath
	var lit any
	switch {
	case left.isField && right.isLit:
		path, lit = left.path, right.lit
	case right.isField && left.isLit:
		path, lit, op = right.path, left.lit, flipComparison(op)
	default:
		return "", fmt.Errorf("filter: %w: comparisons must be between a field and a literal", domain.ErrInvalidArgument)
	}

	expr, err := st.resolve(path, typeHintOf(lit))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", expr.SQL, op, st.b.bind(lit)), nil
}

// compileIn handles `field in [literal, ...]`. An empty list compiles
// to a constant-false predicate rather than an empty SQL "IN ()",
// which Postgres rejects -- but the field is still resolved (see
// below) rather than short-circuited away entirely. Like
// compileComparison, the first list element's Go type becomes the
// field's [TypeHint]; an empty list has no element to derive one
// from, so it resolves with TypeHintUnknown.
func compileIn(args []ast.Expr, st *state) (string, error) {
	if len(args) != 2 {
		return "", unsupportedExprf("in")
	}
	path, ok, err := fieldPathFromExpr(args[0])
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	if args[1].Kind() != ast.ListKind {
		return "", fmt.Errorf("filter: %w: \"in\" requires a list literal", domain.ErrInvalidArgument)
	}
	elems := args[1].AsList().Elements()

	values := make([]any, len(elems))
	hint := TypeHintUnknown
	for i, el := range elems {
		if el.Kind() != ast.LiteralKind {
			return "", fmt.Errorf("filter: %w: \"in\" list elements must be literals", domain.ErrInvalidArgument)
		}
		v, err := literalValue(el)
		if err != nil {
			return "", err
		}
		values[i] = v
		if i == 0 {
			hint = typeHintOf(v)
		}
	}

	// Resolve (and thus validate) the field unconditionally, even for
	// an empty list: skipping this for an empty list would let an
	// unsupported/typo'd field name (e.g. resource.aliases in [])
	// silently compile instead of failing closed like every other
	// shape referencing that field does.
	expr, err := st.resolve(path, hint)
	if err != nil {
		return "", err
	}

	if len(values) == 0 {
		// Always false regardless of expr's value (including SQL
		// NULL, per three-valued logic: NULL AND FALSE is FALSE, not
		// NULL) -- but referencing expr.SQL, rather than a bare
		// "FALSE" literal, keeps any bind parameter its resolution
		// produced (e.g. a label key) referenced by the SQL text
		// instead of left dangling and unused.
		return fmt.Sprintf("(%s IS NOT NULL AND FALSE)", expr.SQL), nil
	}

	placeholders := make([]string, len(values))
	for i, v := range values {
		placeholders[i] = st.b.bind(v)
	}
	return fmt.Sprintf("%s IN (%s)", expr.SQL, strings.Join(placeholders, ", ")), nil
}

// typeHintOf maps a bound Go literal value (see literalValue) to the
// [TypeHint] a FieldResolver needs to pick the right cast.
func typeHintOf(lit any) TypeHint {
	switch lit.(type) {
	case bool:
		return TypeHintBool
	case int64, uint64, float64:
		return TypeHintNumber
	case string:
		return TypeHintString
	default:
		return TypeHintUnknown
	}
}

// operand is the classification of one side of a comparison/in: an
// operand is either a resolvable field path or a literal value, never
// both and never neither on success (see classifyOperand).
type operand struct {
	path    FieldPath
	isField bool
	lit     any
	isLit   bool
}

// classifyOperand determines whether e is a literal or a resolvable
// field path reference. Any expression shape that is neither (e.g.
// arithmetic, another comparison, or a call that isn't the index
// operator) is rejected here as an unsupported field expression,
// matching what a FieldResolver would eventually reject anyway, but
// without depending on one being configured.
func classifyOperand(e ast.Expr) (operand, error) {
	if e.Kind() == ast.LiteralKind {
		v, err := literalValue(e)
		if err != nil {
			return operand{}, err
		}
		return operand{lit: v, isLit: true}, nil
	}
	path, ok, err := fieldPathFromExpr(e)
	if err != nil {
		return operand{}, err
	}
	if !ok {
		return operand{}, fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	return operand{path: path, isField: true}, nil
}

// literalValue extracts e's literal value as one of the primitive Go
// types this package's SQL layer knows how to bind: string, int64,
// uint64, float64, or bool.
func literalValue(e ast.Expr) (any, error) {
	v := e.AsLiteral().Value()
	switch v.(type) {
	case string, int64, uint64, float64, bool:
		return v, nil
	default:
		return nil, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, v)
	}
}

// fieldPathFromExpr flattens a chain of field selects (a.b) and index
// calls (a["b"]) into a [FieldPath], outermost-first, including the
// root identifier as its first segment. ok is false (with a nil
// error) when e is not this shape at all -- e.g. a literal or an
// arithmetic expression -- so callers can distinguish "not a field"
// from "malformed field". Index keys must be string literals, since
// every field path this package's resolvers support (resource.labels
// [...], resource.inventory.conditions[...], ...) is keyed that way;
// index keys become plain path segments, so a [FieldResolver] never
// sees the difference between resource.spec.foo and
// resource.spec["foo"].
func fieldPathFromExpr(e ast.Expr) (FieldPath, bool, error) {
	switch e.Kind() {
	case ast.IdentKind:
		return FieldPath{Segments: []string{e.AsIdent()}}, true, nil
	case ast.SelectKind:
		sel := e.AsSelect()
		if sel.IsTestOnly() {
			return FieldPath{}, false, fmt.Errorf("filter: %w: has() is not supported", domain.ErrInvalidArgument)
		}
		base, ok, err := fieldPathFromExpr(sel.Operand())
		if err != nil || !ok {
			return FieldPath{}, ok, err
		}
		return FieldPath{Segments: append(base.Segments, sel.FieldName())}, true, nil
	case ast.CallKind:
		c := e.AsCall()
		if c.FunctionName() != operators.Index || c.IsMemberFunction() {
			return FieldPath{}, false, nil
		}
		args := c.Args()
		if len(args) != 2 {
			return FieldPath{}, false, nil
		}
		base, ok, err := fieldPathFromExpr(args[0])
		if err != nil || !ok {
			return FieldPath{}, ok, err
		}
		if args[1].Kind() != ast.LiteralKind {
			return FieldPath{}, false, fmt.Errorf("filter: %w: index key must be a string literal", domain.ErrInvalidArgument)
		}
		key, ok := args[1].AsLiteral().Value().(string)
		if !ok {
			return FieldPath{}, false, fmt.Errorf("filter: %w: index key must be a string literal", domain.ErrInvalidArgument)
		}
		return FieldPath{Segments: append(base.Segments, key)}, true, nil
	default:
		return FieldPath{}, false, nil
	}
}

// hasResourceTypeGuard returns the resource_type literal from a
// top-level `resource_type == "..."` conjunct -- i.e. reachable by
// descending only through `&&` -- per the plan's "Minimum acceptable
// POC" rule that type-specific fields (e.g. resource.spec.*) require
// an explicit type filter in the same AND chain. Returns nil if there
// is no such conjunct. A guard inside an `||` branch does not count:
// that branch might not hold for the type-specific side of an OR.
func hasResourceTypeGuard(e ast.Expr) *domain.ResourceType {
	if rt, ok := resourceTypeEquality(e); ok {
		return rt
	}
	if e.Kind() != ast.CallKind {
		return nil
	}
	c := e.AsCall()
	if c.FunctionName() != operators.LogicalAnd {
		return nil
	}
	args := c.Args()
	if len(args) != 2 {
		return nil
	}
	if rt := hasResourceTypeGuard(args[0]); rt != nil {
		return rt
	}
	return hasResourceTypeGuard(args[1])
}

func resourceTypeEquality(e ast.Expr) (*domain.ResourceType, bool) {
	if e.Kind() != ast.CallKind {
		return nil, false
	}
	c := e.AsCall()
	if c.FunctionName() != operators.Equals {
		return nil, false
	}
	args := c.Args()
	if len(args) != 2 {
		return nil, false
	}
	isResourceType := func(x ast.Expr) bool {
		path, ok, err := fieldPathFromExpr(x)
		return err == nil && ok && len(path.Segments) == 1 && path.Segments[0] == "resource_type"
	}
	stringLiteral := func(x ast.Expr) (string, bool) {
		if x.Kind() != ast.LiteralKind {
			return "", false
		}
		s, ok := x.AsLiteral().Value().(string)
		return s, ok
	}
	if isResourceType(args[0]) {
		if s, ok := stringLiteral(args[1]); ok {
			rt := domain.ResourceType(s)
			return &rt, true
		}
	}
	if isResourceType(args[1]) {
		if s, ok := stringLiteral(args[0]); ok {
			rt := domain.ResourceType(s)
			return &rt, true
		}
	}
	return nil, false
}

func unsupportedExprf(what string) error {
	return fmt.Errorf("filter: %w: unsupported %s expression", domain.ErrInvalidArgument, what)
}
