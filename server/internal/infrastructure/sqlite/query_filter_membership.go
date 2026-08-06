package sqlite

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// ResolveJSONMembership implements [querysql.FieldResolver].
// Orchestration (object → presence, scalar reject, classification)
// lives in querysql; this method only maps logical JSON roots to
// columns and renders SQLite json_type/json_each membership SQL.
func (r queryFieldResolver) ResolveJSONMembership(target querysql.JSONMembershipTarget, key string, ctx querysql.ResolveContext) (string, error) {
	if target.Kind != querysql.ContainerKindList && target.Kind != querysql.ContainerKindUnknown {
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
	ref, err := jsonMembershipRef(target, ctx.Bind)
	if err != nil {
		return "", err
	}
	keyPh := ctx.Bind(key)
	switch target.Kind {
	case querysql.ContainerKindList:
		return jsonListStringMembership(ref, keyPh), nil
	case querysql.ContainerKindUnknown:
		return jsonDynamicMembership(ref, keyPh), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
}

// jsonRef addresses a JSON value as column + optional path expression
// so json_type/json_each can inspect nested documents without
// extracting scalars into bare SQL text (which is not valid JSON).
type jsonRef struct {
	column string
	path   string // empty means the whole column; otherwise a SQL expr
}

func jsonMembershipRef(target querysql.JSONMembershipTarget, bind func(any) string) (jsonRef, error) {
	var column string
	switch target.Root {
	case querysql.JSONMembershipRootSpec:
		column = "ri.spec"
	case querysql.JSONMembershipRootObservation:
		column = "inv.observation"
	default:
		return jsonRef{}, fmt.Errorf("filter: %w: unsupported container field", domain.ErrInvalidArgument)
	}
	if len(target.Path) == 0 {
		return jsonRef{column: column}, nil
	}
	return jsonRef{column: column, path: jsonQuotedPathChain(target.Path, bind)}, nil
}

func (r jsonRef) typeExpr() string {
	if r.path == "" {
		return fmt.Sprintf("json_type(%s)", r.column)
	}
	return fmt.Sprintf("json_type(%s, %s)", r.column, r.path)
}

func (r jsonRef) eachExpr() string {
	if r.path == "" {
		return fmt.Sprintf("json_each(%s)", r.column)
	}
	return fmt.Sprintf("json_each(%s, %s)", r.column, r.path)
}

func (r jsonRef) nullCheck() string {
	if r.path == "" {
		return r.column + " IS NULL"
	}
	// Missing nested path → json_type IS NULL.
	return fmt.Sprintf("json_type(%s, %s) IS NULL", r.column, r.path)
}

func jsonListStringMembership(ref jsonRef, keyPlaceholder string) string {
	return fmt.Sprintf(
		`(CASE
			WHEN %s THEN NULL
			WHEN %s <> 'array' THEN NULL
			ELSE EXISTS (
				SELECT 1 FROM %s AS e
				WHERE e.type = 'text' AND e.value = %s
			)
		END)`,
		ref.nullCheck(), ref.typeExpr(), ref.eachExpr(), keyPlaceholder,
	)
}

func jsonDynamicMembership(ref jsonRef, keyPlaceholder string) string {
	keyPath := jsonQuotedPathExpr(keyPlaceholder)
	var objectPresent string
	if ref.path == "" {
		objectPresent = fmt.Sprintf("json_type(%s, %s) IS NOT NULL", ref.column, keyPath)
	} else {
		// Append ."key" onto the container path expression.
		objectPresent = fmt.Sprintf(
			`json_type(%s, %s || '."' || replace(replace(%s, '\', '\\'), '"', '\"') || '"') IS NOT NULL`,
			ref.column, ref.path, keyPlaceholder,
		)
	}
	return fmt.Sprintf(
		`(CASE %s
			WHEN 'object' THEN %s
			WHEN 'array' THEN EXISTS (
				SELECT 1 FROM %s AS e
				WHERE e.type = 'text' AND e.value = %s
			)
			ELSE NULL
		END)`,
		ref.typeExpr(), objectPresent, ref.eachExpr(), keyPlaceholder,
	)
}
