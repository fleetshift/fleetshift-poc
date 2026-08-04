package postgres

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// ResolveJSONMembership implements [querysql.FieldResolver].
// Orchestration (object → presence, scalar reject, classification)
// lives in querysql; this method only maps logical JSON roots to
// columns and renders Postgres JSONB membership SQL.
func (r queryFieldResolver) ResolveJSONMembership(target querysql.JSONMembershipTarget, key string, ctx querysql.ResolveContext) (string, error) {
	if target.Kind != querysql.ContainerKindList && target.Kind != querysql.ContainerKindUnknown {
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
	container, err := jsonbMembershipColumn(target, ctx.Bind)
	if err != nil {
		return "", err
	}
	keyPh := ctx.Bind(key)
	switch target.Kind {
	case querysql.ContainerKindList:
		return jsonbListStringMembership(container, keyPh), nil
	case querysql.ContainerKindUnknown:
		return jsonbDynamicMembership(container, keyPh), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
}

func jsonbMembershipColumn(target querysql.JSONMembershipTarget, bind func(any) string) (string, error) {
	var column string
	switch target.Root {
	case querysql.JSONMembershipRootSpec:
		column = "ri.spec"
	case querysql.JSONMembershipRootObservation:
		column = "inv.observation"
	default:
		return "", fmt.Errorf("filter: %w: unsupported container field", domain.ErrInvalidArgument)
	}
	return chainedJSONB(column, target.Path, bind), nil
}

func chainedJSONB(column string, names []string, bind func(any) string) string {
	var sb strings.Builder
	sb.WriteString(column)
	for _, n := range names {
		fmt.Fprintf(&sb, " -> %s", bind(n))
	}
	return sb.String()
}

func jsonbListStringMembership(container, keyPlaceholder string) string {
	// NULL container → SQL NULL (three-valued); non-array → NULL.
	// Array string membership uses @> (contains the JSON string scalar)
	// rather than jsonb_array_elements so each candidate pays one jsonb
	// operator instead of expanding the array element-by-element.
	return fmt.Sprintf(
		`(CASE
			WHEN %s IS NULL THEN NULL
			WHEN jsonb_typeof(%s) <> 'array' THEN NULL
			ELSE %s @> to_jsonb(%s::text)
		END)`,
		container, container, container, keyPlaceholder,
	)
}

func jsonbDynamicMembership(container, keyPlaceholder string) string {
	return fmt.Sprintf(
		`(CASE jsonb_typeof(%s)
			WHEN 'object' THEN %s ? %s
			WHEN 'array' THEN %s @> to_jsonb(%s::text)
			ELSE NULL
		END)`,
		container, container, keyPlaceholder, container, keyPlaceholder,
	)
}
