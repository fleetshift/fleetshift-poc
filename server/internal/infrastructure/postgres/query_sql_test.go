package postgres

import (
	"strings"
	"testing"
)

// TestBuildQueryResourcesSQL_HydratesFromMaterializedPageWindow
// locks the SQL shape that keeps hydration on the already-limited
// page: filtered_page must be MATERIALIZED, and the outer SELECT must
// join hydration tables from fp (by uid) rather than re-driving from
// a full extension_resources scan that Postgres can decorrelate into
// a 40k-row hash join.
func TestBuildQueryResourcesSQL_HydratesFromMaterializedPageWindow(t *testing.T) {
	order, err := resolveQueryOrder("")
	if err != nil {
		t.Fatalf("resolveQueryOrder: %v", err)
	}
	sql := buildQueryResourcesSQL("TRUE", "TRUE", order, 1)

	if !strings.Contains(sql, "filtered_page AS MATERIALIZED") {
		t.Errorf("SQL missing MATERIALIZED filtered_page CTE:\n%s", sql)
	}
	if strings.Contains(sql, "JOIN LATERAL") {
		t.Errorf("SQL still uses JOIN LATERAL hydration (planner can flatten this into a full-table hash join):\n%s", sql)
	}
	// Hydration must start from the page window, not a bare
	// FROM extension_resources er that the planner can pull above the
	// limit.
	if !strings.Contains(sql, "FROM filtered_page fp") {
		t.Errorf("SQL missing FROM filtered_page fp:\n%s", sql)
	}
	if !strings.Contains(sql, "JOIN extension_resources er ON er.uid = fp.uid") {
		t.Errorf("SQL missing page-window join to extension_resources by uid:\n%s", sql)
	}
}
