package sqlite

import (
	"strings"
	"testing"
)

// TestBuildQueryResourcesSQL_HydratesFromMaterializedPageWindow
// locks the SQL shape that keeps hydration on the already-limited
// page: filtered_page must be MATERIALIZED, and the outer SELECT must
// join hydration tables from fp (by uid) rather than re-driving from
// a full extension_resources scan.
func TestBuildQueryResourcesSQL_HydratesFromMaterializedPageWindow(t *testing.T) {
	order, err := resolveQueryOrder("")
	if err != nil {
		t.Fatalf("resolveQueryOrder: %v", err)
	}
	sql := buildQueryResourcesSQL("TRUE", "TRUE", order)

	if !strings.Contains(sql, "filtered_page AS MATERIALIZED") {
		t.Errorf("SQL missing MATERIALIZED filtered_page CTE:\n%s", sql)
	}
	if !strings.Contains(sql, "FROM filtered_page fp") {
		t.Errorf("SQL missing FROM filtered_page fp:\n%s", sql)
	}
	if !strings.Contains(sql, "JOIN extension_resources er ON er.uid = fp.uid") {
		t.Errorf("SQL missing page-window join to extension_resources by uid:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT ?") {
		t.Errorf("SQL missing LIMIT ? placeholder:\n%s", sql)
	}
	if strings.Contains(sql, "LIMIT $") {
		t.Errorf("SQL uses DollarParams LIMIT; want QuestionParams:\n%s", sql)
	}
}
