package queryrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// runSemanticFilterTests is the cross-backend QueryResources semantic
// matrix: ProtoJSON field names, exact map keys, timestamp string vs
// timestamp() instant semantics, and heterogeneous JSON equality.
// Backends invoke this via [Run]; do not duplicate these outcomes in
// postgres/sqlite SQL-shape unit tests.
func runSemanticFilterTests(t *testing.T, factory Factory) {
	t.Run("MessageFieldsUseCanonicalCamelCase", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, `resource.intentVersion == "1"`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, `resource.pauseReason == ""`)
		assertResultNames(t, results, wantName)

		for _, filter := range []string{
			`resource.intent_version == "1"`,
			`resource.pause_reason == ""`,
			`resource.local_update_time == "2026-06-01T12:00:00Z"`,
			`resource.last_transition_time == "2026-06-01T12:00:00Z"`,
		} {
			err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: filter})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("filter %q: err = %v, want ErrInvalidArgument (no snake_case alias)", filter, err)
			}
		}
	})

	t.Run("OpenMapKeysAreExactLiterals", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		// Without a SchemaProvider, spec/observation paths accept any
		// well-formed segment and preserve it literally — no silent
		// camelCase rewrite of snake_case keys.
		obs := json.RawMessage(`{"apiServerPort":"6443","api_server_port":"wrong"}`)
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Observation:  &obs,
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed observation keys: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		results := queryAll(t, tx, fmt.Sprintf(
			`resourceType == %q && resource.observation.apiServerPort == "6443"`, string(fx.InventoryType)))
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, fmt.Sprintf(
			`resourceType == %q && resource.observation.api_server_port == "6443"`, string(fx.InventoryType)))
		if len(results) != 0 {
			t.Fatalf("api_server_port == \"6443\": len = %d, want 0 (distinct key, value is \"wrong\")", len(results))
		}

		results = queryAll(t, tx, fmt.Sprintf(
			`resourceType == %q && resource.observation.api_server_port == "wrong"`, string(fx.InventoryType)))
		assertResultNames(t, results, wantName)
	})

	t.Run("MapKeysAreCaseSensitiveLiterals", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Labels: map[string]string{
				"node_role": "worker",
				"nodeRole":  "other",
			},
			ObservedAt: fixedTime,
			ReceivedAt: fixedTime,
		}}); err != nil {
			t.Fatalf("seed dual label keys: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		for _, filter := range []string{
			`resource.localLabels["node_role"] == "worker"`,
			`resource.localLabels.node_role == "worker"`,
		} {
			results := queryAll(t, tx, filter)
			assertResultNames(t, results, wantName)
		}
		for _, filter := range []string{
			`resource.localLabels["nodeRole"] == "other"`,
			`resource.localLabels.nodeRole == "other"`,
		} {
			results := queryAll(t, tx, filter)
			assertResultNames(t, results, wantName)
		}

		results := queryAll(t, tx, `resource.localLabels["Node_Role"] == "worker"`)
		if len(results) != 0 {
			t.Fatalf("mismatched key case: len = %d, want 0", len(results))
		}
		results = queryAll(t, tx, `resource.localLabels["node_role"] == "other"`)
		if len(results) != 0 {
			t.Fatalf("node_role must not alias nodeRole: len = %d, want 0", len(results))
		}

		ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "OK", "ready", fixedTime)
		if err != nil {
			t.Fatalf("build Ready: %v", err)
		}
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Conditions:   []domain.Condition{ready},
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed Ready condition: %v", err)
		}
		results = queryAll(t, tx, `resource.conditions["Ready"].status == "True"`)
		assertResultNames(t, results, wantName)
		results = queryAll(t, tx, `resource.conditions["ready"].status == "True"`)
		if len(results) != 0 {
			t.Fatalf("condition key case: len = %d, want 0", len(results))
		}
	})

	t.Run("DoubleQuoteMapKeysMatch", func(t *testing.T) {
		// Map keys may contain `"` (and other special characters).
		// Filters must address those keys as exact literals and match
		// the stored entry — including on SQLite, where labels /
		// conditions / open JSON paths use quoted json_extract members.
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		const key = `has"quote`
		quotedUID := domain.NewExtensionResourceUID()
		quotedName := domain.ResourceName("nodes/quoted-key")
		if err := tx.ExtensionResources().Create(ctx, domain.NewExtensionResource(
			quotedUID, fx.InventoryType, quotedName, fixedTime,
			domain.WithExtensionLabels(map[string]string{key: "lab"}),
		)); err != nil {
			t.Fatalf("seed quoted-key extension labels: %v", err)
		}

		cond, err := domain.NewCondition(key, domain.ConditionTrue, "OK", "ready", fixedTime)
		if err != nil {
			t.Fatalf("build quoted-key condition: %v", err)
		}
		obs := json.RawMessage(`{"has\"quote":"obs"}`)
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         quotedName,
			CandidateUID: quotedUID,
			Labels:       map[string]string{key: "loc"},
			Conditions:   []domain.Condition{cond},
			Observation:  &obs,
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed quoted-key inventory: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, quotedName)
		for _, filter := range []string{
			`resource.labels["has\"quote"] == "lab"`,
			`resource.localLabels["has\"quote"] == "loc"`,
			`resource.conditions["has\"quote"].status == "True"`,
			fmt.Sprintf(`resourceType == %q && resource.observation["has\"quote"] == "obs"`, string(fx.InventoryType)),
		} {
			assertResultNames(t, queryAll(t, tx, filter), wantName)
		}
	})

	t.Run("DirectTimestampStringsMatchResponseSpelling", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		// 500ms → ProtoJSON "…T12:00:00.500Z" (not ".5Z").
		at500 := time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)
		ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "OK", "ready", at500)
		if err != nil {
			t.Fatalf("build Ready: %v", err)
		}
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Conditions:   []domain.Condition{ready},
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed fractional lastTransitionTime: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		results := queryAll(t, tx, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.500Z"`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.5Z"`)
		if len(results) != 0 {
			t.Fatalf(".5Z spelling: len = %d, want 0 (exact ProtoJSON match only)", len(results))
		}
		results = queryAll(t, tx, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T08:00:00-04:00"`)
		if len(results) != 0 {
			t.Fatalf("offset spelling: len = %d, want 0 (direct string is not instant equality)", len(results))
		}

		// localUpdateTime is ProtoJSON of ObservedAt (whole seconds here).
		results = queryAll(t, tx, fmt.Sprintf(
			`resourceType == %q && resource.localUpdateTime == "2026-06-01T12:00:00Z"`, string(fx.InventoryType)))
		assertResultNames(t, results, wantName)
		results = queryAll(t, tx, fmt.Sprintf(
			`resourceType == %q && resource.localUpdateTime == "2026-06-01T12:00:00.000Z"`, string(fx.InventoryType)))
		if len(results) != 0 {
			t.Fatalf(".000Z spelling: len = %d, want 0", len(results))
		}
	})

	t.Run("TimestampFunctionComparesInstants", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		at500 := time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)
		ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "OK", "ready", at500)
		if err != nil {
			t.Fatalf("build Ready: %v", err)
		}
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Conditions:   []domain.Condition{ready},
			Labels:       map[string]string{"seenAt": "2026-06-01T08:00:00-04:00"},
			ObservedAt:   fixedTime, // noon UTC == 08:00-04:00
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed timestamp fixtures: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		typeScope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))

		results := queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T12:00:00.500Z")`)
		assertResultNames(t, results, wantName)

		// Same instant via offset / alternate fractional spelling.
		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T08:00:00.5-04:00")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.localUpdateTime) == timestamp("2026-06-01T08:00:00-04:00")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.localLabels["seenAt"]) == timestamp("2026-06-01T12:00:00Z")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) < timestamp("2026-06-01T13:00:00Z")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T12:00:00.123456789Z")`)
		if len(results) != 0 {
			t.Fatalf("different instant: len = %d, want 0", len(results))
		}

		// != and in are core operators on the write-time norm sibling
		// (condition lastTransitionTime) and native timestamp columns.
		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) != timestamp("2026-06-01T12:00:00Z")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) != timestamp("2026-06-01T12:00:00.500Z")`)
		if len(results) != 0 {
			t.Fatalf("!= same instant: len = %d, want 0", len(results))
		}

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) in [timestamp("2026-06-01T08:00:00.5-04:00"), timestamp("2099-01-01T00:00:00Z")]`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.conditions["Ready"].lastTransitionTime) in [timestamp("2026-06-01T12:00:00Z")]`)
		if len(results) != 0 {
			t.Fatalf("in different instant only: len = %d, want 0", len(results))
		}

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.localUpdateTime) != timestamp("2099-01-01T00:00:00Z")`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.localUpdateTime) in [timestamp("2026-06-01T08:00:00-04:00")]`)
		assertResultNames(t, results, wantName)
	})

	t.Run("TimestampMissingInvalidAndNonString", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		obs := json.RawMessage(`{"badWhen":"2026-06-01","notString":5,"when":"2026-06-01T12:00:00Z"}`)
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Observation:  &obs,
			Labels:       map[string]string{"badSeen": "infinity"},
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed invalid timestamp fixtures: %v", err)
		}

		typeScope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))
		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)

		for _, filter := range []string{
			typeScope + `timestamp(resource.observation.badWhen) == timestamp("2026-06-01T12:00:00Z")`,
			typeScope + `timestamp(resource.observation.notString) == timestamp("2026-06-01T12:00:00Z")`,
			typeScope + `timestamp(resource.observation.missingWhen) == timestamp("2026-06-01T12:00:00Z")`,
			typeScope + `timestamp(resource.localLabels["badSeen"]) == timestamp("2026-06-01T12:00:00Z")`,
			typeScope + `timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T12:00:00Z")`,
		} {
			results := queryAll(t, tx, filter)
			if len(results) != 0 {
				t.Errorf("filter %q: len = %d, want 0 (missing/invalid/non-string are non-matches)", filter, len(results))
			}
		}

		// Soft-or still matches when the timestamp side is a non-match.
		results := queryAll(t, tx, typeScope+
			`(true || timestamp(resource.observation.badWhen) == timestamp("2026-06-01T12:00:00Z"))`)
		assertResultNames(t, results, wantName)

		results = queryAll(t, tx, typeScope+
			`timestamp(resource.observation.when) == timestamp("2026-06-01T12:00:00Z")`)
		assertResultNames(t, results, wantName)
	})

	t.Run("HeterogeneousJSONEqualityAndMissing", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		obs := json.RawMessage(`{"str":"5","num":5,"flag":true}`)
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Observation:  &obs,
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed heterogeneous observation: %v", err)
		}

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		typeScope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))

		assertResultNames(t, queryAll(t, tx, typeScope+`resource.observation.str == "5"`), wantName)
		assertResultNames(t, queryAll(t, tx, typeScope+`resource.observation.num == 5`), wantName)
		assertResultNames(t, queryAll(t, tx, typeScope+`resource.observation.flag == true`), wantName)

		if len(queryAll(t, tx, typeScope+`resource.observation.str == 5`)) != 0 {
			t.Fatalf("string == number: want 0 (heterogeneous)")
		}
		if len(queryAll(t, tx, typeScope+`resource.observation.num == "5"`)) != 0 {
			t.Fatalf("number == string: want 0 (heterogeneous)")
		}

		// Present incompatible != is true; missing path remains a non-match.
		assertResultNames(t, queryAll(t, tx, typeScope+`resource.observation.str != 5`), wantName)
		if len(queryAll(t, tx, typeScope+`resource.observation.absent != 5`)) != 0 {
			t.Fatalf("missing != : want 0")
		}
		assertResultNames(t, queryAll(t, tx, typeScope+`(true || resource.observation.absent != 5)`), wantName)
		if len(queryAll(t, tx, typeScope+`(false && resource.observation.absent != 5)`)) != 0 {
			t.Fatalf("false && missing != : want 0")
		}

		// Known string fields follow the same heterogeneous rules.
		if len(queryAll(t, tx, `resource.intentVersion == 1`)) != 0 {
			t.Fatalf("intentVersion == 1: want 0 (ProtoJSON int64 string vs int literal)")
		}
	})
}

func assertResultNames(t *testing.T, results []domain.QueryResourceResult, want ...string) {
	t.Helper()
	if len(results) != len(want) {
		names := make([]string, len(results))
		for i, r := range results {
			names[i] = r.Name
		}
		t.Fatalf("result names = %v, want %v", names, want)
	}
	got := make(map[string]struct{}, len(results))
	for _, r := range results {
		got[r.Name] = struct{}{}
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			names := make([]string, len(results))
			for i, r := range results {
				names[i] = r.Name
			}
			t.Fatalf("missing result %q; got %v", name, names)
		}
	}
}
