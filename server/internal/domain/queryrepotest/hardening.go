package queryrepotest

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// runHardeningTests exercises repository-level hardening: SQL
// injection attempts in filter string literals must be treated as
// inert data, and malformed page tokens must fail closed with
// ErrInvalidArgument rather than panicking or executing unintended
// SQL. querysql's own unit tests already cover the compiler in
// isolation.
func runHardeningTests(t *testing.T, factory Factory) {
	t.Run("InjectionInLabelValueIsInert", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		const payload = `worker'; DROP TABLE extension_resources; --`
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         fx.InventoryName,
			CandidateUID: fx.InventoryUID,
			Labels:       map[string]string{"node-role": payload},
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed injection-payload label: %v", err)
		}

		filter := fmt.Sprintf("resource.localLabels[\"node-role\"] == %q", payload)
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 (the payload should match as ordinary data)", len(results))
		}

		// If the payload had been interpolated as SQL instead of bound
		// as a parameter, the table would now be gone and this would
		// fail instead of returning the fixture set.
		everything := queryAll(t, tx, "")
		if len(everything) != 2 {
			t.Fatalf("len(everything) = %d, want 2; extension_resources may have been damaged", len(everything))
		}
	})

	t.Run("InjectionInFieldPathSegmentIsRejected", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		// A field path segment can only ever be a CEL identifier
		// (resource.spec.<ident>), so there is no CEL syntax that lets
		// a filter smuggle a raw string like this into the dotted
		// path position at all -- confirm that attempting to do so is
		// rejected as invalid CEL rather than silently accepted.
		filter := fmt.Sprintf(`resourceType == %q && resource.spec."provider); DROP TABLE resource_intents; --" == "aws"`, string(fx.ManagedType))
		err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: filter})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("MalformedPageTokens", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		for name, token := range map[string]string{
			"not base64":           "not-valid-base64!!!",
			"base64 but not JSON":  base64.RawURLEncoding.EncodeToString([]byte("not json")),
			"JSON but wrong shape": base64.RawURLEncoding.EncodeToString([]byte(`{"unexpected":"shape"}`)),
			"future token version": base64.RawURLEncoding.EncodeToString([]byte(`{"version":999,"filter_hash":"x"}`)),
			"empty JSON object":    base64.RawURLEncoding.EncodeToString([]byte(`{}`)),
			"truncated base64":     "eyJ2ZXJzaW9uIjo",
		} {
			t.Run(name, func(t *testing.T) {
				err := queryErr(t, tx, domain.QueryResourcesRequest{PageToken: token})
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Errorf("token %q: err = %v, want ErrInvalidArgument", token, err)
				}
			})
		}
	})
}
