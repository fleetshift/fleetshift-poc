package sqlite

import (
	"database/sql/driver"

	moderncsqlite "modernc.org/sqlite"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func init() {
	// cel_ts_norm converts an RFC 3339 / RFC 3339 Nano string to the
	// fixed-width UTC form used for timestamp() instant comparisons.
	// Invalid input yields SQL NULL (CEL error / non-match).
	moderncsqlite.MustRegisterFunction("cel_ts_norm", &moderncsqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		Scalar: func(_ *moderncsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			s, ok := driverValueString(args[0])
			if !ok {
				return nil, nil
			}
			t, err := querysql.ParseCELTimestamp(s)
			if err != nil {
				return nil, nil
			}
			return formatTimestampNorm(t), nil
		},
	})

	// cel_ts_protojson re-emits a timestamp string in ProtoJSON form
	// for direct string comparisons against response fields.
	moderncsqlite.MustRegisterFunction("cel_ts_protojson", &moderncsqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		Scalar: func(_ *moderncsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			s, ok := driverValueString(args[0])
			if !ok {
				return nil, nil
			}
			t, err := querysql.ParseCELTimestamp(s)
			if err != nil {
				return nil, nil
			}
			return formatProtoJSONTimestamp(t), nil
		},
	})
}

func driverValueString(v driver.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}
