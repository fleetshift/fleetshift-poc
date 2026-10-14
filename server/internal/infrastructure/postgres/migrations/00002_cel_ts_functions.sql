-- +goose Up
-- CEL timestamp helpers for QueryResources filters.
-- cel_ts_norm: RFC 3339 text → fixed-width UTC (nanosecond-preserving).
-- cel_ts_protojson / cel_ts_protojson_tstz: ProtoJSON string rendering.

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cel_ts_norm(s text) RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  m text[];
  frac text;
  base text;
  ts timestamptz;
  utc_sec text;
  nanos text;
BEGIN
  -- Strict RFC 3339 matching Go's time.RFC3339Nano / ParseCELTimestamp:
  -- uppercase T only; comma or dot fraction; fractions longer than
  -- nine digits are truncated to nanoseconds (not rejected).
  m := regexp_match(
    s,
    '^([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2})([.,][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
  );
  IF m IS NULL THEN
    RETURN NULL;
  END IF;

  frac := COALESCE(substring(m[3] from 2), '');
  IF length(frac) > 9 THEN
    frac := left(frac, 9);
  END IF;
  nanos := rpad(frac, 9, '0');

  -- Rebuild without fractional seconds so timestamptz stays exact
  -- at second resolution; nanos are carried separately.
  base := m[1] || 'T' || m[2] || m[4];
  BEGIN
    ts := base::timestamptz;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;

  utc_sec := to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS');
  -- CEL range is years 0001–9999.
  IF utc_sec < '0001-01-01T00:00:00' OR utc_sec > '9999-12-31T23:59:59' THEN
    RETURN NULL;
  END IF;

  RETURN utc_sec || '.' || nanos || 'Z';
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cel_ts_protojson(s text) RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  norm text;
  sec text;
  frac text;
  nanos int;
BEGIN
  norm := cel_ts_norm(s);
  IF norm IS NULL THEN
    RETURN NULL;
  END IF;
  sec := left(norm, 19);
  frac := substring(norm from 21 for 9);
  nanos := frac::int;
  IF nanos = 0 THEN
    RETURN sec || 'Z';
  ELSIF nanos % 1000000 = 0 THEN
    RETURN sec || '.' || lpad((nanos / 1000000)::text, 3, '0') || 'Z';
  ELSIF nanos % 1000 = 0 THEN
    RETURN sec || '.' || lpad((nanos / 1000)::text, 6, '0') || 'Z';
  ELSE
    RETURN sec || '.' || lpad(nanos::text, 9, '0') || 'Z';
  END IF;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION cel_ts_protojson_tstz(ts timestamptz) RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  utc_sec text;
  -- Postgres timestamptz stores microseconds.
  micros int;
BEGIN
  utc_sec := to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS');
  micros := (EXTRACT(MICROSECONDS FROM ts AT TIME ZONE 'UTC')::int) % 1000000;
  IF micros = 0 THEN
    RETURN utc_sec || 'Z';
  ELSIF micros % 1000 = 0 THEN
    RETURN utc_sec || '.' || lpad((micros / 1000)::text, 3, '0') || 'Z';
  ELSE
    RETURN utc_sec || '.' || lpad(micros::text, 6, '0') || 'Z';
  END IF;
END;
$$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS cel_ts_protojson_tstz(timestamptz);
DROP FUNCTION IF EXISTS cel_ts_protojson(text);
DROP FUNCTION IF EXISTS cel_ts_norm(text);
