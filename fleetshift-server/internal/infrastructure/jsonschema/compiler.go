// Package jsonschema implements [domain.SchemaCompiler] using
// google/jsonschema-go.
package jsonschema

import (
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Compiler implements [domain.SchemaCompiler].
type Compiler struct{}

func (Compiler) Compile(raw domain.RawSchema) (domain.Schema, error) {
	var sch jsonschema.Schema
	if err := json.Unmarshal(json.RawMessage(raw), &sch); err != nil {
		return nil, fmt.Errorf("invalid schema JSON: %w", err)
	}
	if err := metaValidate(&sch); err != nil {
		return nil, err
	}
	resolved, err := sch.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("resolve schema: %w", err)
	}
	return &schema{resolved: resolved}, nil
}

var validTypes = map[string]bool{
	"null": true, "boolean": true, "object": true,
	"array": true, "number": true, "integer": true, "string": true,
}

// metaValidate performs lightweight structural checks that the Google library
// defers to validation time (e.g. invalid type keywords).
func metaValidate(sch *jsonschema.Schema) error {
	if sch.Type != "" && !validTypes[sch.Type] {
		return fmt.Errorf("invalid schema: unknown type %q", sch.Type)
	}
	for _, t := range sch.Types {
		if !validTypes[t] {
			return fmt.Errorf("invalid schema: unknown type %q", t)
		}
	}
	for _, sub := range sch.Properties {
		if sub != nil {
			if err := metaValidate(sub); err != nil {
				return err
			}
		}
	}
	if sch.Items != nil {
		if err := metaValidate(sch.Items); err != nil {
			return err
		}
	}
	return nil
}

type schema struct {
	resolved *jsonschema.Resolved
}

func (s *schema) Validate(spec json.RawMessage) error {
	var inst any
	if err := json.Unmarshal(spec, &inst); err != nil {
		return fmt.Errorf("invalid JSON in spec: %w", err)
	}
	if err := s.resolved.Validate(inst); err != nil {
		return fmt.Errorf("spec validation failed: %w", err)
	}
	return nil
}
