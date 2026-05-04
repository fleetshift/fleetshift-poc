package domain

import "encoding/json"

// Schema is a compiled, ready-to-use schema that validates managed
// resource specs. Implementations live in infrastructure.
type Schema interface {
	Validate(spec json.RawMessage) error
}

// SchemaCompiler compiles a [RawSchema] into a [Schema] ready for
// validation. Returns an error if the document is not a valid
// JSON Schema. Implementations live in infrastructure.
type SchemaCompiler interface {
	Compile(raw RawSchema) (Schema, error)
}
