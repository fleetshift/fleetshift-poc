package output

import "fmt"

// Format controls how command output is rendered.
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
)

// Validate returns an error if f is not a supported format.
func (f Format) Validate() error {
	switch f {
	case FormatTable, FormatJSON:
		return nil
	default:
		return fmt.Errorf("unsupported output format %q (valid: table, json)", string(f))
	}
}
