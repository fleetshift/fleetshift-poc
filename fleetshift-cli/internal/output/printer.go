package output

import (
	"io"

	"google.golang.org/protobuf/proto"
)

// Column describes a single table column, mapping a header to a value
// extractor that operates on a proto message.
type Column struct {
	Header string
	Value  func(proto.Message) string
}

// Printer renders proto messages according to the configured [Format].
type Printer struct {
	format Format
	w      io.Writer
}

// NewPrinter returns a [Printer] that writes to w in the given format.
func NewPrinter(w io.Writer, format Format) *Printer {
	return &Printer{w: w, format: format}
}

// PrintResource renders a single proto message.
// For table format, columns define the displayed fields.
// For JSON format, columns are ignored and the full message is marshaled.
func (p *Printer) PrintResource(msg proto.Message, columns []Column) error {
	switch p.format {
	case FormatJSON:
		return printJSON(p.w, msg)
	default:
		return printTable(p.w, []proto.Message{msg}, columns)
	}
}

// PrintResourceList renders a list of proto messages.
// For table format, columns define the displayed fields.
// For JSON format, columns are ignored and the full list is marshaled as a JSON array.
func (p *Printer) PrintResourceList(msgs []proto.Message, columns []Column) error {
	switch p.format {
	case FormatJSON:
		return printJSONList(p.w, msgs)
	default:
		return printTable(p.w, msgs, columns)
	}
}
