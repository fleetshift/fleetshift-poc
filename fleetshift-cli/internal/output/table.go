package output

import (
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/proto"
)

func printTable(w io.Writer, msgs []proto.Message, columns []Column) error {
	if len(columns) == 0 {
		return nil
	}

	widths := make([]int, len(columns))
	for i, c := range columns {
		widths[i] = len(c.Header)
	}

	rows := make([][]string, len(msgs))
	for r, msg := range msgs {
		row := make([]string, len(columns))
		for c, col := range columns {
			row[c] = col.Value(msg)
			if len(row[c]) > widths[c] {
				widths[c] = len(row[c])
			}
		}
		rows[r] = row
	}

	fmts := make([]string, len(columns))
	for i, width := range widths {
		if i == len(columns)-1 {
			fmts[i] = "%s"
		} else {
			fmts[i] = fmt.Sprintf("%%-%ds", width)
		}
	}

	headers := make([]string, len(columns))
	for i, c := range columns {
		headers[i] = fmt.Sprintf(fmts[i], strings.ToUpper(c.Header))
	}
	if _, err := fmt.Fprintln(w, strings.Join(headers, "   ")); err != nil {
		return err
	}

	for _, row := range rows {
		cells := make([]string, len(columns))
		for i, val := range row {
			cells[i] = fmt.Sprintf(fmts[i], val)
		}
		if _, err := fmt.Fprintln(w, strings.Join(cells, "   ")); err != nil {
			return err
		}
	}

	return nil
}
