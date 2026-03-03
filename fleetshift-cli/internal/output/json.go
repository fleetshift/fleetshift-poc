package output

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var jsonOpts = protojson.MarshalOptions{
	Multiline:       true,
	Indent:          "  ",
	UseProtoNames:   true,
	EmitDefaultValues: true,
}

func printJSON(w io.Writer, msg proto.Message) error {
	b, err := jsonOpts.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}

func printJSONList(w io.Writer, msgs []proto.Message) error {
	if len(msgs) == 0 {
		_, err := fmt.Fprintln(w, "[]")
		return err
	}

	if _, err := fmt.Fprintln(w, "["); err != nil {
		return err
	}
	for i, msg := range msgs {
		b, err := jsonOpts.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal json: %w", err)
		}
		suffix := ","
		if i == len(msgs)-1 {
			suffix = ""
		}
		if _, err := fmt.Fprintf(w, "  %s%s\n", string(b), suffix); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(w, "]")
	return err
}
