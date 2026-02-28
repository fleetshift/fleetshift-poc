package domain

import "encoding/json"

// Manifest is an opaque declarative payload delivered to a target.
type Manifest struct {
	Raw json.RawMessage
}
