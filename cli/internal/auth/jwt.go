package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// DecodedJWT holds the decoded (unverified) header and payload of a JWT.
type DecodedJWT struct {
	Header map[string]any `json:"header"`
	Claims map[string]any `json:"claims"`
}

// DecodeJWT base64-decodes the header and payload of a JWT without
// performing signature verification. Intended for local inspection only.
func DecodeJWT(token string) (DecodedJWT, error) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return DecodedJWT{}, fmt.Errorf("expected 3 JWT segments, got %d", len(parts))
	}

	var d DecodedJWT

	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return DecodedJWT{}, fmt.Errorf("decode header: %w", err)
	}
	if err := json.Unmarshal(raw, &d.Header); err != nil {
		return DecodedJWT{}, fmt.Errorf("parse header: %w", err)
	}

	raw, err = base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return DecodedJWT{}, fmt.Errorf("decode payload: %w", err)
	}
	if err := json.Unmarshal(raw, &d.Claims); err != nil {
		return DecodedJWT{}, fmt.Errorf("parse payload: %w", err)
	}

	return d, nil
}
