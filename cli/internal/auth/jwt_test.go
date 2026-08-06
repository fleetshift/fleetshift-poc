package auth

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func encodeSegment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal segment: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func buildJWT(t *testing.T, header, claims map[string]any) string {
	t.Helper()
	return encodeSegment(t, header) + "." + encodeSegment(t, claims) + ".sig"
}

func TestDecodeJWT(t *testing.T) {
	header := map[string]any{"alg": "RS256", "typ": "JWT"}
	claims := map[string]any{
		"sub": "user-42",
		"iss": "https://issuer.example.com",
		"aud": "api://test",
		"exp": float64(1742220000),
	}

	decoded, err := DecodeJWT(buildJWT(t, header, claims))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := decoded.Header["alg"]; got != "RS256" {
		t.Errorf("header alg = %v, want RS256", got)
	}
	if got := decoded.Claims["sub"]; got != "user-42" {
		t.Errorf("claims sub = %v, want user-42", got)
	}
	if got := decoded.Claims["iss"]; got != "https://issuer.example.com" {
		t.Errorf("claims iss = %v, want https://issuer.example.com", got)
	}
}

func TestDecodeJWT_InvalidSegmentCount(t *testing.T) {
	if _, err := DecodeJWT("only.two"); err == nil {
		t.Error("expected error for 2-segment token")
	}
	if _, err := DecodeJWT("single"); err == nil {
		t.Error("expected error for 1-segment token")
	}
}

func TestDecodeJWT_InvalidBase64(t *testing.T) {
	if _, err := DecodeJWT("!!!.valid.sig"); err == nil {
		t.Error("expected error for bad base64 header")
	}
}
