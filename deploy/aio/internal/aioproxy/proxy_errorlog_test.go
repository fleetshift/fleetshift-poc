package aioproxy

import (
	"bytes"
	"strings"
	"testing"
)

func TestUntrustedCertificateLog(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		line string
		drop bool
	}{
		{
			name: "unknown certificate",
			line: "2026/09/03 23:33:36 http: TLS handshake error from 10.88.0.6:39154: remote error: tls: unknown certificate\n",
			drop: true,
		},
		{
			name: "unknown certificate authority",
			line: "http: TLS handshake error from 10.88.0.6:39154: remote error: tls: unknown certificate authority\n",
			drop: true,
		},
		{
			name: "plaintext to https",
			line: "http: TLS handshake error from 127.0.0.1:1: tls: first record does not look like a TLS handshake\n",
			drop: false,
		},
		{
			name: "bad certificate",
			line: "http: TLS handshake error from 127.0.0.1:1: remote error: tls: bad certificate\n",
			drop: false,
		},
		{
			name: "eof",
			line: "http: TLS handshake error from 127.0.0.1:1: EOF\n",
			drop: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isUntrustedCertificateLog(tc.line); got != tc.drop {
				t.Fatalf("drop = %v, want %v for %q", got, tc.drop, tc.line)
			}
		})
	}
}

func TestUntrustedCertFilter_Write(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	f := untrustedCertFilter{w: &buf}

	dropped := []byte("http: TLS handshake error from 10.88.0.6:1: remote error: tls: unknown certificate\n")
	n, err := f.Write(dropped)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(dropped) {
		t.Fatalf("dropped Write = %d, want %d (short write would fail the stdlib logger)", n, len(dropped))
	}
	if buf.Len() != 0 {
		t.Fatalf("untrusted cert alerts leaked: %q", buf.String())
	}

	kept := []byte("http: TLS handshake error from 10.88.0.6:1: tls: first record does not look like a TLS handshake\n")
	n, err = f.Write(kept)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(kept) {
		t.Fatalf("kept Write = %d, want %d", n, len(kept))
	}
	if !strings.Contains(buf.String(), "first record does not look like a TLS handshake") {
		t.Fatalf("expected non-cert handshake error to log, got %q", buf.String())
	}
}
