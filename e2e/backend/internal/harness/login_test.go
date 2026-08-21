package harness

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestReadAuthURL_ParsesLineAmongNoise(t *testing.T) {
	r := strings.NewReader("Waiting for callback...\nAUTH_URL https://example/idp?x=1\nmore\n")
	u, err := readAuthURL(context.Background(), r)
	if err != nil {
		t.Fatalf("readAuthURL: %v", err)
	}
	if u != "https://example/idp?x=1" {
		t.Fatalf("url = %q", u)
	}
}

func TestReadAuthURL_EOFBeforeAuthURL(t *testing.T) {
	_, err := readAuthURL(context.Background(), strings.NewReader("nope\n"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "before AUTH_URL") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadAuthURL_EmptyAUTHURLLine(t *testing.T) {
	_, err := readAuthURL(context.Background(), strings.NewReader("AUTH_URL \n"))
	if err == nil {
		t.Fatal("expected error for empty AUTH_URL")
	}
	if !strings.Contains(err.Error(), "before AUTH_URL") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadAuthURL_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	t.Cleanup(func() {
		_ = pw.Close()
		_ = pr.Close()
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := readAuthURL(ctx, pr)
		errCh <- err
	}()

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readAuthURL did not return after cancel")
	}
}

func TestParseAuthURLLine(t *testing.T) {
	u, ok := parseAuthURLLine("AUTH_URL https://example/idp?x=1")
	if !ok || u != "https://example/idp?x=1" {
		t.Fatalf("got %q %v", u, ok)
	}
	if _, ok := parseAuthURLLine("Waiting for callback..."); ok {
		t.Fatal("non AUTH_URL line")
	}
	if _, ok := parseAuthURLLine("AUTH_URL "); ok {
		t.Fatal("empty URL")
	}
}

func TestParseAccessToken(t *testing.T) {
	t.Parallel()
	got, err := parseAccessToken([]byte(`{"access_token":"live-access","token_type":"Bearer"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != "live-access" {
		t.Fatalf("got %q", got)
	}
}

func TestParseAccessToken_Missing(t *testing.T) {
	t.Parallel()
	if _, err := parseAccessToken([]byte(`{"token_type":"Bearer"}`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseAccessToken_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseAccessToken([]byte("{")); err == nil {
		t.Fatal("expected error")
	}
}
