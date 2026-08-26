package harness

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func TestParseAccessToken_Blank(t *testing.T) {
	t.Parallel()
	if _, err := parseAccessToken([]byte(`{"access_token":"  "}`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyAuthJSON(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	dst := t.TempDir()
	want := `{"issuer_url":"https://example/idp"}`
	if err := os.WriteFile(filepath.Join(src, authConfigName), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, credentialsName), []byte(`{"access_token":"secret"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyAuthJSON(src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dst, authConfigName))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got %q want %q", got, want)
	}
	info, err := os.Stat(filepath.Join(dst, authConfigName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("perm = %o, want 0600", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(dst, credentialsName)); !os.IsNotExist(err) {
		t.Fatalf("credentials.json must not be copied: %v", err)
	}
}

func TestCopyAuthJSON_Missing(t *testing.T) {
	t.Parallel()
	err := copyAuthJSON(t.TempDir(), t.TempDir())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "auth.json") {
		t.Fatalf("error = %v, want auth.json", err)
	}
}

func TestLoginAs_NilFixture(t *testing.T) {
	t.Parallel()
	var f *Fixture
	_, err := f.LoginAs(PersonaDev)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "nil fixture") {
		t.Fatalf("error = %v, want nil fixture", err)
	}
}

func TestLoginAs_EmptyPersona(t *testing.T) {
	t.Parallel()
	f := &Fixture{workDir: t.TempDir(), configDir: t.TempDir()}
	_, err := f.LoginAs("")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "persona is required") {
		t.Fatalf("error = %v, want persona is required", err)
	}
}

func TestLoginAs_MissingAuthJSON(t *testing.T) {
	t.Parallel()
	f := &Fixture{workDir: t.TempDir(), configDir: t.TempDir()}
	_, err := f.LoginAs(PersonaDev)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "auth.json") {
		t.Fatalf("error = %v, want auth.json", err)
	}
}

func TestAccessTokenFrom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, credentialsName), []byte(`{"access_token":"live-access"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := (&Fixture{}).AccessTokenFrom(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "live-access" {
		t.Fatalf("got %q", got)
	}
}

func TestAccessTokenFrom_Nil(t *testing.T) {
	t.Parallel()
	var f *Fixture
	_, err := f.AccessTokenFrom(t.TempDir())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "nil fixture") {
		t.Fatalf("error = %v, want nil fixture", err)
	}
}

func TestAccessTokenFrom_Missing(t *testing.T) {
	t.Parallel()
	_, err := (&Fixture{}).AccessTokenFrom(t.TempDir())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "read credentials") {
		t.Fatalf("error = %v, want read credentials", err)
	}
}
