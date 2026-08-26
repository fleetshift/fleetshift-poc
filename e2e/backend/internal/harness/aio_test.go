package harness

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPrebuiltAIORequested(t *testing.T) {
	t.Setenv(prebuiltAIOEnv, "")
	if prebuiltAIORequested() {
		t.Fatal("empty env should build from source")
	}
	for _, v := range []string{"0", "true", "yes", "TRUE"} {
		t.Setenv(prebuiltAIOEnv, v)
		if prebuiltAIORequested() {
			t.Fatalf("%q should build from source", v)
		}
	}
	t.Setenv(prebuiltAIOEnv, "1")
	if !prebuiltAIORequested() {
		t.Fatal("1 should use prebuilt AIO")
	}
}

func TestRequireLoopbackFree_OK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := requireLoopbackFree("127.0.0.1", strconv.Itoa(port)); err != nil {
		t.Fatal(err)
	}
}

func TestRequireLoopbackFree_Occupied(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	err = requireLoopbackFree("127.0.0.1", port)
	if err == nil {
		t.Fatal("expected occupied port")
	}
	if !strings.Contains(err.Error(), "occupied") {
		t.Fatalf("error = %q, want occupied", err)
	}
}

func TestRequireLoopbackNames(t *testing.T) {
	if err := requireLoopbackNames([]string{"127.0.0.1", "::1"}); err != nil {
		t.Fatal(err)
	}
	if err := requireLoopbackNames(nil); err == nil {
		t.Fatal("expected empty error")
	}
	if err := requireLoopbackNames([]string{"8.8.8.8"}); err == nil {
		t.Fatal("expected non-loopback error")
	}
}

func TestHasIPv4AndIPv6Loopback(t *testing.T) {
	if hasIPv4Loopback([]string{"::1"}) {
		t.Fatal("IPv6-only")
	}
	if !hasIPv4Loopback([]string{"127.0.0.1", "::1"}) {
		t.Fatal("want IPv4 loopback")
	}
	if hasIPv6Loopback([]string{"127.0.0.1"}) {
		t.Fatal("IPv4-only")
	}
	if !hasIPv6Loopback([]string{"127.0.0.1", "::1"}) {
		t.Fatal("want IPv6 loopback")
	}
}

func TestUIPublish(t *testing.T) {
	v4, v6 := uiPublish([]string{"127.0.0.1", "::1"})
	if !v4 || v6 {
		t.Fatalf("dual-stack = ipv4=%v ipv6=%v, want IPv4 only", v4, v6)
	}
	v4, v6 = uiPublish([]string{"127.0.0.1"})
	if !v4 || v6 {
		t.Fatalf("IPv4-only = ipv4=%v ipv6=%v", v4, v6)
	}
	v4, v6 = uiPublish([]string{"::1"})
	if v4 || !v6 {
		t.Fatalf("IPv6-only = ipv4=%v ipv6=%v, want IPv6 only", v4, v6)
	}
}

func TestHTTPSClient_Nil(t *testing.T) {
	t.Parallel()
	var f *Fixture
	_, err := f.HTTPSClient()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "nil fixture") {
		t.Fatalf("error = %v, want nil fixture", err)
	}
}

func TestWaitReadyz_TrustedCAOKWrongCAFails(t *testing.T) {
	caPEM, leaf := generateLoopbackCert(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{leaf}}
	srv.StartTLS()
	defer srv.Close()

	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := waitReadyz(ctx, srv.URL+"/readyz", caFile); err != nil {
		t.Fatalf("waitReadyz: %v", err)
	}

	other := filepath.Join(t.TempDir(), "other.crt")
	otherPEM, _ := generateLoopbackCert(t)
	if err := os.WriteFile(other, otherPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	wrongCtx, wrongCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wrongCancel()
	err := waitReadyz(wrongCtx, srv.URL+"/readyz", other)
	if err == nil {
		t.Fatal("wrong CA must not succeed (TLS must not skip-verify)")
	}
}

func TestFindRepoRoot(t *testing.T) {
	root, err := findRepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if !isRepoRoot(root) {
		t.Fatalf("not a repo root: %s", root)
	}
}

func TestRunQuiet(t *testing.T) {
	t.Parallel()
	t.Run("success writes log", func(t *testing.T) {
		t.Parallel()
		f := &Fixture{workDir: t.TempDir()}
		cmd := exec.Command("sh", "-c", "echo hello; echo err >&2")
		if err := f.runQuiet(cmd, "out.log"); err != nil {
			t.Fatalf("runQuiet: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(f.workDir, "out.log"))
		if err != nil {
			t.Fatal(err)
		}
		s := string(got)
		if !strings.Contains(s, "hello") || !strings.Contains(s, "err") {
			t.Fatalf("log = %q, want stdout and stderr", s)
		}
	})
	t.Run("failure includes log", func(t *testing.T) {
		t.Parallel()
		f := &Fixture{workDir: t.TempDir()}
		cmd := exec.Command("sh", "-c", "echo boom; echo fail-err >&2; exit 7")
		err := f.runQuiet(cmd, "out.log")
		if err == nil {
			t.Fatal("expected error")
		}
		msg := err.Error()
		if !strings.Contains(msg, "boom") || !strings.Contains(msg, "fail-err") {
			t.Fatalf("error = %q, want log dump", msg)
		}
	})
	t.Run("create fails", func(t *testing.T) {
		t.Parallel()
		f := &Fixture{workDir: filepath.Join(t.TempDir(), "missing")}
		err := f.runQuiet(exec.Command("true"), "out.log")
		if err == nil {
			t.Fatal("expected create error")
		}
	})
}

// generateLoopbackCert returns a PEM CA and a leaf TLS certificate for 127.0.0.1.
func generateLoopbackCert(t *testing.T) ([]byte, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "e2e-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(leafPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return caPEM, cert
}
