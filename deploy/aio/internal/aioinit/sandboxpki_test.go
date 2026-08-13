package aioinit_test

import (
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

func testSandboxPKIPaths(root string) aioinit.SandboxPKIPaths {
	return aioinit.SandboxPKIPaths{
		Dir:      filepath.Join(root, "pki"),
		CACert:   filepath.Join(root, "pki", "ca.crt"),
		CAKey:    filepath.Join(root, "pki", "ca.key"),
		LeafCert: filepath.Join(root, "pki", "server.crt"),
		LeafKey:  filepath.Join(root, "pki", "server.key"),
	}
}

func TestEnsureSandboxPKI_GenerateAndReuse(t *testing.T) {
	paths := testSandboxPKIPaths(t.TempDir())
	uid, gid := os.Getuid(), os.Getgid()
	if err := aioinit.EnsureSandboxPKI(paths, uid, gid); err != nil {
		t.Fatal(err)
	}
	ca1, err := os.ReadFile(paths.CACert)
	if err != nil {
		t.Fatal(err)
	}
	leaf1, err := os.ReadFile(paths.LeafCert)
	if err != nil {
		t.Fatal(err)
	}
	assertLeafSAN(t, leaf1)

	if err := aioinit.EnsureSandboxPKI(paths, uid, gid); err != nil {
		t.Fatal(err)
	}
	ca2, err := os.ReadFile(paths.CACert)
	if err != nil {
		t.Fatal(err)
	}
	if string(ca1) != string(ca2) {
		t.Fatal("CA cert changed on reuse")
	}
}

func TestEnsureSandboxPKI_PartialCAState(t *testing.T) {
	uid, gid := os.Getuid(), os.Getgid()
	t.Run("cert without key", func(t *testing.T) {
		paths := testSandboxPKIPaths(t.TempDir())
		if err := os.MkdirAll(paths.Dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(paths.CACert, []byte("not-a-real-cert\n"), 0644); err != nil {
			t.Fatal(err)
		}
		err := aioinit.EnsureSandboxPKI(paths, uid, gid)
		if err == nil || !strings.Contains(err.Error(), "partial CA state") {
			t.Fatalf("EnsureSandboxPKI() = %v, want partial CA state", err)
		}
	})
	t.Run("key without cert", func(t *testing.T) {
		paths := testSandboxPKIPaths(t.TempDir())
		if err := os.MkdirAll(paths.Dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(paths.CAKey, []byte("not-a-real-key\n"), 0600); err != nil {
			t.Fatal(err)
		}
		err := aioinit.EnsureSandboxPKI(paths, uid, gid)
		if err == nil || !strings.Contains(err.Error(), "partial CA state") {
			t.Fatalf("EnsureSandboxPKI() = %v, want partial CA state", err)
		}
	})
}

func assertLeafSAN(t *testing.T, pemBytes []byte) {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("leaf PEM missing")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.IPAddresses) != 1 || !cert.IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("leaf SAN = %v", cert.IPAddresses)
	}
	if len(cert.DNSNames) != 0 {
		t.Fatalf("unexpected DNS SANs: %v", cert.DNSNames)
	}
}
