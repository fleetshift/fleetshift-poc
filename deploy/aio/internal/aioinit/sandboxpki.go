package aioinit

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	// sandboxPKIDir is the fixed sandbox PKI root.
	sandboxPKIDir = "/data/sandbox/pki"

	caCertName   = "ca.crt"
	caKeyName    = "ca.key"
	leafCertName = "server.crt" // on-disk name kept for volume compatibility
	leafKeyName  = "server.key"

	caValidity    = 365 * 24 * time.Hour
	leafValidity  = 90 * 24 * time.Hour
	serialMaxBits = 128
)

// SandboxPKIPaths holds absolute paths for Dex-on sandbox CA and leaf material.
type SandboxPKIPaths struct {
	Dir      string
	CACert   string
	CAKey    string
	LeafCert string
	LeafKey  string
}

// DefaultSandboxPKIPaths returns the fixed layout under /data/sandbox/pki.
func DefaultSandboxPKIPaths() SandboxPKIPaths {
	return SandboxPKIPaths{
		Dir:      sandboxPKIDir,
		CACert:   filepath.Join(sandboxPKIDir, caCertName),
		CAKey:    filepath.Join(sandboxPKIDir, caKeyName),
		LeafCert: filepath.Join(sandboxPKIDir, leafCertName),
		LeafKey:  filepath.Join(sandboxPKIDir, leafKeyName),
	}
}

// EnsureSandboxPKI generates or reuses the Dex-on sandbox CA and leaf.
// Leaf SAN is IP 127.0.0.1 only. dexUID/dexGID own the leaf private key
// (0400); the CA cert is world-readable.
func EnsureSandboxPKI(paths SandboxPKIPaths, dexUID, dexGID int) error {
	if err := os.MkdirAll(paths.Dir, 0755); err != nil {
		return fmt.Errorf("sandbox pki dir: %w", err)
	}

	caCert, caKey, err := loadOrCreateCA(paths)
	if err != nil {
		return err
	}
	if err := ensureLeaf(paths, caCert, caKey); err != nil {
		return err
	}
	return applySandboxPKIOwnership(paths, dexUID, dexGID)
}

// loadOrCreateCA reuses a valid on-disk CA or creates a new one when absent.
// Partial CA state (cert without key or the reverse) is an error.
func loadOrCreateCA(paths SandboxPKIPaths) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPEM, certErr := os.ReadFile(paths.CACert)
	keyPEM, keyErr := os.ReadFile(paths.CAKey)
	switch {
	case certErr == nil && keyErr == nil:
		cert, key, err := parseCA(certPEM, keyPEM)
		if err != nil {
			return nil, nil, fmt.Errorf("existing CA invalid: %w", err)
		}
		return cert, key, nil
	case errors.Is(certErr, os.ErrNotExist) && errors.Is(keyErr, os.ErrNotExist):
		return createCA(paths)
	case certErr == nil && errors.Is(keyErr, os.ErrNotExist):
		return nil, nil, fmt.Errorf("partial CA state: %s present without key", paths.CACert)
	case errors.Is(certErr, os.ErrNotExist) && keyErr == nil:
		return nil, nil, fmt.Errorf("partial CA state: %s present without cert", paths.CAKey)
	default:
		if certErr != nil {
			return nil, nil, fmt.Errorf("read CA cert: %w", certErr)
		}
		return nil, nil, fmt.Errorf("read CA key: %w", keyErr)
	}
}

// createCA writes a new ECDSA P-256 CA cert and key to paths.
func createCA(paths SandboxPKIPaths) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "FleetShift Sandbox CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA cert: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	if err := writePEMFile(paths.CAKey, "EC PRIVATE KEY", mustMarshalEC(key), 0600); err != nil {
		return nil, nil, err
	}
	if err := writePEMFile(paths.CACert, "CERTIFICATE", der, 0644); err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// ensureLeaf reuses a valid leaf under caCert or renews an invalid/missing leaf.
func ensureLeaf(paths SandboxPKIPaths, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) error {
	if leafOK(paths, caCert) {
		return nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate leaf key: %w", err)
	}
	serial, err := randSerial()
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "FleetShift Sandbox Dex"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create leaf cert: %w", err)
	}
	if err := writePEMFile(paths.LeafKey, "EC PRIVATE KEY", mustMarshalEC(key), 0400); err != nil {
		return err
	}
	return writePEMFile(paths.LeafCert, "CERTIFICATE", der, 0644)
}

// leafOK reports whether the on-disk leaf is present, unexpired, SAN-correct,
// and verifiable against caCert.
func leafOK(paths SandboxPKIPaths, caCert *x509.Certificate) bool {
	certPEM, err := os.ReadFile(paths.LeafCert)
	if err != nil {
		return false
	}
	if _, err := os.Stat(paths.LeafKey); err != nil {
		return false
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	if time.Now().After(cert.NotAfter) || time.Now().Before(cert.NotBefore) {
		return false
	}
	if len(cert.IPAddresses) != 1 || !cert.IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) {
		return false
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	_, err = cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	return err == nil
}

// applySandboxPKIOwnership sets root ownership on CA material and dexUID:dexGID on the
// leaf key when running as root. Non-root callers keep creator ownership.
func applySandboxPKIOwnership(paths SandboxPKIPaths, dexUID, dexGID int) error {
	if err := os.Chmod(paths.LeafKey, 0400); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		// Unit tests and non-root runs keep creator ownership.
		return nil
	}
	if err := os.Chown(paths.Dir, 0, 0); err != nil {
		return err
	}
	for _, p := range []string{paths.CACert, paths.CAKey, paths.LeafCert} {
		if err := os.Chown(p, 0, 0); err != nil {
			return err
		}
	}
	if err := os.Chown(paths.LeafKey, dexUID, dexGID); err != nil {
		return fmt.Errorf("chown leaf key: %w", err)
	}
	return nil
}

// parseCA decodes PEM CA cert and EC private key bytes and validates the cert.
func parseCA(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cb, _ := pem.Decode(certPEM)
	if cb == nil {
		return nil, nil, fmt.Errorf("CA cert PEM missing")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	if !cert.IsCA {
		return nil, nil, fmt.Errorf("certificate is not a CA")
	}
	if time.Now().After(cert.NotAfter) {
		return nil, nil, fmt.Errorf("CA expired")
	}
	kb, _ := pem.Decode(keyPEM)
	if kb == nil {
		return nil, nil, fmt.Errorf("CA key PEM missing")
	}
	key, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// writePEMFile atomically writes a PEM block to path with mode.
func writePEMFile(path, typ string, der []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// mustMarshalEC marshals an EC private key or panics.
func mustMarshalEC(key *ecdsa.PrivateKey) []byte {
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(err)
	}
	return b
}

// randSerial returns a random certificate serial number.
func randSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), serialMaxBits)
	return rand.Int(rand.Reader, limit)
}
