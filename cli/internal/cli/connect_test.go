package cli

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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestValidateTransportFlags(t *testing.T) {
	tests := []struct {
		name  string
		flags globalFlags
		want  string
	}{
		{
			name:  "ca file requires tls",
			flags: globalFlags{serverCAFile: "/tmp/ca.pem"},
			want:  "--server-ca-file requires --server-tls",
		},
		{
			name:  "insecure requires tls",
			flags: globalFlags{serverInsecure: true},
			want:  "--server-insecure requires --server-tls",
		},
		{
			name:  "tls flags valid together",
			flags: globalFlags{serverTLS: true, serverCAFile: "/tmp/ca.pem", serverInsecure: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTransportFlags(tt.flags)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateTransportFlags() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateTransportFlags() expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateTransportFlags() error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestDialPlaintext(t *testing.T) {
	addr := startPlaintextTestServer(t)

	conn, err := dial(globalFlags{server: addr})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSWithCAFile(t *testing.T) {
	addr, caFile := startTLSTestServer(t)

	conn, err := dial(globalFlags{
		server:       addr,
		serverTLS:    true,
		serverCAFile: caFile,
	})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSInsecure(t *testing.T) {
	addr, _ := startTLSTestServer(t)

	conn, err := dial(globalFlags{
		server:         addr,
		serverTLS:      true,
		serverInsecure: true,
	})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSCAFileNotFound(t *testing.T) {
	_, err := dial(globalFlags{
		server:       "localhost:0",
		serverTLS:    true,
		serverCAFile: "/no/such/file.pem",
	})
	if err == nil {
		t.Fatal("dial() expected error for missing CA file, got nil")
	}
	if !strings.Contains(err.Error(), "read server CA file") {
		t.Fatalf("dial() error = %q, want substring %q", err.Error(), "read server CA file")
	}
}

func TestDialTLSCAFileInvalidPEM(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(caFile, []byte("not a cert"), 0o600); err != nil {
		t.Fatalf("write bad CA file: %v", err)
	}

	_, err := dial(globalFlags{
		server:       "localhost:0",
		serverTLS:    true,
		serverCAFile: caFile,
	})
	if err == nil {
		t.Fatal("dial() expected error for invalid PEM, got nil")
	}
	if !strings.Contains(err.Error(), "no certificates found") {
		t.Fatalf("dial() error = %q, want substring %q", err.Error(), "no certificates found")
	}
}

func assertHealthServing(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check() error = %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Health.Check() status = %v, want %v", resp.GetStatus(), healthpb.HealthCheckResponse_SERVING)
	}
}

func startTLSTestServer(t *testing.T) (string, string) {
	t.Helper()

	caKey, caCert := generateTestCA(t)
	serverCert := generateServerCert(t, caCert, caKey)
	caFile := writeCAFile(t, caCert)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{serverCert},
		})),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)

	go func() {
		_ = srv.Serve(lis)
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
		_ = lis.Close()
	})

	return lis.Addr().String(), caFile
}

func startPlaintextTestServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)

	go func() {
		_ = srv.Serve(lis)
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
		_ = lis.Close()
	})

	return lis.Addr().String()
}

func generateTestCA(t *testing.T) (*ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return key, cert
}

func generateServerCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
}

func writeCAFile(t *testing.T, cert *x509.Certificate) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "ca.pem")
	pemData := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
	if err := os.WriteFile(p, pemData, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return p
}
