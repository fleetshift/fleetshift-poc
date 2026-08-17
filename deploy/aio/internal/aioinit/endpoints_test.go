package aioinit

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixedEndpoints(t *testing.T) {
	origin := "https://" + net.JoinHostPort(PublicHost, gatewayPort)
	want := Endpoints{
		PublicOrigin:  origin,
		UICallback:    origin + "/auth/callback",
		SilentRenew:   origin + "/silent-renew.html",
		GatewayListen: ":" + gatewayPort,
		HTTPListen:    net.JoinHostPort("127.0.0.1", httpPort),
		DexListen:     net.JoinHostPort("127.0.0.1", dexPort),
		GRPCListen:    ":" + grpcPort,
	}
	if PublicHost != "fleetshift-sandbox.localhost" {
		t.Fatalf("PublicHost = %q", PublicHost)
	}
	if gatewayPort != "8085" {
		t.Fatalf("gatewayPort = %q", gatewayPort)
	}
	if FixedEndpoints != want {
		t.Fatalf("FixedEndpoints = %+v, want %+v", FixedEndpoints, want)
	}
	if PeerDexIssuer != origin+"/dex" {
		t.Fatalf("PeerDexIssuer = %q", PeerDexIssuer)
	}
	if PeerDexIssuer[len(PeerDexIssuer)-1] == '/' {
		t.Fatal("PeerDexIssuer must not have a trailing slash")
	}
}

func TestWritePublicEnv(t *testing.T) {
	t.Run("dex-on", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "public.env")
		if err := WritePublicEnv(path, true); err != nil {
			t.Fatal(err)
		}
		body := string(readFile(t, path))
		for _, want := range []string{
			"PUBLIC_HOST=" + PublicHost + "\n",
			"PUBLIC_ORIGIN=" + FixedEndpoints.PublicOrigin + "\n",
			"DEX_DISCOVERY_URL=" + PeerDexIssuer + "/.well-known/openid-configuration\n",
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("public.env missing %q:\n%s", want, body)
			}
		}
	})
	t.Run("dex-off omits discovery", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "public.env")
		if err := WritePublicEnv(path, false); err != nil {
			t.Fatal(err)
		}
		body := string(readFile(t, path))
		if strings.Contains(body, "DEX_DISCOVERY_URL=") {
			t.Fatalf("Dex-off public.env should omit discovery: %s", body)
		}
	})
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
