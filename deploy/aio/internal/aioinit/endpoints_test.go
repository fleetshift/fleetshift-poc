package aioinit

import "testing"

func TestFixedEndpoints(t *testing.T) {
	want := Endpoints{
		PublicOrigin:  "https://fleetshift-sandbox.localhost:8085",
		UICallback:    "https://fleetshift-sandbox.localhost:8085/auth/callback",
		SilentRenew:   "https://fleetshift-sandbox.localhost:8085/silent-renew.html",
		GatewayListen: ":8085",
		HTTPListen:    "127.0.0.1:8086",
		DexListen:     "127.0.0.1:5556",
		GRPCListen:    ":50051",
	}
	if FixedEndpoints != want {
		t.Fatalf("FixedEndpoints = %+v, want %+v", FixedEndpoints, want)
	}
	if PublicHost != "fleetshift-sandbox.localhost" {
		t.Fatalf("PublicHost = %q", PublicHost)
	}
	if CanonicalHost != "fleetshift-sandbox.localhost:8085" {
		t.Fatalf("CanonicalHost = %q", CanonicalHost)
	}
	if PeerDexIssuer != "https://fleetshift-sandbox.localhost:8085/dex" {
		t.Fatalf("PeerDexIssuer = %q", PeerDexIssuer)
	}
	if PeerDexIssuer[len(PeerDexIssuer)-1] == '/' {
		t.Fatal("PeerDexIssuer must not have a trailing slash")
	}
	if FixedEndpoints.PublicOrigin+"/dex" != PeerDexIssuer {
		t.Fatalf("PeerDexIssuer must be PublicOrigin + /dex, got %q", PeerDexIssuer)
	}
	if FixedEndpoints.PublicOrigin+"/auth/callback" != FixedEndpoints.UICallback {
		t.Fatalf("UICallback = %q", FixedEndpoints.UICallback)
	}
}
