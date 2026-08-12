package aioinit

import "testing"

func TestFixedEndpoints(t *testing.T) {
	want := Endpoints{
		UIOrigin:   "http://127.0.0.1:8085",
		UICallback: "http://127.0.0.1:8085/auth/callback",
		HTTPListen: ":8085",
		GRPCListen: ":50051",
		DexListen:  ":5556",
	}
	if FixedEndpoints != want {
		t.Fatalf("FixedEndpoints = %+v, want %+v", FixedEndpoints, want)
	}
	if PeerDexIssuer != "https://127.0.0.1:5556/dex" {
		t.Fatalf("PeerDexIssuer = %q", PeerDexIssuer)
	}
}
