// Package aioinit holds FleetShift AIO packaging helpers: sealed endpoints,
// sandbox PKI, peer Dex config, serve argv, GCP HCP intent, and kind networking.
package aioinit

// Endpoints is the sealed AIO listen set and matching UI origin/callback URLs.
type Endpoints struct {
	UIOrigin   string
	UICallback string
	HTTPListen string
	GRPCListen string
	// DexListen is the reserved peer-Dex listen address (":5556"). Dex may be
	// parked on Dex-off; the port stays part of the sealed set.
	DexListen string
}

// FixedEndpoints is the sealed AIO endpoint set (8085 / 50051 / 5556).
var FixedEndpoints = Endpoints{
	UIOrigin:   "http://127.0.0.1:8085",
	UICallback: "http://127.0.0.1:8085/auth/callback",
	HTTPListen: ":8085",
	GRPCListen: ":50051",
	DexListen:  ":5556",
}

// PeerDexIssuer is the loopback issuer URL for peer Dex (Dex-on only).
const PeerDexIssuer = "https://127.0.0.1:5556/dex"
