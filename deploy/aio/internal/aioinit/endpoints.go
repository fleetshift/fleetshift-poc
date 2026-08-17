// Package aioinit holds FleetShift AIO packaging helpers: public origin and
// listen addresses, sandbox PKI, peer Dex config, serve argv, GCP HCP intent,
// and kind networking.
package aioinit

const (
	// PublicHost is the browser-facing DNS name.
	PublicHost = "fleetshift-sandbox.localhost"
	// CanonicalHost is the HTTP Host the AIO TLS edge accepts (name + port).
	CanonicalHost = "fleetshift-sandbox.localhost:8085"
	// PeerDexIssuer is the public peer-Dex issuer (Dex-on only). Scheme, host,
	// port, and path must match discovery issuer and ID-token iss exactly.
	PeerDexIssuer = "https://fleetshift-sandbox.localhost:8085/dex"
	// HostsMarker tags the AIO-container /etc/hosts mapping.
	HostsMarker = "fleetshift-aio"
)

// Endpoints is the AIO public origin plus internal listen addresses.
// PublicOrigin is the browser-facing URL. HTTPListen and DexListen are the
// FleetShift and Dex container-loopback binds.
type Endpoints struct {
	PublicOrigin  string
	UICallback    string
	SilentRenew   string
	GatewayListen string
	HTTPListen    string
	DexListen     string
	GRPCListen    string
}

// FixedEndpoints is the AIO endpoint set.
var FixedEndpoints = Endpoints{
	PublicOrigin:  "https://fleetshift-sandbox.localhost:8085",
	UICallback:    "https://fleetshift-sandbox.localhost:8085/auth/callback",
	SilentRenew:   "https://fleetshift-sandbox.localhost:8085/silent-renew.html",
	GatewayListen: ":8085",
	HTTPListen:    "127.0.0.1:8086",
	DexListen:     "127.0.0.1:5556",
	GRPCListen:    ":50051",
}
