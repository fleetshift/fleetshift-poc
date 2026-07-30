package serverapp

import (
	"fmt"
	"net"
)

// Endpoint is one resolved listener address pair.
type Endpoint struct {
	// Bind is the address reported by the listener (may be 0.0.0.0:port).
	Bind string
	// Dial is a safe internal dial target (unspecified hosts normalized to 127.0.0.1).
	Dial string
}

// Endpoints are immutable resolved listener addresses after successful Start.
type Endpoints struct {
	GRPC Endpoint
	HTTP Endpoint
}

// resolveDialAddress converts a listener address into a safe dial target.
// Unspecified hosts (empty, 0.0.0.0, ::) become 127.0.0.1.
func resolveDialAddress(addr net.Addr) (string, error) {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr.String(), nil
	}
	host := tcp.IP.String()
	if tcp.IP == nil || tcp.IP.IsUnspecified() {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", tcp.Port)), nil
}

// endpointFromListener builds an Endpoint from a bound listener's address.
func endpointFromListener(lis net.Listener) (Endpoint, error) {
	bind := lis.Addr().String()
	dial, err := resolveDialAddress(lis.Addr())
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{Bind: bind, Dial: dial}, nil
}
