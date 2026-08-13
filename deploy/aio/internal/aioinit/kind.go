package aioinit

import (
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	// KindEnvPath is sourced by the fleetshift s6 run script for kind networking.
	KindEnvPath = "/run/fleetshift/kind.env"

	kindNodeRouteEnvKey        = "KIND_NODE_ROUTE_BACKEND"
	kindExperimentalNetKey     = "KIND_EXPERIMENTAL_DOCKER_NETWORK"
	kindExperimentalNetDefault = "kind"
)

// ConfigureKindEnv writes path when a container engine socket is mounted.
// Always defaults KIND_EXPERIMENTAL_DOCKER_NETWORK=kind unless that variable
// is already present in the environment (including empty). On Dex-on, also
// sets KIND_NODE_ROUTE_BACKEND so kind control-planes can DNAT
// 127.0.0.1:<dex> to this AIO's address on the shared network, unless that
// variable is already set (operator pin). Failure to resolve a backend
// address when one must be written is fatal (kind + peer Dex requires the route).
func ConfigureKindEnv(path string, dexOn bool, dexListen string) error {
	_ = os.Remove(path)
	if !containerEngineSocketPresent() {
		return nil
	}

	var b strings.Builder
	if _, set := os.LookupEnv(kindExperimentalNetKey); !set {
		b.WriteString(kindExperimentalNetKey + "=" + kindExperimentalNetDefault + "\n")
		fmt.Fprintf(os.Stdout, "aio-init: %s=%s\n", kindExperimentalNetKey, kindExperimentalNetDefault)
	}

	if dexOn {
		if _, set := os.LookupEnv(kindNodeRouteEnvKey); !set {
			ip, err := primaryIPv4()
			if err != nil {
				return fmt.Errorf("kind node route backend: %w", err)
			}
			port := strings.TrimPrefix(dexListen, ":")
			backend := net.JoinHostPort(ip, port)
			b.WriteString(kindNodeRouteEnvKey + "=")
			b.WriteString(backend)
			b.WriteString("\n")
			fmt.Fprintf(os.Stdout, "aio-init: %s=%s\n", kindNodeRouteEnvKey, backend)
		}
	}

	body := b.String()
	if body == "" {
		return nil
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		return fmt.Errorf("write kind env: %w", err)
	}
	return nil
}

// containerEngineSocketPresent reports whether CONTAINER_HOST is a unix:// path
// that currently names a socket on disk.
func containerEngineSocketPresent() bool {
	sock := strings.TrimSpace(os.Getenv("CONTAINER_HOST"))
	if !strings.HasPrefix(sock, "unix://") {
		return false
	}
	path := strings.TrimPrefix(sock, "unix://")
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeSocket != 0
}

// primaryIPv4 returns the first global unicast IPv4 on an up interface.
func primaryIPv4() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil || !ip.IsGlobalUnicast() {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address (join --network kind for kind launches)")
}
