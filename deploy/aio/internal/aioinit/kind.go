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

	loopbackForwardToEnvKey    = "KIND_LOOPBACK_FORWARD_TO"
	loopbackIssuerHostEnvKey   = "KIND_LOOPBACK_ISSUER_HOST"
	kindExperimentalNetKey     = "KIND_EXPERIMENTAL_DOCKER_NETWORK"
	kindExperimentalNetDefault = "kind"
	// loopbackForwardHost is the default KIND_LOOPBACK_FORWARD_TO host. AIO
	// kind runs should publish this as a network alias.
	loopbackForwardHost = "fleetshift"
)

// ConfigureKindEnv writes path when a container engine socket is mounted.
// Always defaults KIND_EXPERIMENTAL_DOCKER_NETWORK=kind unless that variable
// is already present in the environment (including empty). On Dex-on, also
// writes KIND_LOOPBACK_FORWARD_TO=fleetshift:<gateway-port> and
// KIND_LOOPBACK_ISSUER_HOST unless those variables are already set (operator
// pin, including empty to disable). The kind addon reads that env to install
// a loopback TCP proxy and kube-apiserver hostAliases on control-plane nodes.
func ConfigureKindEnv(path string, dexOn bool, gatewayListen string) error {
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
		if _, set := os.LookupEnv(loopbackForwardToEnvKey); !set {
			port, err := listenPort(gatewayListen)
			if err != nil {
				return fmt.Errorf("kind loopback forward port: %w", err)
			}
			destination := net.JoinHostPort(loopbackForwardHost, port)
			b.WriteString(loopbackForwardToEnvKey + "=")
			b.WriteString(destination)
			b.WriteString("\n")
			fmt.Fprintf(os.Stdout, "aio-init: %s=%s\n", loopbackForwardToEnvKey, destination)
		}
		if _, set := os.LookupEnv(loopbackIssuerHostEnvKey); !set {
			b.WriteString(loopbackIssuerHostEnvKey + "=" + PublicHost + "\n")
			fmt.Fprintf(os.Stdout, "aio-init: %s=%s\n", loopbackIssuerHostEnvKey, PublicHost)
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

// listenPort returns the port from a listen address (":8085" or "127.0.0.1:8085").
func listenPort(listen string) (string, error) {
	if after, ok := strings.CutPrefix(listen, ":"); ok {
		if after == "" {
			return "", fmt.Errorf("missing port in %q", listen)
		}
		return after, nil
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if port == "" {
		return "", fmt.Errorf("missing port in %q", listen)
	}
	return port, nil
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
