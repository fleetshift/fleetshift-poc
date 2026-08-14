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
	kindExperimentalNetKey     = "KIND_EXPERIMENTAL_DOCKER_NETWORK"
	kindExperimentalNetDefault = "kind"
	// loopbackForwardHost is the default KIND_LOOPBACK_FORWARD_TO host. AIO
	// kind runs should publish this as a network alias.
	loopbackForwardHost = "fleetshift"
)

// ConfigureKindEnv writes path when a container engine socket is mounted.
// Always defaults KIND_EXPERIMENTAL_DOCKER_NETWORK=kind unless that variable
// is already present in the environment (including empty). On Dex-on, also
// writes KIND_LOOPBACK_FORWARD_TO=fleetshift:<dex-port> unless that variable
// is already set (operator pin, including empty to disable). The kind addon
// reads that env to install a loopback TCP proxy on control-plane nodes.
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
		if _, set := os.LookupEnv(loopbackForwardToEnvKey); !set {
			port := strings.TrimPrefix(dexListen, ":")
			destination := net.JoinHostPort(loopbackForwardHost, port)
			b.WriteString(loopbackForwardToEnvKey + "=")
			b.WriteString(destination)
			b.WriteString("\n")
			fmt.Fprintf(os.Stdout, "aio-init: %s=%s\n", loopbackForwardToEnvKey, destination)
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
