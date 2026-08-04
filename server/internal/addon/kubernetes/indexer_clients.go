package kubernetes

import (
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// IndexerClients constructs Kubernetes clients for in-process indexing from a
// REST config. Production uses [DefaultIndexerClients]; tests inject fakes.
type IndexerClients interface {
	// Dynamic returns a dynamic client for the given REST config.
	Dynamic(*rest.Config) (dynamic.Interface, error)
	// Discovery returns a discovery client for the given REST config.
	Discovery(*rest.Config) (discovery.DiscoveryInterface, error)
}

// DefaultIndexerClients builds real client-go dynamic and discovery clients.
type DefaultIndexerClients struct{}

// Dynamic implements [IndexerClients].
func (DefaultIndexerClients) Dynamic(cfg *rest.Config) (dynamic.Interface, error) {
	return dynamic.NewForConfig(cfg)
}

// Discovery implements [IndexerClients].
func (DefaultIndexerClients) Discovery(cfg *rest.Config) (discovery.DiscoveryInterface, error) {
	return discovery.NewDiscoveryClientForConfig(cfg)
}

// Compile-time check.
var _ IndexerClients = DefaultIndexerClients{}
