package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Property keys for target properties.
const (
	PropAPIServer              = "api_server"
	PropCACert                 = "ca_cert"
	PropServiceAccountToken    = "service_account_token"
	PropServiceAccountTokenRef = "service_account_token_ref"
)

// AgentPool manages the lifecycle of Agents — one per ready target.
// It builds K8s clients from target properties + vault, creates
// Agents with delivery and indexer delegates, and implements
// [domain.DeliveryAgent] by routing to the appropriate Agent.
type AgentPool struct {
	ctx              context.Context
	store            domain.Store
	vault            domain.Vault
	inventoryWriter  domain.InventoryWriter
	deliveryReporter domain.DeliveryReporter
	keyResolver      *domain.KeyResolver
	httpClient       *http.Client
	logger           *slog.Logger

	mu     sync.Mutex
	agents map[domain.TargetID]*Agent
}

var _ domain.IndexAgent = (*AgentPool)(nil)

// NewAgentPool creates an AgentPool. The provided context governs the
// lifetime of all agents created by the AgentPool — it must outlive
// individual request or activity contexts.
func NewAgentPool(
	ctx context.Context,
	store domain.Store,
	vault domain.Vault,
	inventoryWriter domain.InventoryWriter,
	deliveryReporter domain.DeliveryReporter,
	keyResolver *domain.KeyResolver,
	httpClient *http.Client,
	logger *slog.Logger,
) *AgentPool {
	return &AgentPool{
		ctx:              ctx,
		store:            store,
		vault:            vault,
		inventoryWriter:  inventoryWriter,
		deliveryReporter: deliveryReporter,
		keyResolver:      keyResolver,
		httpClient:       httpClient,
		logger:           logger,
		agents:           make(map[domain.TargetID]*Agent),
	}
}

// StartIndexing builds K8s clients, creates an Agent with both
// delivery and indexer delegates, and starts it in a goroutine. It is
// idempotent: if an agent for the given target is already running, it
// returns nil without starting a duplicate.
func (m *AgentPool) StartIndexing(ctx context.Context, target domain.TargetInfo) error {
	id := target.ID()

	m.mu.Lock()
	if _, ok := m.agents[id]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	cfg, err := m.buildRESTConfig(ctx, target)
	if err != nil {
		return fmt.Errorf("build rest config for %s: %w", id, err)
	}

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create dynamic client for %s: %w", id, err)
	}

	discClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create discovery client for %s: %w", id, err)
	}

	logger := m.logger.With("target", string(id))

	dc := newDeliveryDelegate(m.deliveryReporter, m.keyResolver, m.httpClient)
	var ic *indexerDelegate
	if m.inventoryWriter != nil {
		ic = newIndexerDelegate(string(id), dynClient, discClient, m.inventoryWriter, IndexConfig{
			Schema: DefaultKubernetesSchema(),
		}, logger)
	}

	ta := NewAgent(m.ctx, id, cfg, dynClient, discClient, dc, ic, logger)

	// Re-check under lock to handle races.
	m.mu.Lock()
	if _, ok := m.agents[id]; ok {
		m.mu.Unlock()
		ta.Stop()
		return nil
	}
	m.agents[id] = ta
	m.mu.Unlock()

	go ta.start()
	return nil
}

// StopIndexing stops the agent for the given target and removes it
// from tracking. It does NOT delete inventory — the caller is
// responsible for cleanup.
func (m *AgentPool) StopIndexing(ctx context.Context, target domain.TargetInfo) error {
	m.mu.Lock()
	ta, ok := m.agents[target.ID()]
	delete(m.agents, target.ID())
	m.mu.Unlock()

	if !ok {
		return nil
	}
	ta.Stop()
	return nil
}

// GetAgent returns the running Agent for the given ID, or nil if
// no agent is running.
func (m *AgentPool) GetAgent(id domain.TargetID) *Agent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.agents[id]
}

// StopAll stops all running agents.
func (m *AgentPool) StopAll() {
	m.mu.Lock()
	agents := make(map[domain.TargetID]*Agent, len(m.agents))
	for id, ta := range m.agents {
		agents[id] = ta
	}
	m.agents = make(map[domain.TargetID]*Agent)
	m.mu.Unlock()

	for _, ta := range agents {
		ta.Stop()
	}
}

// Deliver implements [domain.DeliveryAgent] by routing to the
// appropriate Agent.
func (m *AgentPool) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	ta := m.GetAgent(target.ID())
	if ta == nil {
		return fmt.Errorf("no agent for target %s", target.ID())
	}
	return ta.Deliver(ctx, target, deliveryID, manifests, auth, att, generation)
}

// Remove implements [domain.DeliveryAgent] by routing to the
// appropriate Agent.
func (m *AgentPool) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	ta := m.GetAgent(target.ID())
	if ta == nil {
		return fmt.Errorf("no agent for target %s", target.ID())
	}
	return ta.Remove(ctx, target, deliveryID, manifests, auth, att, generation)
}

// buildRESTConfig constructs a [rest.Config] from the target's properties
// and optional vault-backed service account token.
func (m *AgentPool) buildRESTConfig(ctx context.Context, target domain.TargetInfo) (*rest.Config, error) {
	props := target.Properties()
	host := props[PropAPIServer]
	if host == "" {
		return nil, fmt.Errorf("missing property %q", PropAPIServer)
	}

	cfg := &rest.Config{
		Host: host,
	}

	if ca := props[PropCACert]; ca != "" {
		cfg.TLSClientConfig = rest.TLSClientConfig{
			CAData: []byte(ca),
		}
	}

	// Resolve SA token: direct value first, then vault reference.
	if tok := props[PropServiceAccountToken]; tok != "" {
		cfg.BearerToken = tok
	} else if ref := props[PropServiceAccountTokenRef]; ref != "" {
		if m.vault == nil {
			return nil, fmt.Errorf("vault required for %q but not configured", PropServiceAccountTokenRef)
		}
		val, err := m.vault.Get(ctx, domain.SecretRef(ref))
		if err != nil {
			return nil, fmt.Errorf("resolve vault ref %q: %w", ref, err)
		}
		cfg.BearerToken = string(val)
	}

	return cfg, nil
}
