package kind

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	ownershipConfigMapName      = "fleetshift-kind-ownership"
	ownershipConfigMapNamespace = "kube-system"
	ownershipGenerationKey      = "generation"
)

// GenerationDisposition is the result of [GenerationStore.CheckAndAdvance].
type GenerationDisposition int

const (
	// GenerationCreated means no prior record existed; proposed was established.
	GenerationCreated GenerationDisposition = iota
	// GenerationSame means proposed equaled the recorded high-water mark.
	GenerationSame
	// GenerationAdvanced means proposed was greater than recorded; the mark was raised.
	GenerationAdvanced
	// GenerationStale means proposed was less than recorded; the mark was unchanged.
	GenerationStale
)

func (d GenerationDisposition) String() string {
	switch d {
	case GenerationCreated:
		return "created"
	case GenerationSame:
		return "same"
	case GenerationAdvanced:
		return "advanced"
	case GenerationStale:
		return "stale"
	default:
		return fmt.Sprintf("GenerationDisposition(%d)", int(d))
	}
}

// GenerationStore persists the last-accepted delivery generation for a
// kind cluster. The Kubernetes implementation stores it in a ConfigMap
// inside the cluster. Callers must treat [GenerationStale] from
// CheckAndAdvance as a hard failure on every path.
//
// Intentional toy limitations:
//   - fs-- naming is convention, not proof of ownership.
//   - Create-crash or persist failure before the ConfigMap write: the
//     next delivery sees a missing ConfigMap, treats configuration as
//     unknown, and recreates (possible recreation loop until persist
//     succeeds).
//   - Tombstone gaps after Remove or failed recreation lose the
//     high-water mark.
//   - Peek-then-recreate has a concurrency gap without an external journal.
type GenerationStore interface {
	Get(ctx context.Context, kindClusterName string, kubeconfig []byte) (recorded domain.Generation, found bool, err error)
	CheckAndAdvance(ctx context.Context, kindClusterName string, kubeconfig []byte, proposed domain.Generation) (GenerationDisposition, domain.Generation, error)
	// Forget drops local state for kindClusterName. No-op for the
	// Kubernetes store (the ConfigMap is deleted with the cluster).
	Forget(kindClusterName string)
}

// MemoryGenerationStore is a process-local store for tests. State is
// keyed by kind cluster name and cleared by Forget when the cluster
// is deleted.
type MemoryGenerationStore struct {
	mu   sync.Mutex
	gens map[string]domain.Generation
}

// NewMemoryGenerationStore returns an empty in-memory [GenerationStore].
func NewMemoryGenerationStore() *MemoryGenerationStore {
	return &MemoryGenerationStore{gens: make(map[string]domain.Generation)}
}

// Get implements [GenerationStore].
func (s *MemoryGenerationStore) Get(_ context.Context, kindClusterName string, _ []byte) (domain.Generation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.gens[kindClusterName]
	return g, ok, nil
}

// CheckAndAdvance implements [GenerationStore].
func (s *MemoryGenerationStore) CheckAndAdvance(_ context.Context, kindClusterName string, _ []byte, proposed domain.Generation) (GenerationDisposition, domain.Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recorded, found := s.gens[kindClusterName]
	if !found {
		s.gens[kindClusterName] = proposed
		return GenerationCreated, proposed, nil
	}
	if proposed < recorded {
		return GenerationStale, recorded, nil
	}
	if proposed == recorded {
		return GenerationSame, recorded, nil
	}
	s.gens[kindClusterName] = proposed
	return GenerationAdvanced, proposed, nil
}

// Forget implements [GenerationStore].
func (s *MemoryGenerationStore) Forget(kindClusterName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.gens, kindClusterName)
}

// SetForTest sets the recorded generation without CheckAndAdvance.
// Used to simulate races in unit tests.
func (s *MemoryGenerationStore) SetForTest(kindClusterName string, gen domain.Generation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gens[kindClusterName] = gen
}

// kubeGenerationStore reads/writes the ownership ConfigMap via the
// cluster API using the provided kubeconfig.
type kubeGenerationStore struct {
	newClient func(kubeconfig []byte) (kubernetes.Interface, error)
}

func newKubeGenerationStore() *kubeGenerationStore {
	return &kubeGenerationStore{newClient: clientFromKubeconfig}
}

func clientFromKubeconfig(kubeconfig []byte) (kubernetes.Interface, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

func (s *kubeGenerationStore) Forget(string) {}

func (s *kubeGenerationStore) Get(ctx context.Context, _ string, kubeconfig []byte) (domain.Generation, bool, error) {
	client, err := s.newClient(kubeconfig)
	if err != nil {
		return 0, false, err
	}
	cm, err := client.CoreV1().ConfigMaps(ownershipConfigMapNamespace).Get(ctx, ownershipConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	g, err := parseGenerationData(cm.Data)
	if err != nil {
		return 0, false, err
	}
	return g, true, nil
}

func (s *kubeGenerationStore) CheckAndAdvance(ctx context.Context, _ string, kubeconfig []byte, proposed domain.Generation) (GenerationDisposition, domain.Generation, error) {
	client, err := s.newClient(kubeconfig)
	if err != nil {
		return 0, 0, err
	}
	for range 8 {
		cm, err := client.CoreV1().ConfigMaps(ownershipConfigMapNamespace).Get(ctx, ownershipConfigMapName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = client.CoreV1().ConfigMaps(ownershipConfigMapNamespace).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ownershipConfigMapName,
					Namespace: ownershipConfigMapNamespace,
				},
				Data: map[string]string{ownershipGenerationKey: strconv.FormatInt(int64(proposed), 10)},
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			if err != nil {
				return 0, 0, err
			}
			return GenerationCreated, proposed, nil
		}
		if err != nil {
			return 0, 0, err
		}
		recorded, err := parseGenerationData(cm.Data)
		if err != nil {
			return 0, 0, err
		}
		if proposed < recorded {
			return GenerationStale, recorded, nil
		}
		if proposed == recorded {
			return GenerationSame, recorded, nil
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[ownershipGenerationKey] = strconv.FormatInt(int64(proposed), 10)
		_, err = client.CoreV1().ConfigMaps(ownershipConfigMapNamespace).Update(ctx, cm, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			return 0, 0, err
		}
		return GenerationAdvanced, proposed, nil
	}
	return 0, 0, fmt.Errorf("check and advance generation: exceeded conflict retries")
}

func parseGenerationData(data map[string]string) (domain.Generation, error) {
	if data == nil {
		return 0, fmt.Errorf("ownership configmap missing data")
	}
	raw, ok := data[ownershipGenerationKey]
	if !ok || raw == "" {
		return 0, fmt.Errorf("ownership configmap missing %q", ownershipGenerationKey)
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse ownership generation %q: %w", raw, err)
	}
	return domain.Generation(n), nil
}
