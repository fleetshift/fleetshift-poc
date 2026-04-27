package grpc

import (
	"encoding/json"
	"sync"
	"time"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type ConditionEvent struct {
	TargetID   string             `json:"target_id"`
	DeliveryID string             `json:"delivery_id"`
	Conditions []ConditionPayload `json:"conditions"`
	ObservedAt time.Time          `json:"observed_at"`
}

type ConditionPayload struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type ConditionBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[uint64]chan []byte
	nextID      uint64
	latest      map[domain.TargetID][]byte
}

func NewConditionBroadcaster() *ConditionBroadcaster {
	return &ConditionBroadcaster{
		subscribers: make(map[uint64]chan []byte),
		latest:      make(map[domain.TargetID][]byte),
	}
}

func (b *ConditionBroadcaster) Publish(targetID domain.TargetID, report *pb.DeliveryConditionReport) {
	evt := ConditionEvent{
		TargetID:   string(targetID),
		DeliveryID: report.DeliveryId,
	}
	if report.ObservedAt != nil {
		evt.ObservedAt = report.ObservedAt.AsTime()
	} else {
		evt.ObservedAt = time.Now()
	}
	for _, c := range report.Conditions {
		evt.Conditions = append(evt.Conditions, ConditionPayload{
			Type:    c.Type,
			Status:  c.Status,
			Reason:  c.Reason,
			Message: c.Message,
		})
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return
	}

	b.mu.Lock()
	b.latest[targetID] = data
	subs := make([]chan []byte, 0, len(b.subscribers))
	for _, ch := range b.subscribers {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- data:
		default:
		}
	}
}

func (b *ConditionBroadcaster) Subscribe() (id uint64, ch <-chan []byte, snapshot [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id = b.nextID
	c := make(chan []byte, 16)
	b.subscribers[id] = c

	snap := make([][]byte, 0, len(b.latest))
	for _, data := range b.latest {
		snap = append(snap, data)
	}

	return id, c, snap
}

func (b *ConditionBroadcaster) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subscribers[id]; ok {
		close(ch)
		delete(b.subscribers, id)
	}
}
