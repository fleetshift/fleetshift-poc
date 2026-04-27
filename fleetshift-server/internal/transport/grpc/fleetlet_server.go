package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FleetletSession holds the state for a single connected fleetlet.
type FleetletSession struct {
	TargetID domain.TargetID

	mu       sync.Mutex
	deliver  grpc.BidiStreamingServer[pb.DeliverEvent, pb.DeliverInstruction]
	pending  map[string]chan *pb.DeliverEvent // delivery_id → response
	closedCh chan struct{}

	// Health tracking
	lastHeartbeat time.Time
	addonHealth   []*pb.AddonHealth
	healthy       bool
	graceful      bool
}

func (s *FleetletSession) setDeliverStream(stream grpc.BidiStreamingServer[pb.DeliverEvent, pb.DeliverInstruction]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliver = stream
}

func (s *FleetletSession) SendDeliveryRequest(req *pb.DeliveryRequest) (chan *pb.DeliverEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliver == nil {
		return nil, fmt.Errorf("no deliver stream for target %s", s.TargetID)
	}
	ch := make(chan *pb.DeliverEvent, 8)
	s.pending[req.DeliveryId] = ch
	err := s.deliver.Send(&pb.DeliverInstruction{
		Instruction: &pb.DeliverInstruction_DeliveryRequest{
			DeliveryRequest: req,
		},
	})
	if err != nil {
		delete(s.pending, req.DeliveryId)
		return nil, fmt.Errorf("send delivery request: %w", err)
	}
	return ch, nil
}

func (s *FleetletSession) SendRemoveRequest(req *pb.RemoveRequest) (chan *pb.DeliverEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliver == nil {
		return nil, fmt.Errorf("no deliver stream for target %s", s.TargetID)
	}
	ch := make(chan *pb.DeliverEvent, 4)
	s.pending[req.DeliveryId] = ch
	err := s.deliver.Send(&pb.DeliverInstruction{
		Instruction: &pb.DeliverInstruction_RemoveRequest{
			RemoveRequest: req,
		},
	})
	if err != nil {
		delete(s.pending, req.DeliveryId)
		return nil, fmt.Errorf("send remove request: %w", err)
	}
	return ch, nil
}

func (s *FleetletSession) dispatchEvent(deliveryID string, evt *pb.DeliverEvent) {
	s.mu.Lock()
	ch, ok := s.pending[deliveryID]
	s.mu.Unlock()
	if ok {
		ch <- evt
	}
}

func (s *FleetletSession) completeDelivery(deliveryID string) {
	s.mu.Lock()
	if ch, ok := s.pending[deliveryID]; ok {
		close(ch)
		delete(s.pending, deliveryID)
	}
	s.mu.Unlock()
}

// FleetletServer implements the FleetletService gRPC server. It manages
// connected fleetlet sessions and bridges them to the platform's
// delivery pipeline.
type FleetletServer struct {
	pb.UnimplementedFleetletServiceServer

	Targets     *application.TargetService
	Logger      *slog.Logger
	Broadcaster *ConditionBroadcaster

	mu       sync.RWMutex
	sessions map[domain.TargetID]*FleetletSession
}

func NewFleetletServer(targets *application.TargetService, logger *slog.Logger) *FleetletServer {
	return &FleetletServer{
		Targets:     targets,
		Logger:      logger.With("component", "fleetlet-server"),
		Broadcaster: NewConditionBroadcaster(),
		sessions:    make(map[domain.TargetID]*FleetletSession),
	}
}

// Session returns the session for the given target, or nil if not connected.
func (s *FleetletServer) Session(id domain.TargetID) *FleetletSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

// StartHealthChecker spawns a goroutine that periodically checks all
// sessions for expired heartbeats and publishes health state changes.
func (s *FleetletServer) StartHealthChecker(ctx context.Context, timeout, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.checkHealth(timeout)
			}
		}
	}()
}

func (s *FleetletServer) checkHealth(timeout time.Duration) {
	s.mu.RLock()
	sessions := make([]*FleetletSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.RUnlock()

	now := time.Now()
	for _, sess := range sessions {
		sess.mu.Lock()
		if sess.healthy && !sess.lastHeartbeat.IsZero() && now.Sub(sess.lastHeartbeat) > timeout {
			sess.healthy = false
			sess.mu.Unlock()
			s.Logger.Warn("heartbeat expired",
				"target_id", sess.TargetID,
				"last_heartbeat", sess.lastHeartbeat)
			s.publishHealthEvent(sess.TargetID, sess)
		} else {
			sess.mu.Unlock()
		}
	}
}

func (s *FleetletServer) publishHealthEvent(targetID domain.TargetID, session *FleetletSession) {
	var conditions []*pb.Condition

	if session == nil {
		conditions = append(conditions, &pb.Condition{
			Type:    "FleetletHealth",
			Status:  "False",
			Reason:  "Disconnected",
			Message: `{"healthy":false,"graceful":false}`,
		})
	} else {
		session.mu.Lock()
		status, reason := "True", "Healthy"
		if session.graceful {
			status, reason = "False", "GracefulShutdown"
		} else if !session.healthy {
			status, reason = "False", "HeartbeatExpired"
		}
		msg := fmt.Sprintf(`{"healthy":%t,"graceful":%t,"lastHeartbeat":"%s"}`,
			session.healthy, session.graceful,
			session.lastHeartbeat.Format(time.RFC3339))

		conditions = append(conditions, &pb.Condition{
			Type:    "FleetletHealth",
			Status:  status,
			Reason:  reason,
			Message: msg,
		})

		for _, ah := range session.addonHealth {
			addonStatus := "True"
			if !ah.Connected {
				addonStatus = "False"
			}
			conditions = append(conditions, &pb.Condition{
				Type:    "AddonHealth",
				Status:  addonStatus,
				Reason:  ah.Name,
				Message: ah.Message,
			})
		}
		session.mu.Unlock()
	}

	report := &pb.DeliveryConditionReport{
		DeliveryId: "health-" + string(targetID),
		TargetId:   string(targetID),
		Conditions: conditions,
		ObservedAt: timestamppb.Now(),
	}
	s.Broadcaster.Publish(targetID, report)
}

// Control handles target registration on a bidirectional stream.
func (s *FleetletServer) Control(stream grpc.BidiStreamingServer[pb.ControlEvent, pb.ControlInstruction]) error {
	for {
		evt, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if goodbye := evt.GetGoodbye(); goodbye != nil {
			tid := domain.TargetID(goodbye.TargetId)
			s.Logger.Info("fleetlet sent goodbye",
				"target_id", tid, "reason", goodbye.Reason)

			session := s.Session(tid)
			if session != nil {
				session.mu.Lock()
				session.graceful = true
				session.mu.Unlock()
			}
			continue
		}

		reg := evt.GetRegisterTarget()
		if reg == nil {
			continue
		}

		targetID := domain.TargetID(reg.TargetId)
		logger := s.Logger.With("target_id", targetID, "target_type", reg.TargetType)

		labels := reg.Labels
		if labels == nil {
			labels = make(map[string]string)
		}
		properties := reg.Properties
		if properties == nil {
			properties = make(map[string]string)
		}

		var accepted []domain.ResourceType
		for _, rt := range reg.AcceptedResourceTypes {
			accepted = append(accepted, domain.ResourceType(rt))
		}

		target := domain.TargetInfo{
			ID:                    targetID,
			Type:                  domain.TargetType(reg.TargetType),
			Name:                  reg.Name,
			Labels:                labels,
			Properties:            properties,
			AcceptedResourceTypes: accepted,
		}

		if err := s.Targets.Register(stream.Context(), target); err != nil && !errors.Is(err, domain.ErrAlreadyExists) {
			logger.Warn("target registration failed", "error", err)
			if sendErr := stream.Send(&pb.ControlInstruction{
				Instruction: &pb.ControlInstruction_Error{
					Error: &pb.FleetletError{
						Code:     pb.FleetletError_CODE_INVALID_ARGUMENT,
						Message:  err.Error(),
						TargetId: reg.TargetId,
					},
				},
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		session := &FleetletSession{
			TargetID: targetID,
			pending:  make(map[string]chan *pb.DeliverEvent),
			closedCh: make(chan struct{}),
		}

		s.mu.Lock()
		s.sessions[targetID] = session
		s.mu.Unlock()

		logger.Info("target registered via fleetlet")

		if err := stream.Send(&pb.ControlInstruction{
			Instruction: &pb.ControlInstruction_TargetAccepted{
				TargetAccepted: &pb.TargetAccepted{
					TargetId: reg.TargetId,
				},
			},
		}); err != nil {
			return err
		}
	}
}

// Deliver handles the delivery lifecycle on a bidirectional stream.
// The fleetlet identifies itself via "target-id" gRPC metadata.
func (s *FleetletServer) Deliver(stream grpc.BidiStreamingServer[pb.DeliverEvent, pb.DeliverInstruction]) error {
	var targetID domain.TargetID
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if vals := md.Get("target-id"); len(vals) > 0 {
			targetID = domain.TargetID(vals[0])
		}
	}

	if targetID == "" {
		return fmt.Errorf("deliver stream missing target-id metadata")
	}

	session := s.Session(targetID)
	if session == nil {
		return fmt.Errorf("no session for target %s; register via Control first", targetID)
	}

	session.setDeliverStream(stream)
	s.Logger.Info("deliver stream established", "target_id", targetID)
	s.publishHealthEvent(targetID, session)

	defer func() {
		session.mu.Lock()
		graceful := session.graceful
		session.mu.Unlock()

		s.mu.Lock()
		delete(s.sessions, targetID)
		s.mu.Unlock()
		close(session.closedCh)

		if graceful {
			s.Logger.Info("fleetlet disconnected gracefully", "target_id", targetID)
		} else {
			s.Logger.Warn("fleetlet disconnected unexpectedly", "target_id", targetID)
		}
		s.Broadcaster.Remove(targetID)
	}()

	for {
		evt, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch e := evt.Event.(type) {
		case *pb.DeliverEvent_DeliveryAccepted:
			session.dispatchEvent(e.DeliveryAccepted.DeliveryId, evt)
		case *pb.DeliverEvent_DeliveryProgress:
			session.dispatchEvent(e.DeliveryProgress.DeliveryId, evt)
		case *pb.DeliverEvent_DeliveryCompleted:
			session.dispatchEvent(e.DeliveryCompleted.DeliveryId, evt)
			session.completeDelivery(e.DeliveryCompleted.DeliveryId)
		case *pb.DeliverEvent_RemoveCompleted:
			session.dispatchEvent(e.RemoveCompleted.DeliveryId, evt)
			session.completeDelivery(e.RemoveCompleted.DeliveryId)
		case *pb.DeliverEvent_ConditionReport:
			s.Logger.Info("addon data report",
				"target_id", targetID,
				"delivery_id", e.ConditionReport.DeliveryId,
				"conditions", len(e.ConditionReport.Conditions))
			s.Broadcaster.Publish(targetID, e.ConditionReport)
		case *pb.DeliverEvent_Heartbeat:
			hb := e.Heartbeat
			session.mu.Lock()
			session.lastHeartbeat = time.Now()
			session.addonHealth = hb.AddonHealth
			session.healthy = true
			session.mu.Unlock()

			s.Logger.Debug("heartbeat received",
				"target_id", targetID,
				"addons", len(hb.AddonHealth))

			s.publishHealthEvent(targetID, session)
		}
	}
}
