package grpc

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// FleetletSession holds the state for a single connected fleetlet.
type FleetletSession struct {
	TargetID domain.TargetID

	mu       sync.Mutex
	deliver  grpc.BidiStreamingServer[pb.DeliverEvent, pb.DeliverInstruction]
	pending  map[string]chan *pb.DeliverEvent // delivery_id → response
	closedCh chan struct{}
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

	defer func() {
		s.mu.Lock()
		delete(s.sessions, targetID)
		s.mu.Unlock()
		close(session.closedCh)
		s.Logger.Info("fleetlet disconnected", "target_id", targetID)
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
		}
	}
}
