// Package fleetlet provides a [domain.DeliveryAgent] adapter that
// bridges the platform's delivery pipeline to remote fleetlets
// connected via gRPC.
package fleetlet

import (
	"context"
	"fmt"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

const TargetType domain.TargetType = "fleetlet"

// Agent implements [domain.DeliveryAgent] by forwarding delivery
// requests to a connected fleetlet via the FleetletService Deliver
// stream. Each delivery is correlated by delivery_id: the agent sends
// a DeliveryRequest and waits for the matching DeliveryCompleted event.
type Agent struct {
	Server *transportgrpc.FleetletServer
}

func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	session := a.Server.Session(target.ID)
	if session == nil {
		return domain.DeliveryResult{}, fmt.Errorf(
			"%w: no fleetlet connected for target %s",
			domain.ErrInvalidArgument, target.ID)
	}

	pbManifests := make([]*pb.Manifest, len(manifests))
	for i, m := range manifests {
		pbManifests[i] = &pb.Manifest{
			ResourceType: string(m.ResourceType),
			Raw:          m.Raw,
		}
	}

	req := &pb.DeliveryRequest{
		DeliveryId: string(deliveryID),
		TargetId:   string(target.ID),
		Manifests:  pbManifests,
	}

	eventCh, err := session.SendDeliveryRequest(req)
	if err != nil {
		return domain.DeliveryResult{}, fmt.Errorf("send delivery request: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return domain.DeliveryResult{}, ctx.Err()
		case evt, ok := <-eventCh:
			if !ok {
				return domain.DeliveryResult{
					State: domain.DeliveryStateDelivered,
				}, nil
			}

			if completed := evt.GetDeliveryCompleted(); completed != nil {
				return mapDeliveryResult(completed), nil
			}

			if signaler != nil {
				if progress := evt.GetDeliveryProgress(); progress != nil {
					signaler.Emit(ctx, domain.DeliveryEvent{
						Kind:    domain.DeliveryEventProgress,
						Message: progress.Message,
					})
				}
			}
		}
	}
}

func (a *Agent) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	session := a.Server.Session(target.ID)
	if session == nil {
		return fmt.Errorf(
			"%w: no fleetlet connected for target %s",
			domain.ErrInvalidArgument, target.ID)
	}

	req := &pb.RemoveRequest{
		DeliveryId: string(deliveryID),
		TargetId:   string(target.ID),
	}

	eventCh, err := session.SendRemoveRequest(req)
	if err != nil {
		return fmt.Errorf("send remove request: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-eventCh:
			if !ok {
				return nil
			}
			if rc := evt.GetRemoveCompleted(); rc != nil {
				if !rc.Success {
					return fmt.Errorf("remove failed: %s", rc.Message)
				}
				return nil
			}
		}
	}
}

func mapDeliveryResult(c *pb.DeliveryCompleted) domain.DeliveryResult {
	var state domain.DeliveryState
	switch c.State {
	case pb.DeliveryCompleted_STATE_DELIVERED:
		state = domain.DeliveryStateDelivered
	case pb.DeliveryCompleted_STATE_FAILED:
		state = domain.DeliveryStateFailed
	case pb.DeliveryCompleted_STATE_PARTIAL:
		state = domain.DeliveryStatePartial
	default:
		state = domain.DeliveryStateFailed
	}

	return domain.DeliveryResult{
		State:   state,
		Message: c.Message,
	}
}
