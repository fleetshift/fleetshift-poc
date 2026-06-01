package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ManagedResourceService manages the lifecycle of managed resource
// instances: create, read, list, and delete. Spec validation is handled
// at the transport layer via protovalidate before reaching this service.
type ManagedResourceService struct {
	Store         domain.Store
	CreateWF      domain.CreateManagedResourceWorkflow
	DeleteWF      domain.DeleteManagedResourceWorkflow
	ResumeWF      domain.ResumeManagedResourceWorkflow
	ProvenanceSvc *domain.ProvenanceService
}

// CreateManagedResourceInput carries the fields needed to create a
// managed resource instance.
type CreateManagedResourceInput struct {
	ResourceType       domain.ResourceType
	Name               domain.ResourceName
	Spec               json.RawMessage
	Provenance         *domain.Provenance
	UserSignature      []byte
	ValidUntil         time.Time
	ExpectedGeneration domain.Generation
}

// Create persists a pre-validated managed resource, derives fulfillment
// strategies from the type's relation, and starts the create workflow.
// Spec validation is handled at the transport layer via protovalidate
// before reaching this method.
//
// NOTE: the application layer intentionally does not re-validate the
// spec against the schema. Doing so would couple this layer to proto
// descriptors and the transport's compilation pipeline. The E2E test
// bypasses the transport layer, so it does not exercise validation —
// that trade-off is accepted. Revisit if non-transport callers are
// added in production.
func (s *ManagedResourceService) Create(ctx context.Context, in CreateManagedResourceInput) (domain.ManagedResourceView, error) {
	if in.ResourceType == "" {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: resource type is required", domain.ErrInvalidArgument)
	}
	if in.Name == "" {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: name is required", domain.ErrInvalidArgument)
	}
	if len(in.Spec) == 0 {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: spec is required", domain.ErrInvalidArgument)
	}

	// Look up the type definition to get the fulfillment relation.
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	typeDef, err := tx.ManagedResources().GetType(ctx, in.ResourceType)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("lookup type %q: %w", in.ResourceType, err)
	}

	var prov *domain.Provenance
	if len(in.UserSignature) > 0 {
		ac := AuthFromContext(ctx)
		if ac == nil || ac.Subject == nil {
			return domain.ManagedResourceView{}, fmt.Errorf(
				"%w: signing a managed resource requires an authenticated caller",
				domain.ErrInvalidArgument,
			)
		}
		prov, err = s.ProvenanceSvc.BuildManagedResourceProvenance(
			ctx,
			tx.SignerEnrollments(),
			ac.Subject,
			in.ResourceType,
			in.Name,
			in.Spec,
			in.ExpectedGeneration,
			in.UserSignature,
			in.ValidUntil,
		)
		if err != nil {
			return domain.ManagedResourceView{}, fmt.Errorf("build provenance: %w", err)
		}
	} else if in.Provenance != nil {
		prov = in.Provenance
	}

	if err := tx.Commit(); err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("commit read tx: %w", err)
	}

	var auth domain.DeliveryAuth
	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	exec, err := s.CreateWF.Start(ctx, domain.CreateManagedResourceInput{
		ResourceType: in.ResourceType,
		Name:         in.Name,
		Spec:         in.Spec,
		TypeDef:      typeDef,
		Provenance:   prov,
		Auth:         auth,
	})
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("start create workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}

// Get retrieves a managed resource view by type and name.
func (s *ManagedResourceService) Get(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ManagedResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	view, err := tx.ManagedResources().GetView(ctx, rt, name)
	if err != nil {
		return domain.ManagedResourceView{}, err
	}
	return view, tx.Commit()
}

// List returns all managed resource views for a given type.
func (s *ManagedResourceService) List(ctx context.Context, rt domain.ResourceType) ([]domain.ManagedResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	views, err := tx.ManagedResources().ListViewsByType(ctx, rt)
	if err != nil {
		return nil, err
	}
	return views, tx.Commit()
}

// Delete starts the delete workflow for a managed resource.
func (s *ManagedResourceService) Delete(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ManagedResourceView, error) {
	var auth domain.DeliveryAuth
	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	exec, err := s.DeleteWF.Start(ctx, domain.DeleteManagedResourceInput{
		ResourceType: rt,
		Name:         name,
		Auth:         auth,
	})
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("start delete workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}

// ResumeManagedResourceInput carries the fields needed to resume a
// paused managed resource.
type ResumeManagedResourceInput struct {
	ResourceType  domain.ResourceType
	Name          domain.ResourceName
	UserSignature []byte
	ValidUntil    time.Time
}

// Resume resumes a managed resource that is paused for authentication
// by starting a durable resume-managed-resource workflow. The workflow
// updates auth/provenance, bumps the generation, and guarantees
// orchestration converges the resumed state.
func (s *ManagedResourceService) Resume(ctx context.Context, in ResumeManagedResourceInput) (domain.ManagedResourceView, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.ManagedResourceView{}, fmt.Errorf(
			"%w: resuming a managed resource requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	mr, err := tx.ManagedResources().GetInstance(ctx, in.ResourceType, in.Name)
	if err != nil {
		return domain.ManagedResourceView{}, err
	}
	f, err := tx.Fulfillments().Get(ctx, mr.FulfillmentID)
	if err != nil {
		return domain.ManagedResourceView{}, err
	}
	currentGen := f.Generation
	if err := tx.Commit(); err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.ResumeWF.Start(ctx, domain.ResumeManagedResourceInput{
		ResourceType: in.ResourceType,
		Name:         in.Name,
		Auth: domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		},
		UserSignature: in.UserSignature,
		ValidUntil:    in.ValidUntil,
	}, currentGen)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("start resume-managed-resource workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("resume-managed-resource workflow: %w", err)
	}

	return result, nil
}
