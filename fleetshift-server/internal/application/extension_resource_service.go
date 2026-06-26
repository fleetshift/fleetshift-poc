package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ExtensionResourceService manages the lifecycle of extension resource
// instances: create, read, list, delete, and resume. Spec validation is
// handled at the transport layer via protovalidate before reaching this
// service.
//
// Type definitions are read from [domain.ExtensionResourceRepository],
// and management metadata (fulfillment relation, attestation) lives on
// the type rather than being a separate concept.
type ExtensionResourceService struct {
	store         domain.Store
	createWF      domain.CreateManagedResourceWorkflow
	deleteWF      domain.DeleteManagedResourceWorkflow
	resumeWF      domain.ResumeManagedResourceWorkflow
	provenanceSvc *domain.ProvenanceService
	now           func() time.Time
}

// ExtensionResourceServiceOption configures an
// [ExtensionResourceService].
type ExtensionResourceServiceOption func(*ExtensionResourceService)

// WithExtensionResourceClock overrides the wall-clock used for
// timestamps. Defaults to [time.Now].
func WithExtensionResourceClock(fn func() time.Time) ExtensionResourceServiceOption {
	return func(s *ExtensionResourceService) {
		if fn != nil {
			s.now = fn
		}
	}
}

// NewExtensionResourceService creates a service with the given store,
// workflows, and options.
func NewExtensionResourceService(
	store domain.Store,
	createWF domain.CreateManagedResourceWorkflow,
	deleteWF domain.DeleteManagedResourceWorkflow,
	resumeWF domain.ResumeManagedResourceWorkflow,
	provenanceSvc *domain.ProvenanceService,
	opts ...ExtensionResourceServiceOption,
) *ExtensionResourceService {
	s := &ExtensionResourceService{
		store:         store,
		createWF:      createWF,
		deleteWF:      deleteWF,
		resumeWF:      resumeWF,
		provenanceSvc: provenanceSvc,
		now:           time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreateExtensionResourceInput carries the fields needed to create an
// extension resource instance.
type CreateExtensionResourceInput struct {
	ResourceType  domain.ResourceType
	Name          domain.ResourceName
	Spec          json.RawMessage
	Provenance    *domain.Provenance
	UserSignature []byte
	ValidUntil    time.Time
}

// Create persists a pre-validated extension resource, derives
// fulfillment strategies from the type's management relation, and
// starts the create workflow. Spec validation is handled at the
// transport layer via protovalidate before reaching this method.
func (s *ExtensionResourceService) Create(ctx context.Context, in CreateExtensionResourceInput) (domain.ExtensionResourceView, error) {
	if len(in.Spec) == 0 {
		return domain.ExtensionResourceView{}, fmt.Errorf("%w: spec is required", domain.ErrInvalidArgument)
	}

	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	typeDef, err := tx.ExtensionResources().GetType(ctx, in.ResourceType)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("lookup type %q: %w", in.ResourceType, err)
	}

	mgmt := typeDef.Management()
	if mgmt == nil {
		return domain.ExtensionResourceView{}, fmt.Errorf(
			"%w: type %q has no management metadata; cannot create managed instance",
			domain.ErrInvalidArgument, in.ResourceType)
	}

	var prov *domain.Provenance
	if len(in.UserSignature) > 0 {
		ac := AuthFromContext(ctx)
		if ac == nil || ac.Subject == nil {
			return domain.ExtensionResourceView{}, fmt.Errorf(
				"%w: signing a resource requires an authenticated caller",
				domain.ErrInvalidArgument,
			)
		}
		prov, err = s.provenanceSvc.BuildManagedResourceProvenance(
			ctx,
			tx.SignerEnrollments(),
			ac.Subject,
			in.ResourceType,
			in.Name,
			in.Spec,
			1,
			in.UserSignature,
			in.ValidUntil,
		)
		if err != nil {
			return domain.ExtensionResourceView{}, fmt.Errorf("build provenance: %w", err)
		}
	} else if in.Provenance != nil {
		prov = in.Provenance
	}

	if err := tx.Commit(); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("commit read tx: %w", err)
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

	exec, err := s.createWF.Start(ctx, domain.CreateManagedResourceInput{
		ResourceType: in.ResourceType,
		Name:         in.Name,
		Spec:         in.Spec,
		TypeDef:      typeDef,
		Provenance:   prov,
		Auth:         auth,
	})
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("start create workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}

// Get retrieves an extension resource view by type and name.
func (s *ExtensionResourceService) Get(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ExtensionResourceView, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	view, err := tx.ExtensionResources().GetView(ctx, rt, name)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	return view, tx.Commit()
}

// List returns all extension resource views for a given type.
func (s *ExtensionResourceService) List(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	views, err := tx.ExtensionResources().ListViewsByType(ctx, rt)
	if err != nil {
		return nil, err
	}
	return views, tx.Commit()
}

// Delete starts the delete workflow for an extension resource.
func (s *ExtensionResourceService) Delete(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ExtensionResourceView, error) {
	var auth domain.DeliveryAuth
	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	typeDef, err := tx.ExtensionResources().GetType(ctx, rt)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("lookup type %q: %w", rt, err)
	}
	if typeDef.Management() == nil {
		return domain.ExtensionResourceView{}, fmt.Errorf(
			"%w: type %q has no management metadata; cannot delete managed instance",
			domain.ErrInvalidArgument, rt)
	}

	if err := tx.Commit(); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.deleteWF.Start(ctx, domain.DeleteManagedResourceInput{
		ResourceType: rt,
		Name:         name,
		Auth:         auth,
		TypeDef:      typeDef,
	})
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("start delete workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}

// ResumeExtensionResourceInput carries the fields needed to resume a
// paused extension resource.
type ResumeExtensionResourceInput struct {
	ResourceType       domain.ResourceType
	Name               domain.ResourceName
	UserSignature      []byte
	ValidUntil         time.Time
	Etag               domain.Etag
	ExpectedGeneration domain.Generation
}

// Resume resumes an extension resource that is paused for
// authentication by starting a durable resume workflow. The workflow
// updates auth/provenance, bumps the generation, and guarantees
// orchestration converges the resumed state.
func (s *ExtensionResourceService) Resume(ctx context.Context, in ResumeExtensionResourceInput) (domain.ExtensionResourceView, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.ExtensionResourceView{}, fmt.Errorf(
			"%w: resuming a resource requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	er, err := tx.ExtensionResources().Get(ctx, in.ResourceType, in.Name)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	managed := er.Managed()
	if managed == nil {
		return domain.ExtensionResourceView{}, fmt.Errorf(
			"%w: extension resource %s/%s has no managed state",
			domain.ErrInvalidArgument, in.ResourceType, in.Name)
	}
	f, err := tx.Fulfillments().Get(ctx, managed.FulfillmentID())
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	currentGen := f.Generation()
	if err := tx.Commit(); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.resumeWF.Start(ctx, domain.ResumeManagedResourceInput{
		ResourceType: in.ResourceType,
		Name:         in.Name,
		Auth: domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		},
		UserSignature:      in.UserSignature,
		ValidUntil:         in.ValidUntil,
		Etag:               in.Etag,
		ExpectedGeneration: in.ExpectedGeneration,
	}, currentGen)
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("start resume workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}
