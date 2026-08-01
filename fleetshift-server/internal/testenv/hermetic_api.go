package testenv

import (
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Hermetic-api profile constants.
const (
	ProfileHermeticAPI = "hermetic-api"

	HermeticAddonID       domain.AddonID      = "hermetic.fleetshift.io"
	HermeticTargetType    domain.TargetType   = "hermetic-api"
	HermeticTargetID      domain.TargetID     = "hermetic-1"
	HermeticManifestType  domain.ManifestType = "hermetic.resource"
	HermeticInventoryType domain.ResourceType = "hermetic.fleetshift.io/Widget"
)

// HermeticCapabilities is the capability set published after hermetic-api
// readiness. Only these proven capabilities are advertised for the profile.
var HermeticCapabilities = []string{
	// Shared-cache in-memory SQLite via the production opener
	"sqlite-memory",
	// In-process memworkflow.Registry — serialization/concurrency only
	"memworkflow",
	// Fake delivery agent driven by testenv.Delivery (gate/fail/progress)
	"scripted-delivery",
	// Inventory state injected through testenv.Inventory for the hermetic Widget schema
	"controlled-inventory",
	// oidctest programmatic tokens and JWKS (IssueToken)
	"programmatic-identity",
}

// hermeticDescriptor returns the fake add-on descriptor for hermetic-api.
func hermeticDescriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   HermeticAddonID,
		Name: "Hermetic API Fake Add-on",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: HermeticTargetType},
			domain.InventoryResourceCapability{ResourceType: HermeticInventoryType},
		},
	}
}

// hermeticInventorySchema returns the controlled-inventory extension schema.
func hermeticInventorySchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: HermeticInventoryType,
		ProtoPackage: "hermetic.fleetshift.v1",
		Version:      "v1",
		CollectionID: "widgets",
		Singular:     "Widget",
		Plural:       "Widgets",
		Inventory:    &domain.InventorySchema{},
	}
}

// hermeticTarget returns the single scripted target published by hermetic-api.
func hermeticTarget() domain.TargetInfo {
	return domain.NewTargetInfo(
		HermeticTargetID,
		HermeticTargetType,
		"Hermetic Scripted Target",
		domain.TargetStateReady,
		nil,
		nil,
		[]domain.ManifestType{HermeticManifestType},
	)
}
