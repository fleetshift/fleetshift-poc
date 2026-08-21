package scripted_test

import (
	"context"
	"testing"

	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func TestDescriptor_FixedIdentifiers(t *testing.T) {
	desc := scripted.Descriptor()
	if desc.ID != "scripted.fleetshift.io" {
		t.Errorf("ID = %q, want scripted.fleetshift.io", desc.ID)
	}
	if desc.Name != "Scripted Delivery Provider" {
		t.Errorf("Name = %q, want Scripted Delivery Provider", desc.Name)
	}
	if len(desc.Capabilities) != 3 {
		t.Fatalf("len(Capabilities) = %d, want 3", len(desc.Capabilities))
	}

	// Verify exact capability types and values.
	var hasDelivery, hasManaged, hasInventory bool
	for _, cap := range desc.Capabilities {
		switch c := cap.(type) {
		case domain.DeliveryCapability:
			if c.TargetType != "scripted" {
				t.Errorf("DeliveryCapability.TargetType = %q, want scripted", c.TargetType)
			}
			hasDelivery = true
		case domain.ManagedResourceCapability:
			if c.ResourceType != "scripted.fleetshift.io/ScriptedResource" {
				t.Errorf("ManagedResourceCapability.ResourceType = %q", c.ResourceType)
			}
			hasManaged = true
		case domain.InventoryResourceCapability:
			if c.ResourceType != "scripted.fleetshift.io/ScriptedResource" {
				t.Errorf("InventoryResourceCapability.ResourceType = %q", c.ResourceType)
			}
			hasInventory = true
		}
	}
	if !hasDelivery || !hasManaged || !hasInventory {
		t.Errorf("missing capabilities: delivery=%v managed=%v inventory=%v", hasDelivery, hasManaged, hasInventory)
	}
}

func TestSchema_FixedIdentifiers(t *testing.T) {
	schema := scripted.Schema()
	if schema.ResourceType != "scripted.fleetshift.io/ScriptedResource" {
		t.Errorf("ResourceType = %q", schema.ResourceType)
	}
	if schema.ProtoPackage != "addons.scripted.v1" {
		t.Errorf("ProtoPackage = %q", schema.ProtoPackage)
	}
	if schema.CollectionID != "scriptedResources" {
		t.Errorf("CollectionID = %q", schema.CollectionID)
	}
	if schema.Management == nil {
		t.Fatal("Management is nil")
	}
	if schema.Management.SpecMessage != "addons.scripted.v1.ScriptedResourceSpec" {
		t.Errorf("SpecMessage = %q", schema.Management.SpecMessage)
	}
	if schema.Inventory == nil {
		t.Error("Inventory is nil")
	}
}

func TestSchema_CompileInline(t *testing.T) {
	schema := scripted.Schema()
	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		schema.EntryFile,
		protoreflect.FullName(schema.Management.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	if desc.Message == nil {
		t.Fatal("message descriptor is nil")
	}
	if got := string(desc.Message.FullName()); got != "addons.scripted.v1.ScriptedResourceSpec" {
		t.Errorf("message full name = %q", got)
	}

	// Verify key fields exist.
	for _, field := range []string{"behavior", "inventory"} {
		if desc.Message.Fields().ByName(protoreflect.Name(field)) == nil {
			t.Errorf("field %q not found", field)
		}
	}
}

func TestSchema_ValidateAnnotations(t *testing.T) {
	schema := scripted.Schema()
	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		schema.EntryFile,
		protoreflect.FullName(schema.Management.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{
			name:    "empty spec is valid (all defaults to prompt success)",
			json:    `{}`,
			wantErr: false,
		},
		{
			name: "constant latency and outcome",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"latency": {"constant": "0.5s"},
							"outcome": {"constant": "SUCCESS"}
						}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "sequence outcome",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"outcome": {"sequence": {"values": ["FAILURE", "FAILURE", "SUCCESS"]}}
						}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "latency at upper bound (300s)",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"latency": {"constant": "300s"}
						}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "latency exceeds upper bound (301s)",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"latency": {"constant": "301s"}
						}
					}
				}
			}`,
			wantErr: true,
		},
		{
			name: "negative latency",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"latency": {"constant": "-1s"}
						}
					}
				}
			}`,
			wantErr: true,
		},
		{
			name: "empty sequence rejected",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"outcome": {"sequence": {"values": []}}
						}
					}
				}
			}`,
			wantErr: true,
		},
		{
			name: "unspecified outcome value in sequence rejected",
			json: `{
				"behavior": {
					"delivery": {
						"acknowledgement": {
							"outcome": {"sequence": {"values": ["OUTCOME_VALUE_UNSPECIFIED"]}}
						}
					}
				}
			}`,
			wantErr: true,
		},
		{
			name: "inventory with labels and observation",
			json: `{
				"inventory": {
					"labels": {"key": "value"},
					"observation": {"nodes": 3}
				}
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := dynamicpb.NewMessage(desc.Message)
			if err := protojson.Unmarshal([]byte(tt.json), msg); err != nil {
				if tt.wantErr {
					return // unmarshal rejection is acceptable
				}
				t.Fatalf("unmarshal: %v", err)
			}
			err := validator.Validate(msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("validate error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
