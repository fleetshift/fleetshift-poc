package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestValidateInventoryDelta(t *testing.T) {
	instanceID, err := NewAlias("gcp", "instance_id", "vm-1")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	instanceIDRef, err := NewAliasRef("gcp", "instance_id")
	if err != nil {
		t.Fatalf("NewAliasRef: %v", err)
	}
	ready, err := NewCondition("Ready", ConditionTrue, "AllGood", "ok", time.Unix(0, 0))
	if err != nil {
		t.Fatalf("NewCondition: %v", err)
	}

	tests := []struct {
		name    string
		delta   InventoryDelta
		wantErr error
	}{
		{
			name:  "empty heartbeat delta is valid",
			delta: InventoryDelta{},
		},
		{
			name: "UpsertAliases alone is valid",
			delta: InventoryDelta{
				UpsertAliases: NewAliasSet([]Alias{instanceID}),
			},
		},
		{
			name: "ReplaceLabels alone is valid",
			delta: InventoryDelta{
				ReplaceLabels: map[string]string{"env": "prod"},
			},
		},
		{
			name: "empty ReplaceLabels (clear all) is valid",
			delta: InventoryDelta{
				ReplaceLabels: map[string]string{},
			},
		},
		{
			name: "UpsertLabels alone is valid",
			delta: InventoryDelta{
				UpsertLabels: map[string]string{"env": "prod"},
			},
		},
		{
			name: "UpsertLabels combined with DeleteLabels for different keys is valid",
			delta: InventoryDelta{
				UpsertLabels: map[string]string{"env": "prod"},
				DeleteLabels: []string{"tier"},
			},
		},
		{
			name: "ReplaceLabels combined with DeleteLabels is rejected",
			delta: InventoryDelta{
				ReplaceLabels: map[string]string{"env": "prod"},
				DeleteLabels:  []string{"tier"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "ReplaceLabels combined with UpsertLabels is rejected",
			delta: InventoryDelta{
				ReplaceLabels: map[string]string{"env": "prod"},
				UpsertLabels:  map[string]string{"tier": "1"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "same label key in UpsertLabels and DeleteLabels is rejected",
			delta: InventoryDelta{
				UpsertLabels: map[string]string{"env": "prod"},
				DeleteLabels: []string{"env"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "same condition type in UpsertConditions and DeleteConditions is rejected",
			delta: InventoryDelta{
				UpsertConditions: []Condition{ready},
				DeleteConditions: []ConditionType{ready.Type()},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "ReplaceConditions alone is valid",
			delta: InventoryDelta{
				ReplaceConditions: []Condition{ready},
			},
		},
		{
			name: "empty ReplaceConditions (clear all) is valid",
			delta: InventoryDelta{
				ReplaceConditions: []Condition{},
			},
		},
		{
			name: "ReplaceConditions combined with UpsertConditions is rejected",
			delta: InventoryDelta{
				ReplaceConditions: []Condition{ready},
				UpsertConditions:  []Condition{ready},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "ReplaceConditions combined with DeleteConditions is rejected",
			delta: InventoryDelta{
				ReplaceConditions: []Condition{ready},
				DeleteConditions:  []ConditionType{ready.Type()},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "DeleteAliases alone is unimplemented",
			delta: InventoryDelta{
				DeleteAliases: []AliasRef{instanceIDRef},
			},
			wantErr: ErrUnimplemented,
		},
		{
			name: "DeleteAliases combined with UpsertAliases for the same key is still unimplemented, not the label/condition-style overlap error",
			delta: InventoryDelta{
				UpsertAliases: NewAliasSet([]Alias{instanceID}),
				DeleteAliases: []AliasRef{instanceIDRef},
			},
			wantErr: ErrUnimplemented,
		},
		{
			name: "ReplaceAliases alone is unimplemented",
			delta: InventoryDelta{
				ReplaceAliases: NewAliasSet([]Alias{instanceID}),
			},
			wantErr: ErrUnimplemented,
		},
		{
			name: "ReplaceAliases combined with UpsertAliases is still unimplemented",
			delta: InventoryDelta{
				ReplaceAliases: NewAliasSet([]Alias{instanceID}),
				UpsertAliases:  NewAliasSet([]Alias{instanceID}),
			},
			wantErr: ErrUnimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateInventoryDelta(tt.delta)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateInventoryDelta() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateInventoryDelta() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateInventoryReplacements_RejectsDeletePayloadAndDuplicates(t *testing.T) {
	name := ResourceName("nodes/n1")
	obs := json.RawMessage(`{"k":"v"}`)
	now := time.Unix(1, 0).UTC()
	alias, err := NewAlias("ns", "k", "v")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	ready, err := NewCondition("Ready", ConditionTrue, "AllGood", "ok", time.Unix(0, 0))
	if err != nil {
		t.Fatalf("NewCondition: %v", err)
	}

	cases := []struct {
		name string
		in   []InventoryReplacement
	}{
		{"missing type", []InventoryReplacement{{Name: name, IsDelete: true}}},
		{"missing name", []InventoryReplacement{{ResourceType: "inv.fleetshift.io/Node", IsDelete: true}}},
		{"aliases", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			Aliases: NewAliasSet([]Alias{alias}),
		}}},
		{"labels", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			Labels: map[string]string{"k": "v"},
		}}},
		{"observation", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			Observation: &obs,
		}}},
		{"conditions", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			Conditions: []Condition{ready},
		}}},
		{"candidate uid", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			CandidateUID: NewExtensionResourceUID(),
		}}},
		{"timestamps", []InventoryReplacement{{
			ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true,
			ObservedAt: now, ReceivedAt: now,
		}}},
		{"duplicate deletes", []InventoryReplacement{
			{ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true},
			{ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true},
		}},
		{"contradictory", []InventoryReplacement{
			{ResourceType: "inv.fleetshift.io/Node", Name: name, IsDelete: true},
			{ResourceType: "inv.fleetshift.io/Node", Name: name, CandidateUID: NewExtensionResourceUID(), ObservedAt: now, ReceivedAt: now},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateInventoryReplacements(tc.in); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestValidateInventoryReplacements_AcceptsMixedDistinctKeys(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	err := ValidateInventoryReplacements([]InventoryReplacement{
		{ResourceType: "inv.fleetshift.io/Node", Name: "nodes/a", IsDelete: true},
		{ResourceType: "inv.fleetshift.io/Node", Name: "nodes/b", CandidateUID: NewExtensionResourceUID(), ObservedAt: now, ReceivedAt: now},
	})
	if err != nil {
		t.Fatalf("ValidateInventoryReplacements: %v", err)
	}
}
