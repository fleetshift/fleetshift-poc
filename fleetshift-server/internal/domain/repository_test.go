package domain

import (
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
			name: "ReplaceLabels combined with DeleteLabels is rejected",
			delta: InventoryDelta{
				ReplaceLabels: map[string]string{"env": "prod"},
				DeleteLabels:  []string{"tier"},
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
