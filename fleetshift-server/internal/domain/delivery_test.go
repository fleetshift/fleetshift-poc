package domain

import (
	"errors"
	"testing"
	"time"
)

func TestDeliveryState_IsTerminal(t *testing.T) {
	terminal := []DeliveryState{
		DeliveryStateDelivered,
		DeliveryStateFailed,
		DeliveryStatePartial,
		DeliveryStateAuthFailed,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%q: want terminal", s)
		}
	}

	nonTerminal := []DeliveryState{
		DeliveryStatePending,
		DeliveryStateAccepted,
		DeliveryStateProgressing,
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%q: want non-terminal", s)
		}
	}
}

func TestDelivery_TransitionTo(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	validTransitions := []struct {
		from, to DeliveryState
	}{
		{DeliveryStatePending, DeliveryStateAccepted},
		{DeliveryStatePending, DeliveryStateProgressing},
		{DeliveryStatePending, DeliveryStateFailed},
		{DeliveryStatePending, DeliveryStateAuthFailed},
		{DeliveryStateAccepted, DeliveryStateProgressing},
		{DeliveryStateAccepted, DeliveryStateDelivered},
		{DeliveryStateAccepted, DeliveryStateFailed},
		{DeliveryStateProgressing, DeliveryStateDelivered},
		{DeliveryStateProgressing, DeliveryStateFailed},
		{DeliveryStateProgressing, DeliveryStatePartial},
	}

	for _, tt := range validTransitions {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			d := Delivery{State: tt.from, CreatedAt: now, UpdatedAt: now}
			if err := d.TransitionTo(tt.to, later); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.State != tt.to {
				t.Errorf("State = %q, want %q", d.State, tt.to)
			}
			if d.UpdatedAt != later {
				t.Errorf("UpdatedAt = %v, want %v", d.UpdatedAt, later)
			}
		})
	}
}

func TestDelivery_TransitionTo_FromTerminal_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	terminalStates := []DeliveryState{
		DeliveryStateDelivered,
		DeliveryStateFailed,
		DeliveryStatePartial,
		DeliveryStateAuthFailed,
	}
	for _, from := range terminalStates {
		t.Run(string(from)+"->progressing", func(t *testing.T) {
			d := Delivery{State: from, CreatedAt: now, UpdatedAt: now}
			err := d.TransitionTo(DeliveryStateProgressing, later)
			if err == nil {
				t.Fatal("expected error for terminal -> non-terminal transition")
			}
			if !errors.Is(err, ErrIllegalStateTransition) {
				t.Errorf("error = %v, want ErrIllegalStateTransition", err)
			}
			if d.State != from {
				t.Errorf("State changed to %q, want %q (unchanged)", d.State, from)
			}
			if d.UpdatedAt != now {
				t.Error("UpdatedAt changed on failed transition")
			}
		})
	}
}

func TestDelivery_TransitionTo_NoOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := Delivery{State: DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now}
	if err := d.TransitionTo(DeliveryStateProgressing, later); err != nil {
		t.Fatalf("same-state transition should be a no-op, got: %v", err)
	}
	if d.UpdatedAt != now {
		t.Error("UpdatedAt changed on no-op transition")
	}
}

func TestDelivery_TransitionTo_UnknownState_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := Delivery{State: DeliveryStatePending, CreatedAt: now, UpdatedAt: now}
	err := d.TransitionTo(DeliveryState("bogus"), later)
	if err == nil {
		t.Fatal("expected error for unknown target state")
	}
	if !errors.Is(err, ErrIllegalStateTransition) {
		t.Errorf("error = %v, want ErrIllegalStateTransition", err)
	}
	if d.State != DeliveryStatePending {
		t.Errorf("State changed to %q, want %q (unchanged)", d.State, DeliveryStatePending)
	}
	if d.UpdatedAt != now {
		t.Error("UpdatedAt changed on failed transition")
	}
}

func TestDelivery_TransitionTo_Backward_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := Delivery{State: DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now}
	err := d.TransitionTo(DeliveryStatePending, later)
	if err == nil {
		t.Fatal("expected error for backward transition")
	}
	if !errors.Is(err, ErrIllegalStateTransition) {
		t.Errorf("error = %v, want ErrIllegalStateTransition", err)
	}
}
