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

func TestDelivery_Redispatch(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	t.Run("advances generation and resets state", func(t *testing.T) {
		d := Delivery{
			ID:         "d1",
			State:      DeliveryStateDelivered,
			Manifests:  []Manifest{{Raw: []byte("old")}},
			Generation: 1,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		newManifests := []Manifest{{Raw: []byte("new")}}
		if err := d.Redispatch(newManifests, 2, later); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
		if d.Generation != 2 {
			t.Errorf("Generation = %d, want 2", d.Generation)
		}
		if len(d.Manifests) != 1 || string(d.Manifests[0].Raw) != "new" {
			t.Errorf("Manifests not replaced")
		}
		if d.UpdatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.UpdatedAt, later)
		}
	})

	t.Run("same generation fails", func(t *testing.T) {
		d := Delivery{State: DeliveryStateDelivered, Generation: 3, UpdatedAt: now}
		err := d.Redispatch(nil, 3, later)
		if err == nil {
			t.Fatal("expected error for same generation")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
		if d.State != DeliveryStateDelivered {
			t.Errorf("State changed on failed redispatch")
		}
	})

	t.Run("lower generation fails", func(t *testing.T) {
		d := Delivery{State: DeliveryStateProgressing, Generation: 5, UpdatedAt: now}
		err := d.Redispatch(nil, 3, later)
		if err == nil {
			t.Fatal("expected error for lower generation")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
	})

	t.Run("works from non-terminal state", func(t *testing.T) {
		d := Delivery{State: DeliveryStateProgressing, Generation: 1, UpdatedAt: now}
		newManifests := []Manifest{{Raw: []byte("v2")}}
		if err := d.Redispatch(newManifests, 2, later); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
	})
}

func TestDelivery_Withdraw(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	t.Run("resets terminal delivery to pending", func(t *testing.T) {
		d := Delivery{
			ID:         "d1",
			State:      DeliveryStateDelivered,
			Manifests:  []Manifest{{Raw: []byte("keep")}},
			Generation: 5,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(7, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true")
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
		if d.Generation != 7 {
			t.Errorf("Generation = %d, want 7", d.Generation)
		}
		if len(d.Manifests) != 1 || string(d.Manifests[0].Raw) != "keep" {
			t.Error("Manifests changed during withdraw")
		}
		if d.UpdatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.UpdatedAt, later)
		}
	})

	t.Run("terminal same generation resets to pending", func(t *testing.T) {
		d := Delivery{
			State:      DeliveryStateDelivered,
			Generation: 5,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for same-gen terminal withdraw")
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
		if d.Generation != 5 {
			t.Errorf("Generation = %d, want 5", d.Generation)
		}
	})

	t.Run("pending same generation returns unmodified", func(t *testing.T) {
		d := Delivery{
			State:      DeliveryStatePending,
			Generation: 5,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if modified {
			t.Fatal("expected modified = false for same-gen pending")
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State changed unexpectedly to %q", d.State)
		}
		if d.UpdatedAt != now {
			t.Error("UpdatedAt changed on unmodified withdraw")
		}
	})

	t.Run("pending higher generation bumps generation", func(t *testing.T) {
		d := Delivery{
			State:      DeliveryStatePending,
			Generation: 3,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for higher-gen pending")
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
		if d.Generation != 5 {
			t.Errorf("Generation = %d, want 5", d.Generation)
		}
		if d.UpdatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.UpdatedAt, later)
		}
	})

	t.Run("in-progress same generation returns unmodified", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStateAccepted,
			DeliveryStateProgressing,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := Delivery{State: state, Generation: 5, UpdatedAt: now}
				modified, err := d.Withdraw(5, later)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if modified {
					t.Fatal("expected modified = false for same-gen in-progress")
				}
				if d.State != state {
					t.Errorf("State changed to %q", d.State)
				}
				if d.UpdatedAt != now {
					t.Error("UpdatedAt changed on unmodified withdraw")
				}
			})
		}
	})

	t.Run("in-progress higher generation resets to pending", func(t *testing.T) {
		d := Delivery{
			State:      DeliveryStateAccepted,
			Generation: 3,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for higher-gen in-progress")
		}
		if d.State != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State, DeliveryStatePending)
		}
		if d.Generation != 5 {
			t.Errorf("Generation = %d, want 5", d.Generation)
		}
	})

	t.Run("fails if generation moves backwards", func(t *testing.T) {
		d := Delivery{
			State:      DeliveryStateDelivered,
			Generation: 5,
			UpdatedAt:  now,
		}
		modified, err := d.Withdraw(3, later)
		if err == nil {
			t.Fatal("expected error for backwards generation")
		}
		if modified {
			t.Fatal("expected modified = false on error")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
		if d.Generation != 5 {
			t.Errorf("Generation changed to %d on failed withdraw", d.Generation)
		}
		if d.State != DeliveryStateDelivered {
			t.Errorf("State changed on failed withdraw")
		}
	})
}
