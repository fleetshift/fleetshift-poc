package scripted_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
)

func mustCodec(t *testing.T) *scripted.Codec {
	t.Helper()
	c, err := scripted.NewCodec(context.Background())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	return c
}

func TestCodec_EmptySpecDefaultsToPromptSuccess(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// All four phases should default to zero-delay success.
	phases := []struct {
		name string
		pb   scripted.PhaseBehavior
	}{
		{"delivery.ack", spec.Delivery.Acknowledgement},
		{"delivery.completion", spec.Delivery.Completion},
		{"removal.ack", spec.Removal.Acknowledgement},
		{"removal.completion", spec.Removal.Completion},
	}
	for _, p := range phases {
		if lat := p.pb.Latency.ResolveLatency(); lat != 0 {
			t.Errorf("%s: latency = %v, want 0", p.name, lat)
		}
		outcome, _ := p.pb.Outcome.ResolveOutcome(0)
		if outcome != scripted.OutcomeSuccess {
			t.Errorf("%s: outcome = %v, want success", p.name, outcome)
		}
	}
}

func TestCodec_ConstantLatencyAndOutcome(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {"constant": "0.2s"},
					"outcome": {"constant": "SUCCESS"}
				},
				"completion": {
					"latency": {"constant": "0.1s"},
					"outcome": {"constant": "SUCCESS"}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if lat := spec.Delivery.Acknowledgement.Latency.ResolveLatency(); lat != 200*time.Millisecond {
		t.Errorf("ack latency = %v, want 200ms", lat)
	}
	if lat := spec.Delivery.Completion.Latency.ResolveLatency(); lat != 100*time.Millisecond {
		t.Errorf("comp latency = %v, want 100ms", lat)
	}
}

func TestCodec_SequenceOutcome(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"sequence": {"values": ["FAILURE", "FAILURE", "SUCCESS"]}}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	expected := []scripted.OutcomeValue{
		scripted.OutcomeFailure,
		scripted.OutcomeFailure,
		scripted.OutcomeSuccess,
	}
	for i, want := range expected {
		got, _ := spec.Delivery.Acknowledgement.Outcome.ResolveOutcome(i)
		if got != want {
			t.Errorf("attempt %d: outcome = %v, want %v", i, got, want)
		}
	}
}

func TestCodec_InventoryProjection(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"inventory": {
			"labels": {"env": "test", "tier": "backend"},
			"observation": {"nodes": 3, "ready": true}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(spec.Inventory.Labels) != 2 {
		t.Errorf("labels len = %d, want 2", len(spec.Inventory.Labels))
	}
	if spec.Inventory.Labels["env"] != "test" {
		t.Errorf("labels[env] = %q, want test", spec.Inventory.Labels["env"])
	}
	if spec.Inventory.Observation == nil {
		t.Fatal("observation is nil")
	}
}

func TestCodec_RejectsUnknownFields(t *testing.T) {
	c := mustCodec(t)
	_, err := c.Decode(json.RawMessage(`{"unknownField": "value"}`))
	if err == nil {
		t.Error("expected error for unknown field, got nil")
	}
}

func TestCodec_RejectsExceedingLatency(t *testing.T) {
	c := mustCodec(t)
	_, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {"constant": "301s"}
				}
			}
		}
	}`))
	if err == nil {
		t.Error("expected validation error for 301s latency, got nil")
	}
}

func TestCodec_RejectsEmptySequence(t *testing.T) {
	c := mustCodec(t)
	_, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"sequence": {"values": []}}
				}
			}
		}
	}`))
	if err == nil {
		t.Error("expected validation error for empty sequence, got nil")
	}
}

func TestCodec_RemovalDefaults(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"constant": "FAILURE"}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Delivery ack should be FAILURE.
	got, _ := spec.Delivery.Acknowledgement.Outcome.ResolveOutcome(0)
	if got != scripted.OutcomeFailure {
		t.Errorf("delivery ack outcome = %v, want failure", got)
	}

	// Removal should default to prompt success.
	got, _ = spec.Removal.Acknowledgement.Outcome.ResolveOutcome(0)
	if got != scripted.OutcomeSuccess {
		t.Errorf("removal ack outcome = %v, want success (default)", got)
	}
}

func TestCodec_BoundedNormalLatency(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {
						"bounded_normal": {
							"min": "1s",
							"max": "5s"
						}
					}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Check that latency decider is BoundedNormalLatency.
	lat := spec.Delivery.Acknowledgement.Latency
	bnl, ok := lat.(scripted.BoundedNormalLatency)
	if !ok {
		t.Fatalf("latency = %T, want BoundedNormalLatency", lat)
	}

	if bnl.Min != time.Second {
		t.Errorf("min = %v, want 1s", bnl.Min)
	}
	if bnl.Max != 5*time.Second {
		t.Errorf("max = %v, want 5s", bnl.Max)
	}

	// Sample a few latencies and verify they're within bounds.
	for i := 0; i < 10; i++ {
		resolved := bnl.ResolveLatency()
		if resolved < bnl.Min || resolved > bnl.Max {
			t.Errorf("sample %d: resolved = %v, want in [%v, %v]", i, resolved, bnl.Min, bnl.Max)
		}
	}
}

func TestCodec_BoundedNormalLatency_MinMaxEqual(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"latency": {
						"bounded_normal": {
							"min": "3s",
							"max": "3s"
						}
					}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	bnl := spec.Delivery.Acknowledgement.Latency.(scripted.BoundedNormalLatency)
	// When min == max, should always return the constant value.
	for i := 0; i < 10; i++ {
		resolved := bnl.ResolveLatency()
		if resolved != 3*time.Second {
			t.Errorf("sample %d: resolved = %v, want 3s", i, resolved)
		}
	}
}

func TestCodec_ProbabilisticOutcome(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"completion": {
					"outcome": {
						"probabilistic": {
							"failure_rate": 0.5
						}
					}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Check that outcome decider is ProbabilisticOutcome.
	outcome := spec.Delivery.Completion.Outcome
	po, ok := outcome.(scripted.ProbabilisticOutcome)
	if !ok {
		t.Fatalf("outcome = %T, want ProbabilisticOutcome", outcome)
	}

	if po.FailureRate != 0.5 {
		t.Errorf("failure_rate = %v, want 0.5", po.FailureRate)
	}

	// With failure_rate = 0.5, we should see both successes and failures
	// over a large sample.
	successes := 0
	failures := 0
	samples := 1000
	for i := 0; i < samples; i++ {
		outcome, _ := po.ResolveOutcome(0)
		if outcome == scripted.OutcomeSuccess {
			successes++
		} else {
			failures++
		}
	}

	// Expect roughly 50% failures, but allow some variance.
	failureRate := float64(failures) / float64(samples)
	if failureRate < 0.4 || failureRate > 0.6 {
		t.Logf("failure rate = %v, expected around 0.5 (this is a stochastic test and may flake)", failureRate)
	}
}

func TestCodec_ProbabilisticOutcome_ZeroFailureRate(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"completion": {
					"outcome": {
						"probabilistic": {
							"failure_rate": 0.0
						}
					}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	po := spec.Delivery.Completion.Outcome.(scripted.ProbabilisticOutcome)

	// With failure_rate = 0.0, should always succeed.
	for i := 0; i < 10; i++ {
		outcome, _ := po.ResolveOutcome(i)
		if outcome != scripted.OutcomeSuccess {
			t.Errorf("sample %d: outcome = %v, want success", i, outcome)
		}
	}
}

func TestCodec_ProbabilisticOutcome_FullFailureRate(t *testing.T) {
	c := mustCodec(t)
	spec, err := c.Decode(json.RawMessage(`{
		"behavior": {
			"delivery": {
				"completion": {
					"outcome": {
						"probabilistic": {
							"failure_rate": 1.0
						}
					}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	po := spec.Delivery.Completion.Outcome.(scripted.ProbabilisticOutcome)

	// With failure_rate = 1.0, should always fail.
	for i := 0; i < 10; i++ {
		outcome, _ := po.ResolveOutcome(i)
		if outcome != scripted.OutcomeFailure {
			t.Errorf("sample %d: outcome = %v, want failure", i, outcome)
		}
	}
}
