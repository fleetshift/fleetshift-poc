package directkey

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestNativeHintsContainTheInnerAssertion(t *testing.T) {
	producer := newTestProducer(t)
	// Native parsing exposes opaque common content without decoding its body.
	want := protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: []byte("opaque, not JSON")}
	evidence, err := producer.CreateEvidence(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	hints, err := NewTarget().ParseHints(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if hints.Assertion.PredicateType != want.PredicateType || !bytes.Equal(hints.Assertion.Bytes, want.Bytes) {
		t.Fatalf("hint assertion = %+v, want %+v", hints.Assertion, want)
	}
	original := append([]byte(nil), evidence.Bytes...)
	hints.Assertion.Bytes[0] = 'X'
	if !bytes.Equal(evidence.Bytes, original) {
		t.Fatal("native output aliases its evidence input")
	}
}

func TestManagerHintsReturnTheSameInnerAssertion(t *testing.T) {
	producer := newTestProducer(t)
	manager := NewManager()
	enrollment, err := producer.CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Enroll(enrollment); err != nil {
		t.Fatal(err)
	}
	want := protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: []byte("opaque native content")}
	evidence, err := producer.CreateEvidence(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	for name, parse := range map[string]func(protocol.TypedEvidence) (protocol.TentativeHints, error){
		"ParseHints":    manager.ParseHints,
		"CheckDelivery": manager.CheckDelivery,
	} {
		t.Run(name, func(t *testing.T) {
			hints, err := parse(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if hints.Subject != producer.Principal().Subject || hints.Assertion.PredicateType != want.PredicateType || !bytes.Equal(hints.Assertion.Bytes, want.Bytes) {
				t.Fatalf("native hints=%+v", hints)
			}
			original := append([]byte(nil), evidence.Bytes...)
			hints.Assertion.Bytes[0] = 'X'
			if !bytes.Equal(evidence.Bytes, original) {
				t.Fatal("manager hints alias evidence")
			}
		})
	}
}

func TestEnrollmentHintsContainTheSuiteOwnedAssertion(t *testing.T) {
	producer := newTestProducer(t)
	evidence, err := producer.CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	hints, err := NewTarget().ParseHints(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if hints.Assertion.PredicateType != PredicateTypeEnrollmentV1 {
		t.Fatal("missing suite-owned purpose")
	}
	var assertion EnrollmentAssertion
	if err := json.Unmarshal(hints.Assertion.Bytes, &assertion); err != nil {
		t.Fatal(err)
	}
	if assertion.Principal != producer.Principal() || !bytes.Equal(assertion.PublicKey, producer.PublicKey()) {
		t.Fatalf("enrollment assertion=%+v", assertion)
	}
}
