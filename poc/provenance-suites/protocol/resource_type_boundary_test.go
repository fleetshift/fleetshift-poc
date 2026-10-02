package protocol

import (
	"errors"
	"testing"
)

func TestCommonPredicateDecodingRequiresResourceType(t *testing.T) {
	for _, field := range []string{"", `,"resource_type":null`, `,"resource_type":""`, `,"resource_type":"clusters"`, `,"resource_type":"kind.example/v1/Cluster"`, `,"resource_type":42`} {
		for _, purpose := range []PredicateType{PredicateTypeManagedResourceV1, PredicateTypeFulfillmentRelationV1} {
			assertion := TypedAssertion{PredicateType: purpose, Bytes: []byte(`{"media_type":"application/json"` + field + `}`)}
			var err error
			if purpose == PredicateTypeManagedResourceV1 {
				_, err = DecodeManagedResourceAuthorization(assertion)
			} else {
				_, err = DecodeFulfillmentRelation(assertion)
			}
			if !errors.Is(err, ErrMalformedEvidence) {
				t.Errorf("purpose %s, field %q: %v", purpose, field, err)
			}
		}
	}
}
