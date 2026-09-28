package protocol

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestUnverifiedTimestampBindingIdentityUsesDomainTagAndLengthDelimiting(t *testing.T) {
	binding := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("token-bytes"),
		Message: []byte("message-bytes"),
	}
	got, err := binding.Identity()
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	if got == "" {
		t.Fatal("Identity returned an empty digest")
	}

	want := digestLengthDelimited(purposeTimestampBindingIdentity, []byte(binding.Format), binding.Token, binding.Message)
	if got != want {
		t.Fatalf("Identity = %q, want domain-separated length-delimited digest %q", got, want)
	}

	jsonDigest, err := DigestObject(purposeTimestampBindingIdentity, binding)
	if err != nil {
		t.Fatalf("DigestObject: %v", err)
	}
	if got == jsonDigest {
		t.Fatal("Identity used JSON DigestObject encoding")
	}

	rawConcat := append(append([]byte(nil), binding.Token...), binding.Message...)
	if got == DigestBytes(rawConcat) {
		t.Fatal("Identity hashed raw token||message without a domain tag or length delimiting")
	}
}

func TestUnverifiedTimestampBindingIdentityIsSensitiveToExactBytes(t *testing.T) {
	base := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("ab"),
		Message: []byte("c"),
	}
	baseID, err := base.Identity()
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}

	split := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("a"),
		Message: []byte("bc"),
	}
	splitID, err := split.Identity()
	if err != nil {
		t.Fatalf("split Identity: %v", err)
	}
	if splitID == baseID {
		t.Fatal("length-delimited encoding did not distinguish token/message splits of the same concatenation")
	}

	tokenChanged := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("ad"),
		Message: []byte("c"),
	}
	tokenID, err := tokenChanged.Identity()
	if err != nil {
		t.Fatalf("token Identity: %v", err)
	}
	if tokenID == baseID {
		t.Fatal("Identity ignored an exact token-byte change")
	}

	messageChanged := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("ab"),
		Message: []byte("d"),
	}
	messageID, err := messageChanged.Identity()
	if err != nil {
		t.Fatalf("message Identity: %v", err)
	}
	if messageID == baseID {
		t.Fatal("Identity ignored an exact message-byte change")
	}
}

func TestUnverifiedTimestampBindingIdentityRejectsUnknownFormatAndSizeBounds(t *testing.T) {
	valid := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("token"),
		Message: []byte("message"),
	}
	if _, err := valid.Identity(); err != nil {
		t.Fatalf("valid Identity: %v", err)
	}

	for _, tc := range []struct {
		name    string
		binding UnverifiedTimestampBinding
	}{
		{
			name: "empty format",
			binding: UnverifiedTimestampBinding{
				Token:   []byte("token"),
				Message: []byte("message"),
			},
		},
		{
			name: "unknown format",
			binding: UnverifiedTimestampBinding{
				Format:  "unknown/v1",
				Token:   []byte("token"),
				Message: []byte("message"),
			},
		},
		{
			name: "oversized token",
			binding: UnverifiedTimestampBinding{
				Format:  TimestampFormatRFC3161V1,
				Token:   bytes.Repeat([]byte("t"), MaxTimestampTokenBytes+1),
				Message: []byte("message"),
			},
		},
		{
			name: "oversized message",
			binding: UnverifiedTimestampBinding{
				Format:  TimestampFormatRFC3161V1,
				Token:   []byte("token"),
				Message: bytes.Repeat([]byte("m"), MaxTimestampMessageBytes+1),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.binding.Identity()
			if !errors.Is(err, ErrInvalidTimestampBinding) {
				t.Fatalf("error = %v, want ErrInvalidTimestampBinding", err)
			}
			if got != "" {
				t.Fatalf("Identity = %q, want empty digest on closed-set/bounds failure", got)
			}
		})
	}
}

func TestUnverifiedTimestampBindingIdentityIsInsensitiveToLaterMutation(t *testing.T) {
	token := []byte("token-bytes")
	message := []byte("message-bytes")
	binding := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   token,
		Message: message,
	}
	cloned := binding.Clone()
	got, err := binding.Identity()
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}

	token[0] ^= 0xff
	message[0] ^= 0xff

	clonedID, err := cloned.Identity()
	if err != nil {
		t.Fatalf("clone Identity: %v", err)
	}
	if clonedID != got {
		t.Fatalf("clone Identity = %q, want %q after mutating the original slices", clonedID, got)
	}

	mutatedID, err := binding.Identity()
	if err != nil {
		t.Fatalf("mutated Identity: %v", err)
	}
	if mutatedID == got {
		t.Fatal("Identity of the mutated binding still matched the pre-mutation digest")
	}
}

func TestTimestampAuthorityClientIsIntentOnly(t *testing.T) {
	receipt := TimestampReceipt{
		Format: TimestampFormatRFC3161V1,
		Token:  []byte("rfc3161-token"),
	}
	if receipt.Format != TimestampFormatRFC3161V1 {
		t.Fatalf("Format = %q, want %s", receipt.Format, TimestampFormatRFC3161V1)
	}
	if string(receipt.Token) != "rfc3161-token" {
		t.Fatalf("Token = %q, want rfc3161-token", receipt.Token)
	}

	var client TimestampAuthorityClient = intentOnlyTimestampAuthority{}
	if _, err := client.Timestamp(context.Background(), []byte("message")); !errors.Is(err, errIntentOnlyTimestamp) {
		t.Fatalf("Timestamp error = %v, want intent-only sentinel", err)
	}
}

var errIntentOnlyTimestamp = errors.New("timestamp authority client is intent-only")

type intentOnlyTimestampAuthority struct{}

func (intentOnlyTimestampAuthority) Timestamp(context.Context, []byte) (TimestampReceipt, error) {
	return TimestampReceipt{}, errIntentOnlyTimestamp
}
