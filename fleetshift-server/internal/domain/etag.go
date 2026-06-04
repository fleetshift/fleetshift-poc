package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// Etag returns a weak domain-state concurrency token (RFC 9110 Section
// 8.8.1) that changes whenever any API-visible field of the deployment
// view changes. The value is opaque, W/-prefixed, and quoted.
func (v DeploymentView) Etag() Etag {
	h := sha256.New()
	hashDeploymentFields(h, v)
	hashFulfillmentFields(h, v.Fulfillment)
	return weakEtag(h)
}

// Etag returns a weak domain-state concurrency token (RFC 9110 Section
// 8.8.1) that changes whenever any API-visible field of the managed
// resource view changes. The value is opaque, W/-prefixed, and quoted.
func (v ManagedResourceView) Etag() Etag {
	h := sha256.New()
	hashManagedResourceFields(h, v)
	hashFulfillmentFields(h, v.Fulfillment)
	return weakEtag(h)
}

func hashDeploymentFields(h hash.Hash, v DeploymentView) {
	hashString(h, string(v.Deployment.ID))
	hashString(h, v.Deployment.UID)
}

func hashManagedResourceFields(h hash.Hash, v ManagedResourceView) {
	hashString(h, string(v.ManagedResource.ResourceType))
	hashString(h, string(v.ManagedResource.Name))
	hashString(h, v.ManagedResource.UID)
	binary.Write(h, binary.BigEndian, int64(v.ManagedResource.CurrentVersion))
	binary.Write(h, binary.BigEndian, int64(v.Intent.Version))
	hashBytes(h, v.Intent.Spec)
}

func hashFulfillmentFields(h hash.Hash, f Fulfillment) {
	binary.Write(h, binary.BigEndian, int64(f.Generation))
	hashString(h, string(f.State))
	hashString(h, f.StatusReason)
	binary.Write(h, binary.BigEndian, int64(len(f.ResolvedTargets)))
	for _, t := range f.ResolvedTargets {
		hashString(h, string(t))
	}
}

// hashString writes len(s) as a big-endian int64 followed by the
// string bytes, making variable-length field boundaries unambiguous.
func hashString(h hash.Hash, s string) {
	binary.Write(h, binary.BigEndian, int64(len(s)))
	h.Write([]byte(s))
}

// hashBytes writes len(b) as a big-endian int64 followed by the raw
// bytes, making variable-length field boundaries unambiguous.
func hashBytes(h hash.Hash, b []byte) {
	binary.Write(h, binary.BigEndian, int64(len(b)))
	h.Write(b)
}

func weakEtag(h hash.Hash) Etag {
	sum := h.Sum(nil)
	return Etag(fmt.Sprintf(`W/"%x"`, sum[:16]))
}
