package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"time"
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
	h.Write([]byte(v.Deployment.ID))
	h.Write([]byte(v.Deployment.UID))
	hashTime(h, v.Deployment.CreatedAt)
	hashTime(h, v.Deployment.UpdatedAt)
}

func hashManagedResourceFields(h hash.Hash, v ManagedResourceView) {
	h.Write([]byte(v.ManagedResource.ResourceType))
	h.Write([]byte(v.ManagedResource.Name))
	h.Write([]byte(v.ManagedResource.UID))
	hashInt64(h, int64(v.ManagedResource.CurrentVersion))
	hashInt64(h, int64(v.Intent.Version))
	h.Write(v.Intent.Spec)
}

func hashFulfillmentFields(h hash.Hash, f Fulfillment) {
	hashInt64(h, int64(f.Generation))
	h.Write([]byte(f.State))
	h.Write([]byte(f.StatusReason))
	for _, t := range f.ResolvedTargets {
		h.Write([]byte(t))
	}
	hashTime(h, f.CreatedAt)
	hashTime(h, f.UpdatedAt)
}

func hashInt64(h hash.Hash, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	h.Write(buf[:])
}

func hashTime(h hash.Hash, t time.Time) {
	hashInt64(h, t.UnixNano())
}

func weakEtag(h hash.Hash) Etag {
	sum := h.Sum(nil)
	return Etag(fmt.Sprintf(`W/"%x"`, sum[:16]))
}
