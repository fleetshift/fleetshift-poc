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
	h.Write([]byte(v.Deployment.ID))
	h.Write([]byte(v.Deployment.UID))
}

func hashManagedResourceFields(h hash.Hash, v ManagedResourceView) {
	h.Write([]byte(v.ManagedResource.ResourceType))
	h.Write([]byte(v.ManagedResource.Name))
	h.Write([]byte(v.ManagedResource.UID))
	binary.Write(h, binary.BigEndian, int64(v.ManagedResource.CurrentVersion))
	binary.Write(h, binary.BigEndian, int64(v.Intent.Version))
	h.Write(v.Intent.Spec)
}

func hashFulfillmentFields(h hash.Hash, f Fulfillment) {
	binary.Write(h, binary.BigEndian, int64(f.Generation))
	h.Write([]byte(f.State))
	h.Write([]byte(f.StatusReason))
	for _, t := range f.ResolvedTargets {
		h.Write([]byte(t))
	}
}

func weakEtag(h hash.Hash) Etag {
	sum := h.Sum(nil)
	return Etag(fmt.Sprintf(`W/"%x"`, sum[:16]))
}
