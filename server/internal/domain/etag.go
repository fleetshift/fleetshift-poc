package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"
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
// 8.8.1) that changes whenever any API-visible field of the extension
// resource view changes. The value is opaque, W/-prefixed, and quoted.
func (v ExtensionResourceView) Etag() Etag {
	h := sha256.New()
	hashExtensionResourceFields(h, v)
	if v.Fulfillment != nil {
		hashFulfillmentFields(h, *v.Fulfillment)
	}
	return weakEtag(h)
}

func hashExtensionResourceFields(h hash.Hash, v ExtensionResourceView) {
	hashString(h, string(v.Resource.resourceType))
	hashString(h, string(v.Resource.name))
	hashString(h, v.Resource.uid.String())
	hashStringStringMap(h, v.Resource.labels)
	if v.Resource.managed != nil {
		binary.Write(h, binary.BigEndian, int64(v.Resource.managed.currentVersion))
	}
	if v.Intent != nil {
		binary.Write(h, binary.BigEndian, int64(v.Intent.Version))
		hashBytes(h, v.Intent.Spec)
	}
	// TODO: If the capability is no longer active, we should either:
	// - Not return this from repository queries at all (ideal)
	// - Make etag type-aware to avoid etag changing for invisible reasons
	// (the data won't be returned at the transport layer,
	// so if considered for etag still, it can be surprising)
	if v.Resource.inventory != nil {
		// Wire local_labels / index_update_time / local_update_time /
		// conditions / observation — all API-visible when inventory
		// capability is declared.
		hashStringStringMap(h, v.Resource.inventory.labels)
		if v.Resource.inventory.observation != nil {
			hashBytes(h, *v.Resource.inventory.observation)
		}
		binary.Write(h, binary.BigEndian, int64(len(v.Resource.inventory.conditions)))
		for _, c := range v.Resource.inventory.conditions {
			hashString(h, string(c.conditionType))
			hashString(h, string(c.status))
			hashString(h, c.reason)
			hashString(h, c.message)
		}
		binary.Write(h, binary.BigEndian, v.Resource.inventory.observedAt.UnixNano())
		binary.Write(h, binary.BigEndian, v.Resource.inventory.updatedAt.UnixNano())
	}
	hashAliases(h, v.Resource.reportedAliases)
}

func hashDeploymentFields(h hash.Hash, v DeploymentView) {
	hashString(h, string(v.Deployment.name))
	hashString(h, v.Deployment.uid.String())
}

func hashFulfillmentFields(h hash.Hash, f Fulfillment) {
	binary.Write(h, binary.BigEndian, int64(f.generation))
	hashString(h, string(f.state))
	hashString(h, f.statusReason)
	binary.Write(h, binary.BigEndian, int64(len(f.resolvedTargets)))
	for _, t := range f.resolvedTargets {
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

func hashAliases(h hash.Hash, aliases AliasSet) {
	binary.Write(h, binary.BigEndian, int64(aliases.Len()))
	for alias := range aliases.All() {
		hashString(h, string(alias.Namespace()))
		hashString(h, string(alias.Key()))
		hashString(h, string(alias.Value()))
	}
}

// hashStringStringMap writes map entries in sorted key order so etag
// computation is independent of Go map iteration order.
func hashStringStringMap(h hash.Hash, m map[string]string) {
	binary.Write(h, binary.BigEndian, int64(len(m)))
	if len(m) == 0 {
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		hashString(h, k)
		hashString(h, m[k])
	}
}

func weakEtag(h hash.Hash) Etag {
	sum := h.Sum(nil)
	return Etag(fmt.Sprintf(`W/"%x"`, sum[:16]))
}
