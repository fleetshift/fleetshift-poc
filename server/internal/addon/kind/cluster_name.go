package kind

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// kindClusterNamePrefix marks FleetShift-owned kind/docker cluster
// names. This is a naming convention, not cryptographic proof of
// ownership — an externally created name with this prefix may be
// adopted as ours.
const kindClusterNamePrefix = "fs--"

// maxKindClusterNameLen is kind's practical name length warning
// threshold. Longer names are rejected before calling the provider.
const maxKindClusterNameLen = 50

// kindResourceIDPattern matches Kind's cluster-name charset. Leading
// and trailing '.' / '-' are allowed (Kind accepts them; docker may
// still reject some edge cases at create time).
var kindResourceIDPattern = regexp.MustCompile(`^[a-z0-9.-]+$`)

// encodeKindClusterName returns the kind/docker cluster name for a
// platform resource ID. The platform [domain.ResourceName] remains
// canonical for APIs and inventory; only provider Create/Delete/
// KubeConfig/List matching use the encoded form.
func encodeKindClusterName(id domain.ResourceID) (string, error) {
	s := string(id)
	if s == "" {
		return "", fmt.Errorf("%w: resource id must not be empty", domain.ErrInvalidArgument)
	}
	if !kindResourceIDPattern.MatchString(s) {
		return "", fmt.Errorf("%w: resource id %q is not a valid kind cluster name segment (want lowercase [a-z0-9.-]+)", domain.ErrInvalidArgument, s)
	}
	name := kindClusterNamePrefix + s
	if len(name) > maxKindClusterNameLen {
		return "", fmt.Errorf("%w: encoded kind cluster name %q exceeds %d characters", domain.ErrInvalidArgument, name, maxKindClusterNameLen)
	}
	return name, nil
}

// decodeKindClusterName parses an ownership-encoded kind name.
// ok is false when name is not FleetShift-owned encoding.
func decodeKindClusterName(name string) (id domain.ResourceID, ok bool) {
	const prefix = kindClusterNamePrefix
	if len(name) <= len(prefix) || name[:len(prefix)] != prefix {
		return "", false
	}
	idPart := name[len(prefix):]
	if idPart == "" || !kindResourceIDPattern.MatchString(idPart) {
		return "", false
	}
	return domain.ResourceID(idPart), true
}

// findOwnedCluster returns the encoded kind name for resourceID if
// present in listed. listed may contain foreign names; only an exact
// encode match counts as owned.
func findOwnedCluster(listed []string, id domain.ResourceID) (kindName string, found bool) {
	want, err := encodeKindClusterName(id)
	if err != nil {
		return "", false
	}
	for _, n := range listed {
		if n == want {
			return want, true
		}
	}
	return "", false
}

// foreignClusterConflict reports whether listed contains the bare
// platform resource ID. An existing bare id means a non-FleetShift
// cluster occupies the natural name and must not be adopted or
// recreated.
func foreignClusterConflict(listed []string, id domain.ResourceID) bool {
	bare := string(id)
	return slices.Contains(listed, bare)
}
