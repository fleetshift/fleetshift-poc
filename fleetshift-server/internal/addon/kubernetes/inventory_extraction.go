package kubernetes

import (
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/jsonpath"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ExtractObservedResource converts an unstructured Kubernetes object
// and its schema entry into an [InventoryObjectReport] plus an
// [inventoryNode] for in-memory edge computation.
//
// The report always uses [ObjectResourceType] identity: ResourceName,
// FleetShift labels, and observation come from [ObjectResourceName],
// [ObjectLabels], and [ObjectObservation]. Schema-defined fields,
// optional size-capped annotations, and [SchemaEntry.ComputeExtra]
// land in observation.extracted. Kubernetes object labels stay on the
// inventory node (for selector matching), not on the report.
func ExtractObservedResource(r *unstructured.Unstructured, entry SchemaEntry, targetID string) (InventoryObjectReport, inventoryNode, error) {
	uid := string(r.GetUID())
	observedAt := time.Now()

	id := KubernetesObjectIdentity{
		TargetID:  domain.TargetID(targetID),
		GVR:       entry.GVR,
		Kind:      r.GetKind(),
		Namespace: r.GetNamespace(),
		Name:      r.GetName(),
		UID:       uid,
	}

	name, err := ObjectResourceName(id)
	if err != nil {
		return InventoryObjectReport{}, inventoryNode{}, fmt.Errorf("extract observed resource: %w", err)
	}

	// ownerReferences — controlling owner's UID for edge computation.
	ownerUID := ""
	if ownerRefs, found, _ := unstructured.NestedSlice(r.Object, "metadata", "ownerReferences"); found {
		for _, ref := range ownerRefs {
			if m, ok := ref.(map[string]any); ok {
				if ctrl, _ := m["controller"].(bool); ctrl {
					if u, ok := m["uid"].(string); ok {
						ownerUID = u
					}
				}
			}
		}
	}

	// Schema-defined extracted fields (plus optional filtered
	// annotations and ComputeExtra). Base identity/metadata such as
	// namespace, generation, deletionTimestamp, and ownerReferences
	// live in [ObjectObservation]'s metadata, not here.
	extracted := make(map[string]any)

	for _, f := range entry.Fields {
		v := extractSingleField(r, f)
		if v != nil {
			extracted[f.Name] = v
		}
	}

	if entry.ExtractAnnotations {
		if annotations := extractAnnotations(r, entry.AnnotationSizeCap); annotations != nil {
			extracted["annotations"] = annotations
		}
	}

	if entry.ComputeExtra != nil {
		entry.ComputeExtra(r, extracted)
	}

	obs := ObjectObservation(id, r, extracted)
	conditions := ObjectConditions(extractRawConditions(r), observedAt)

	var k8sLabels map[string]string
	if l := r.GetLabels(); len(l) > 0 {
		k8sLabels = l
	}

	report := InventoryObjectReport{
		Name:        name,
		Labels:      ObjectLabels(id),
		Observation: &obs,
		Conditions:  conditions,
		ObservedAt:  observedAt,
	}

	node := inventoryNode{
		UID:        uid,
		Kind:       r.GetKind(),
		Name:       r.GetName(),
		Namespace:  r.GetNamespace(),
		OwnerUID:   ownerUID,
		Labels:     k8sLabels,
		Properties: extracted,
		GVR:        entry.GVR,
	}

	return report, node, nil
}

// extractAnnotations copies annotations from the resource, excluding
// kubectl.kubernetes.io/last-applied-configuration and any values
// longer than sizeCap characters. Returns nil if no annotations remain.
func extractAnnotations(r *unstructured.Unstructured, sizeCap int) map[string]string {
	raw := r.GetAnnotations()
	if len(raw) == 0 {
		return nil
	}

	// Default size cap
	if sizeCap <= 0 {
		sizeCap = 64
	}

	// Remove large kubectl annotation
	annotations := make(map[string]string, len(raw))
	maps.Copy(annotations, raw)

	delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")

	// Remove annotations exceeding size cap
	for key, val := range annotations {
		if len(val) > sizeCap {
			delete(annotations, key)
		}
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

// extractRawConditions reads .status.conditions from the unstructured
// object as [RawCondition] values for [ObjectConditions] projection.
func extractRawConditions(r *unstructured.Unstructured) []RawCondition {
	raw, found, err := unstructured.NestedSlice(r.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}

	out := make([]RawCondition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, RawCondition{
			Type:               stringFromMap(m, "type"),
			Status:             stringFromMap(m, "status"),
			Reason:             stringFromMap(m, "reason"),
			Message:            stringFromMap(m, "message"),
			LastTransitionTime: stringFromMap(m, "lastTransitionTime"),
		})
	}
	return out
}

// extractSingleField evaluates one field extraction against the resource.
func extractSingleField(r *unstructured.Unstructured, f FieldExtraction) any {
	// Normalize JSONPath: accept both ".spec.field" and "{.spec.field}".
	expr := strings.TrimSuffix(strings.TrimPrefix(f.JSONPath, "{"), "}")
	expr = "{" + expr + "}"

	jp := jsonpath.New(f.Name).AllowMissingKeys(true)
	if err := jp.Parse(expr); err != nil {
		return nil
	}

	results, err := jp.FindResults(r.Object)
	if err != nil || len(results) == 0 || len(results[0]) == 0 {
		return nil
	}

	// DataTypeSlice: collect all results into a list.
	if f.DataType == DataTypeSlice {
		// collectSlice returns []any; a typed-nil slice must not
		// escape as a non-nil any, or callers treating nil as
		// "skip this field" would incorrectly keep it.
		if items := collectSlice(results); items != nil {
			return items
		}
		return nil
	}

	val := results[0][0].Interface()

	switch f.DataType {
	case DataTypeBytes:
		s, ok := val.(string)
		if !ok {
			return nil
		}
		b, err := memoryToBytes(s)
		if err != nil {
			return nil
		}
		return float64(b)

	case DataTypeNumber:
		return coerceNumber(val)

	case DataTypeMapString:
		// Same typed-nil concern as DataTypeSlice: coerceMapString
		// returns map[string]any, so a nil map must be converted to
		// a true nil any before returning.
		if m := coerceMapString(val); m != nil {
			return m
		}
		return nil

	default:
		// Default / DataTypeString: return the value as-is.
		return val
	}
}

// collectSlice gathers all JSONPath results into a []any slice.
func collectSlice(results [][]reflect.Value) []any {
	var items []any
	for _, v := range results[0] {
		val := v.Interface()
		if slice, ok := val.([]any); ok {
			items = append(items, slice...)
		} else {
			items = append(items, val)
		}
	}
	if len(items) == 0 {
		return nil
	}
	return items
}

// coerceNumber converts a value to float64. Accepts int, int64,
// float64, and string (parsed as float64).
func coerceNumber(val any) any {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}
		return n
	default:
		return nil
	}
}

// coerceMapString converts a map[string]any to a map[string]any with
// string values.
func coerceMapString(val any) map[string]any {
	m, ok := val.(map[string]any)
	if !ok {
		return nil
	}
	fields := make(map[string]any, len(m))
	for k, v := range m {
		fields[k] = fmt.Sprint(v)
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// memoryToBytes parses a Kubernetes quantity string and returns the value in
// bytes (mirrors search-collector common.go:894-900).
func memoryToBytes(memory string) (int64, error) {
	quantity, err := resource.ParseQuantity(memory)
	if err != nil {
		return 0, err
	}
	return quantity.Value(), nil
}

// stringFromMap extracts a string value from a map, returning "" if the key is
// missing or the value is not a string.
func stringFromMap(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}
