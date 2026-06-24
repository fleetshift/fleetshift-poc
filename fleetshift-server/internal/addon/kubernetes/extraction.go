package kubernetes

import (
	"maps"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/jsonpath"
)


// ExtractObservedResource converts an unstructured k8s resource and its schema
// entry into a domain InventoryItem and an inventoryNode.
func ExtractObservedResource(r *unstructured.Unstructured, entry SchemaEntry, targetID string) (domain.InventoryItem, inventoryNode) {
	uid := string(r.GetUID())

	// Build inventory type from apiVersion and kind.
	var invType domain.InventoryType
	parts := strings.SplitN(r.GetAPIVersion(), "/", 2)
	if len(parts) == 2 {
		invType = domain.InventoryType(parts[0] + "/" + parts[1] + "/" + r.GetKind())
	} else {
		invType = domain.InventoryType(parts[0] + "/" + r.GetKind())
	}

	// Labels.
	var labels map[string]string
	if l := r.GetLabels(); len(l) > 0 {
		labels = l
	}

	// Schema-defined observed fields plus base metadata fields.
	fields := make(map[string]any)

	// Conditions.
	conditions := extractConditions(r)

	// Extract schema-defined fields first
	if len(entry.Fields) > 0 {
		for _, f := range entry.Fields {
			v := extractSingleField(r, f)
			if v != nil {
				fields[f.Name] = v
			}
		}
	}

	// ownerReferences — extract controlling owner's UID
	ownerUID := ""
	if ownerRefs, found, _ := unstructured.NestedSlice(r.Object, "metadata", "ownerReferences"); found {
		for _, ref := range ownerRefs {
			if m, ok := ref.(map[string]any); ok {
				if ctrl, _ := m["controller"].(bool); ctrl {
					if uid, ok := m["uid"].(string); ok {
						ownerUID = uid
					}
				}
			}
		}
	}

	// namespace
	if ns := r.GetNamespace(); ns != "" {
		fields["namespace"] = ns
	}

	// controllingOwnerUID
	if ownerUID != "" {
		fields["controllingOwnerUID"] = ownerUID
	}

	// generation
	if gen, found, _ := unstructured.NestedInt64(r.Object, "metadata", "generation"); found {
		fields["generation"] = gen
	}

	// deletionTimestamp
	if dt, found, _ := unstructured.NestedString(r.Object, "metadata", "deletionTimestamp"); found && dt != "" {
		fields["deletionTimestamp"] = dt
	}

	// annotations
	if entry.ExtractAnnotations {
		if annotations := extractAnnotations(r, entry.AnnotationSizeCap); annotations != nil {
			fields["annotations"] = annotations
		}
	}

	// ComputeExtra hook invocation
	if entry.ComputeExtra != nil {
		entry.ComputeExtra(r, fields)
	}

	// Marshal observed fields to JSON
	var observed json.RawMessage
	if len(fields) > 0 {
		data, err := json.Marshal(fields)
		if err == nil {
			observed = data
		}
	}

	id := domain.InventoryItemID(targetID + "/" + uid)

	createdAt := r.GetCreationTimestamp().Time
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	item := domain.NewObservedInventoryItem(
		id, invType, r.GetName(),
		nil, // properties — not used by k8s extraction
		labels,
		domain.TargetID(targetID), observed,
		conditions, createdAt, time.Now(),
	)

	node := inventoryNode{
		UID:        uid,
		Kind:       r.GetKind(),
		Name:       r.GetName(),
		Namespace:  r.GetNamespace(),
		OwnerUID:   ownerUID,
		Labels:     labels,
		Properties: fields,
		GVR:        entry.GVR,
	}

	return item, node
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

// extractConditions reads .status.conditions from the unstructured object and
// returns domain InventoryCondition values.
func extractConditions(r *unstructured.Unstructured) []domain.InventoryCondition {
	raw, found, err := unstructured.NestedSlice(r.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}

	out := make([]domain.InventoryCondition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := domain.InventoryCondition{
			Type:    stringFromMap(m, "type"),
			Status:  stringFromMap(m, "status"),
			Reason:  stringFromMap(m, "reason"),
			Message: stringFromMap(m, "message"),
		}
		if ltt := stringFromMap(m, "lastTransitionTime"); ltt != "" {
			if t, err := time.Parse(time.RFC3339, ltt); err == nil {
				c.LastTransitionTime = &t
			}
		}
		out = append(out, c)
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
		return collectSlice(results)
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
		return coerceMapString(val)

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
