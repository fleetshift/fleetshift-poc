package artifacts

import "strings"

var sensitiveKeys = []string{"password", "secret", "token", "credential", "key"}

// ScrubJSON recursively walks a JSON-like map and redacts values
// for keys matching sensitive patterns. Returns a new map (does not mutate input).
func ScrubJSON(data map[string]any) map[string]any {
	result := make(map[string]any, len(data))
	for k, v := range data {
		if isSensitiveKey(k) {
			result[k] = "[REDACTED]"
			continue
		}
		switch val := v.(type) {
		case map[string]any:
			result[k] = ScrubJSON(val)
		default:
			result[k] = v
		}
	}
	return result
}

func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	for _, s := range sensitiveKeys {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}
