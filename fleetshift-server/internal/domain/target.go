package domain

// TargetInfo describes a registered target.
type TargetInfo struct {
	ID         TargetID
	Name       string
	Labels     map[string]string
	Properties map[string]string
}
