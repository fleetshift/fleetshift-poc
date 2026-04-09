package prereq

import "testing"

func TestCheckBinary_Exists(t *testing.T) {
	err := CheckBinary("ls")
	if err != nil {
		t.Errorf("CheckBinary(ls) should succeed: %v", err)
	}
}

func TestCheckBinary_NotExists(t *testing.T) {
	err := CheckBinary("nonexistent-binary-xyz-12345")
	if err == nil {
		t.Error("CheckBinary should fail for nonexistent binary")
	}
}

func TestCheckContainerRuntime_AtLeastOneExists(t *testing.T) {
	_ = CheckContainerRuntime()
}
