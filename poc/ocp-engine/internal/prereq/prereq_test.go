package prereq

import "testing"

func TestCheckBinary_Exists(t *testing.T) {
	if err := CheckBinary("ls"); err != nil {
		t.Errorf("CheckBinary(ls) should succeed: %v", err)
	}
}

func TestCheckBinary_NotExists(t *testing.T) {
	if err := CheckBinary("nonexistent-binary-xyz-12345"); err == nil {
		t.Error("CheckBinary should fail for nonexistent binary")
	}
}
