package steps

import "testing"

// RunStep runs name as a subtest of t. If t has already failed, it registers
// a skipped subtest instead so later steps still appear in testdox output.
func RunStep(t *testing.T, name string, fn func(*testing.T)) {
	t.Helper()
	t.Run(name, func(st *testing.T) {
		if t.Failed() {
			st.Skip("earlier step failed")
			return
		}
		fn(st)
	})
}
