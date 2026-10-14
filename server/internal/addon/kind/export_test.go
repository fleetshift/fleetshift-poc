package kind

// OverrideLoopbackForwardBin points Ensure at path for the rest of the test.
// Restore with the returned function (typically t.Cleanup).
func OverrideLoopbackForwardBin(path string) func() {
	prev := loopbackForwardBin
	loopbackForwardBin = path
	return func() { loopbackForwardBin = prev }
}
