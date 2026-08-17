package bootstrap

import "testing"

func TestAdvertisedUIOrigin(t *testing.T) {
	t.Run("uses configured origin", func(t *testing.T) {
		got, err := advertisedUIOrigin(Config{
			HTTPAddr: "127.0.0.1:8086",
			UIOrigin: "https://fleetshift-sandbox.localhost:8085",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != "https://fleetshift-sandbox.localhost:8085" {
			t.Fatalf("advertisedUIOrigin = %q", got)
		}
	})
	t.Run("falls back to derived origin", func(t *testing.T) {
		got, err := advertisedUIOrigin(Config{HTTPAddr: "127.0.0.1:8086"})
		if err != nil {
			t.Fatal(err)
		}
		if got != "http://127.0.0.1:8086" {
			t.Fatalf("advertisedUIOrigin = %q", got)
		}
	})
}
