package logpipeline

import "testing"

func TestParser_DetectsBootstrapComplete(t *testing.T) {
	p := NewParser()
	event := p.Parse(`level=info msg="It is now safe to remove the bootstrap resources"`)
	if event != "bootstrap_complete" {
		t.Errorf("event = %q, want %q", event, "bootstrap_complete")
	}
	if !p.BootstrapComplete() {
		t.Error("BootstrapComplete() = false, want true")
	}
}

func TestParser_DetectsInstallComplete(t *testing.T) {
	p := NewParser()
	event := p.Parse(`level=info msg="Install complete!"`)
	if event != "install_complete" {
		t.Errorf("event = %q, want %q", event, "install_complete")
	}
}

func TestParser_NoMatch(t *testing.T) {
	p := NewParser()
	event := p.Parse(`level=info msg="Creating VPC"`)
	if event != "" {
		t.Errorf("expected no event, got %q", event)
	}
}

func TestParser_BootstrapComplete_DefaultFalse(t *testing.T) {
	p := NewParser()
	if p.BootstrapComplete() {
		t.Error("BootstrapComplete() = true before any bootstrap log line")
	}
}

func TestParser_MilestoneFiresOnce(t *testing.T) {
	p := NewParser()
	event1 := p.Parse(`level=info msg="It is now safe to remove the bootstrap resources"`)
	event2 := p.Parse(`level=info msg="It is now safe to remove the bootstrap resources"`)
	if event1 != "bootstrap_complete" {
		t.Errorf("first parse: event = %q, want bootstrap_complete", event1)
	}
	if event2 != "" {
		t.Errorf("second parse: event = %q, want empty (already fired)", event2)
	}
}
