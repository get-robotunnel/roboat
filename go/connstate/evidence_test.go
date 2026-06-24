package connstate

import (
	"testing"
	"time"
)

func TestUpsertSessionPreservesEvidenceSeparation(t *testing.T) {
	resetEvidenceForTest()

	now := time.Date(2026, 3, 17, 4, 0, 0, 0, time.UTC)
	agent := EndpointProfile{
		PrivateAddrs:  []string{"192.168.1.10/24"},
		PublicIPHint:  "198.51.100.10",
		TransportCaps: []string{"stun_p2p", "turn_relay"},
		Source:        "agent_report",
		Confidence:    0.9,
	}
	cli := EndpointProfile{
		PrivateAddrs:  []string{"192.168.1.20/24"},
		TransportCaps: []string{"lan_tcp", "stun_p2p"},
		Source:        "cli_report",
		Confidence:    0.8,
	}
	obs := PlatformObservation{
		ObservedSrcIP: "203.0.113.5",
		ObservedVia:   "trusted_xff",
		ProxyTrusted:  true,
		BootstrapID:   "sess_123",
		Phase:         "plan_requested",
	}

	saved := UpsertSession(BootstrapSession{
		SessionID:    "sess_123",
		BootstrapID:  "sess_123",
		RobotID:      "robot_a",
		UserID:       "user_a",
		CLIID:        "cli_a",
		AgentProfile: &agent,
		CLIProfile:   &cli,
		Observation:  &obs,
	}, now)

	if saved.AgentProfile == nil || saved.CLIProfile == nil || saved.Observation == nil {
		t.Fatalf("expected complete evidence in stored session")
	}
	if saved.AgentProfile.Source != "agent_report" {
		t.Fatalf("unexpected agent source: %q", saved.AgentProfile.Source)
	}
	if saved.CLIProfile.Source != "cli_report" {
		t.Fatalf("unexpected cli source: %q", saved.CLIProfile.Source)
	}
	if saved.Observation.ObservedSrcIP != "203.0.113.5" {
		t.Fatalf("unexpected observed source IP: %q", saved.Observation.ObservedSrcIP)
	}

	snap, ok := SessionSnapshot("sess_123")
	if !ok {
		t.Fatalf("expected session snapshot")
	}
	if snap.AgentProfile.PublicIPHint != "198.51.100.10" {
		t.Fatalf("agent profile was not preserved")
	}
	if snap.CLIProfile.PrivateAddrs[0] != "192.168.1.20/24" {
		t.Fatalf("cli profile was not preserved")
	}
	if snap.Observation.ObservedVia != "trusted_xff" {
		t.Fatalf("observation channel was not preserved")
	}
}

func TestSessionRetentionPrunesStaleEntries(t *testing.T) {
	resetEvidenceForTest()

	t0 := time.Date(2026, 3, 17, 4, 0, 0, 0, time.UTC)
	_ = UpsertSession(BootstrapSession{
		SessionID:   "sess_old",
		BootstrapID: "sess_old",
		RobotID:     "robot_1",
	}, t0)

	t1 := t0.Add(sessionRetention + 2*time.Minute)
	_ = UpsertSession(BootstrapSession{
		SessionID:   "sess_new",
		BootstrapID: "sess_new",
		RobotID:     "robot_2",
	}, t1)

	if _, ok := SessionSnapshot("sess_old"); ok {
		t.Fatalf("expected stale session to be pruned")
	}
	if _, ok := SessionSnapshot("sess_new"); !ok {
		t.Fatalf("expected fresh session to remain")
	}
}
