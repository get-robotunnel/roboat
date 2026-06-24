package connstate

import (
	"testing"
	"time"
)

func resetStateForTest() {
	mu.Lock()
	defer mu.Unlock()
	state = map[string]RobotRuntimeState{}
}

func TestRuntimeStateUsesMonotonicLastSeen(t *testing.T) {
	resetStateForTest()

	robotID := "robot-1"
	t0 := time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(10 * time.Second)
	t2 := t0.Add(20 * time.Second)

	UpsertHeartbeat(robotID, "1.2.3.4", "192.168.1.9", "unknown", t1)
	// Out-of-order control update should not rewind canonical last_seen.
	UpsertControlConnected(robotID, "", "", "", t0)

	s, ok := Snapshot(robotID)
	if !ok {
		t.Fatalf("expected runtime snapshot")
	}
	if !s.ControlConnected {
		t.Fatalf("expected control_connected=true")
	}
	if !s.LastHeartbeatAt.Equal(t1) {
		t.Fatalf("unexpected last_heartbeat_at: got=%s want=%s", s.LastHeartbeatAt, t1)
	}
	if !s.LastSeenAt.Equal(t1) {
		t.Fatalf("unexpected last_seen_at after out-of-order update: got=%s want=%s", s.LastSeenAt, t1)
	}

	TouchControlActivity(robotID, t2)
	s, _ = Snapshot(robotID)
	if !s.LastSeenAt.Equal(t2) {
		t.Fatalf("touch control activity should advance last_seen_at: got=%s want=%s", s.LastSeenAt, t2)
	}

	// Older disconnect event must not rewind last_seen.
	SetControlDisconnected(robotID, t0)
	s, _ = Snapshot(robotID)
	if s.ControlConnected {
		t.Fatalf("expected control_connected=false after disconnect")
	}
	if !s.LastSeenAt.Equal(t2) {
		t.Fatalf("disconnect rewound last_seen_at: got=%s want=%s", s.LastSeenAt, t2)
	}
	if !s.ControlDisconnectedAt.Equal(t0) {
		t.Fatalf("expected control_disconnected_at to record disconnect time: got=%s want=%s", s.ControlDisconnectedAt, t0)
	}
}

func TestHeartbeatKeepsLatestTimestamp(t *testing.T) {
	resetStateForTest()

	robotID := "robot-2"
	newer := time.Date(2026, 3, 13, 11, 0, 0, 0, time.UTC)
	older := newer.Add(-15 * time.Second)

	UpsertHeartbeat(robotID, "10.0.0.1", "", "unknown", newer)
	UpsertHeartbeat(robotID, "", "", "", older)

	s, ok := Snapshot(robotID)
	if !ok {
		t.Fatalf("expected runtime snapshot")
	}
	if !s.LastHeartbeatAt.Equal(newer) {
		t.Fatalf("last_heartbeat_at regressed: got=%s want=%s", s.LastHeartbeatAt, newer)
	}
	if !s.LastSeenAt.Equal(newer) {
		t.Fatalf("last_seen_at regressed: got=%s want=%s", s.LastSeenAt, newer)
	}
	if s.RobotPublicIP != "10.0.0.1" {
		t.Fatalf("expected robot_public_ip to remain from latest known value")
	}
}

func TestIsOnlinePrefersFreshHeartbeat(t *testing.T) {
	now := time.Date(2026, 3, 13, 12, 0, 0, 0, time.UTC)

	s := RobotRuntimeState{
		ControlConnected: true,
		LastHeartbeatAt:  now.Add(-(heartbeatOnlineTTL - 2*time.Second)),
		LastSeenAt:       now.Add(-10 * time.Minute),
	}
	if !IsOnlineAt(s, now) {
		t.Fatalf("expected fresh heartbeat to keep robot online")
	}

	s.LastHeartbeatAt = now.Add(-(heartbeatOnlineTTL + 2*time.Second))
	if IsOnlineAt(s, now) {
		t.Fatalf("expected stale heartbeat to mark robot offline even when control is connected")
	}
}

func TestIsOnlineFallsBackToControlWhenNoHeartbeat(t *testing.T) {
	now := time.Date(2026, 3, 13, 12, 0, 0, 0, time.UTC)

	s := RobotRuntimeState{
		ControlConnected:   true,
		ControlConnectedAt: now.Add(-(controlFallbackTTL - 5*time.Second)),
		LastSeenAt:         now.Add(-(controlFallbackTTL - 5*time.Second)),
	}
	if !IsOnlineAt(s, now) {
		t.Fatalf("expected control fallback to keep robot online when no heartbeat exists")
	}

	s.LastSeenAt = now.Add(-(controlFallbackTTL + 5*time.Second))
	s.ControlConnectedAt = s.LastSeenAt
	if IsOnlineAt(s, now) {
		t.Fatalf("expected stale control fallback to mark robot offline")
	}
}

func TestIsOnlineDropsImmediatelyWhenControlDisconnectsAfterHeartbeat(t *testing.T) {
	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)

	s := RobotRuntimeState{
		ControlConnected:      false,
		ControlDisconnectedAt: now,
		LastHeartbeatAt:       now.Add(-2 * time.Second),
		LastSeenAt:            now,
	}
	if IsOnlineAt(s, now) {
		t.Fatalf("expected control disconnect to make online state converge immediately")
	}

	s.LastHeartbeatAt = now.Add(2 * time.Second)
	if !IsOnlineAt(s, now) {
		t.Fatalf("expected newer heartbeat after disconnect to restore online state")
	}
}

func TestRecordControlContentionPersistsLatestEvidence(t *testing.T) {
	resetStateForTest()

	robotID := "robot-contention-1"
	t0 := time.Date(2026, 3, 26, 14, 0, 0, 0, time.UTC)
	t1 := t0.Add(15 * time.Second)

	UpsertControlConnected(robotID, "180.164.139.2", "192.168.1.10", "unknown", t0)
	RecordControlContention(
		robotID,
		"116.105.225.251",
		"180.164.139.2",
		"reject_new",
		"competing_control_connection",
		t1,
	)

	s, ok := Snapshot(robotID)
	if !ok {
		t.Fatalf("expected runtime snapshot")
	}
	if !s.LastControlContentionAt.Equal(t1) {
		t.Fatalf("unexpected contention timestamp: got=%s want=%s", s.LastControlContentionAt, t1)
	}
	if s.LastControlContentionIncomingIP != "116.105.225.251" {
		t.Fatalf("unexpected incoming contention ip: %q", s.LastControlContentionIncomingIP)
	}
	if s.LastControlContentionIncumbentIP != "180.164.139.2" {
		t.Fatalf("unexpected incumbent contention ip: %q", s.LastControlContentionIncumbentIP)
	}
	if s.LastControlContentionPolicy != "reject_new" {
		t.Fatalf("unexpected contention policy: %q", s.LastControlContentionPolicy)
	}
	if s.LastControlContentionReason != "competing_control_connection" {
		t.Fatalf("unexpected contention reason: %q", s.LastControlContentionReason)
	}

	// Older contention evidence should not rewind timestamp.
	RecordControlContention(robotID, "1.1.1.1", "2.2.2.2", "reject_new", "older", t0)
	s, _ = Snapshot(robotID)
	if !s.LastControlContentionAt.Equal(t1) {
		t.Fatalf("contention timestamp regressed: got=%s want=%s", s.LastControlContentionAt, t1)
	}
}
