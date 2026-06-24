package connstate

import (
	"strings"
	"sync"
	"time"
)

// RobotRuntimeState stores transient CP runtime data.
// This data intentionally lives in memory only.
type RobotRuntimeState struct {
	RobotID               string    `json:"robot_id"`
	RobotPublicIP         string    `json:"robot_public_ip"`
	RobotLocalIP          string    `json:"robot_local_ip,omitempty"`
	RobotNATType          string    `json:"robot_nat_type"`
	ControlConnected      bool      `json:"control_connected"`
	ControlConnectedAt    time.Time `json:"control_connected_at,omitempty"`
	ControlDisconnectedAt time.Time `json:"control_disconnected_at,omitempty"`
	// LastControlContentionAt tracks the latest control-channel contention
	// event (for example duplicate robot_id attempting concurrent control
	// connect). This is observability only and does not drive routing.
	LastControlContentionAt          time.Time `json:"last_control_contention_at,omitempty"`
	LastControlContentionIncomingIP  string    `json:"last_control_contention_incoming_ip,omitempty"`
	LastControlContentionIncumbentIP string    `json:"last_control_contention_incumbent_ip,omitempty"`
	LastControlContentionPolicy      string    `json:"last_control_contention_policy,omitempty"`
	LastControlContentionReason      string    `json:"last_control_contention_reason,omitempty"`
	// LastHeartbeatAt tracks the latest heartbeat update from /api/heartbeat.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at,omitempty"`
	// LastSeenAt is the canonical liveness timestamp across heartbeat/control activity.
	LastSeenAt time.Time `json:"last_seen_at,omitempty"`
}

var (
	mu    sync.RWMutex
	state = map[string]RobotRuntimeState{}
)

const (
	// Heartbeat interval defaults to 30s on the agent, so online TTL must stay
	// comfortably above that interval to tolerate jitter and brief platform blips.
	heartbeatOnlineTTL = 75 * time.Second
	controlFallbackTTL = 90 * time.Second
)

func UpsertHeartbeat(robotID, robotPublicIP, robotLocalIP, natType string, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	at = normalizeAt(at)

	robotPublicIP = strings.TrimSpace(robotPublicIP)
	robotLocalIP = strings.TrimSpace(robotLocalIP)
	natType = strings.TrimSpace(natType)
	if natType == "" {
		natType = "unknown"
	}

	mu.Lock()
	defer mu.Unlock()

	prev := state[robotID]
	if robotPublicIP == "" {
		robotPublicIP = prev.RobotPublicIP
	}
	if robotLocalIP == "" {
		robotLocalIP = prev.RobotLocalIP
	}
	if natType == "" || natType == "unknown" {
		natType = prev.RobotNATType
		if natType == "" {
			natType = "unknown"
		}
	}

	state[robotID] = RobotRuntimeState{
		RobotID:                          robotID,
		RobotPublicIP:                    robotPublicIP,
		RobotLocalIP:                     robotLocalIP,
		RobotNATType:                     natType,
		ControlConnected:                 prev.ControlConnected,
		ControlConnectedAt:               prev.ControlConnectedAt,
		ControlDisconnectedAt:            prev.ControlDisconnectedAt,
		LastControlContentionAt:          prev.LastControlContentionAt,
		LastControlContentionIncomingIP:  prev.LastControlContentionIncomingIP,
		LastControlContentionIncumbentIP: prev.LastControlContentionIncumbentIP,
		LastControlContentionPolicy:      prev.LastControlContentionPolicy,
		LastControlContentionReason:      prev.LastControlContentionReason,
		LastHeartbeatAt:                  latestTime(prev.LastHeartbeatAt, at),
		LastSeenAt:                       latestTime(prev.LastSeenAt, at),
	}
}

func UpsertControlConnected(robotID, robotPublicIP, robotLocalIP, natType string, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	at = normalizeAt(at)

	robotPublicIP = strings.TrimSpace(robotPublicIP)
	robotLocalIP = strings.TrimSpace(robotLocalIP)
	natType = strings.TrimSpace(natType)
	if natType == "" {
		natType = "unknown"
	}

	mu.Lock()
	defer mu.Unlock()

	prev := state[robotID]
	if robotPublicIP == "" {
		robotPublicIP = prev.RobotPublicIP
	}
	if robotLocalIP == "" {
		robotLocalIP = prev.RobotLocalIP
	}
	if natType == "" || natType == "unknown" {
		natType = prev.RobotNATType
		if natType == "" {
			natType = "unknown"
		}
	}

	state[robotID] = RobotRuntimeState{
		RobotID:                          robotID,
		RobotPublicIP:                    robotPublicIP,
		RobotLocalIP:                     robotLocalIP,
		RobotNATType:                     natType,
		ControlConnected:                 true,
		ControlConnectedAt:               latestTime(prev.ControlConnectedAt, at),
		ControlDisconnectedAt:            time.Time{},
		LastControlContentionAt:          prev.LastControlContentionAt,
		LastControlContentionIncomingIP:  prev.LastControlContentionIncomingIP,
		LastControlContentionIncumbentIP: prev.LastControlContentionIncumbentIP,
		LastControlContentionPolicy:      prev.LastControlContentionPolicy,
		LastControlContentionReason:      prev.LastControlContentionReason,
		LastHeartbeatAt:                  prev.LastHeartbeatAt,
		LastSeenAt:                       latestTime(prev.LastSeenAt, at),
	}
}

func RecordControlContention(robotID, incomingIP, incumbentIP, policy, reason string, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	at = normalizeAt(at)

	mu.Lock()
	defer mu.Unlock()

	prev := state[robotID]
	if strings.TrimSpace(prev.RobotID) == "" {
		prev.RobotID = robotID
	}
	prev.LastControlContentionAt = latestTime(prev.LastControlContentionAt, at)
	prev.LastControlContentionIncomingIP = strings.TrimSpace(incomingIP)
	prev.LastControlContentionIncumbentIP = strings.TrimSpace(incumbentIP)
	prev.LastControlContentionPolicy = strings.TrimSpace(policy)
	prev.LastControlContentionReason = strings.TrimSpace(reason)
	prev.LastSeenAt = latestTime(prev.LastSeenAt, at)
	state[robotID] = prev
}

func Snapshot(robotID string) (RobotRuntimeState, bool) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return RobotRuntimeState{}, false
	}

	mu.RLock()
	defer mu.RUnlock()
	s, ok := state[robotID]
	return s, ok
}

// IsOnline returns runtime liveness using heartbeat-first semantics.
// If heartbeat has ever been observed, heartbeat freshness is canonical.
// Otherwise, control connectivity is used as a fallback signal.
func IsOnline(s RobotRuntimeState) bool {
	return IsOnlineAt(s, time.Now().UTC())
}

// IsOnlineAt evaluates liveness against a caller-provided timestamp.
func IsOnlineAt(s RobotRuntimeState, now time.Time) bool {
	now = normalizeAt(now)
	if !s.ControlConnected && !s.ControlDisconnectedAt.IsZero() {
		if s.LastHeartbeatAt.IsZero() || !s.LastHeartbeatAt.After(s.ControlDisconnectedAt) {
			return false
		}
	}
	if !s.LastHeartbeatAt.IsZero() {
		return now.Sub(s.LastHeartbeatAt) <= heartbeatOnlineTTL
	}
	if !s.ControlConnected {
		return false
	}

	ref := latestTime(s.ControlConnectedAt, s.LastSeenAt)
	if ref.IsZero() {
		return true
	}
	return now.Sub(ref) <= controlFallbackTTL
}

func TouchControlActivity(robotID string, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	at = normalizeAt(at)

	mu.Lock()
	defer mu.Unlock()

	prev := state[robotID]
	if strings.TrimSpace(prev.RobotID) == "" {
		prev.RobotID = robotID
	}
	if !prev.ControlConnected {
		prev.ControlConnected = true
		if prev.ControlConnectedAt.IsZero() {
			prev.ControlConnectedAt = at
		}
	}
	prev.ControlDisconnectedAt = time.Time{}
	prev.LastSeenAt = latestTime(prev.LastSeenAt, at)
	state[robotID] = prev
}

func SetControlDisconnected(robotID string, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	at = normalizeAt(at)

	mu.Lock()
	defer mu.Unlock()

	prev, ok := state[robotID]
	if !ok {
		return
	}
	prev.ControlConnected = false
	prev.ControlDisconnectedAt = latestTime(prev.ControlDisconnectedAt, at)
	prev.LastSeenAt = latestTime(prev.LastSeenAt, at)
	state[robotID] = prev
}

func normalizeAt(at time.Time) time.Time {
	if at.IsZero() {
		return time.Now().UTC()
	}
	return at.UTC()
}

func latestTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if b.After(a) {
		return b
	}
	return a
}
