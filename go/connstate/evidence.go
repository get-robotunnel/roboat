package connstate

import (
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	sessionRetention = 30 * time.Minute
)

// LastKnownGoodRoute captures the most recent successful route evidence for an endpoint.
type LastKnownGoodRoute struct {
	RouteType    string   `json:"route_type,omitempty"`
	PublicIP     string   `json:"public_ip,omitempty"`
	PrivateAddrs []string `json:"private_addrs,omitempty"`
	ObservedAt   string   `json:"observed_at,omitempty"`
}

// EndpointProfile is endpoint self-reported network posture.
// It is source-attributed and never overwritten by platform observations.
type EndpointProfile struct {
	PrivateAddrs   []string            `json:"private_addrs,omitempty"`
	DefaultIface   string              `json:"default_iface,omitempty"`
	DefaultGateway string              `json:"default_gateway,omitempty"`
	PublicIPHint   string              `json:"public_ip_hint,omitempty"`
	TransportCaps  []string            `json:"transport_caps,omitempty"`
	NetworkTags    []string            `json:"network_tags,omitempty"`
	LastKnownGood  *LastKnownGoodRoute `json:"last_known_good,omitempty"`

	Source     string  `json:"source,omitempty"`
	ObservedAt string  `json:"observed_at,omitempty"`
	TTLSeconds int     `json:"ttl_seconds,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

// PlatformObservation is control-plane observed evidence.
// This must stay separate from self-reported endpoint profiles.
type PlatformObservation struct {
	ObservedSrcIP   string `json:"observed_src_ip,omitempty"`
	ObservedVia     string `json:"observed_via,omitempty"`
	ProxyTrusted    bool   `json:"proxy_trusted"`
	ConnectedAt     string `json:"connected_at,omitempty"`
	LastHeartbeatAt string `json:"last_heartbeat_at,omitempty"`
	CPRTTMs         int    `json:"cp_rtt_ms,omitempty"`
	WSUpgradeOK     bool   `json:"ws_upgrade_ok"`
	TurnCredFetchOK bool   `json:"turn_cred_fetch_ok"`
	BootstrapID     string `json:"bootstrap_id,omitempty"`
	Phase           string `json:"phase,omitempty"`
	PhaseElapsedMs  int    `json:"phase_elapsed_ms,omitempty"`
}

// BootstrapSession captures all evidence bound to a single connection attempt.
type BootstrapSession struct {
	SessionID   string `json:"session_id"`
	BootstrapID string `json:"bootstrap_id"`
	RobotID     string `json:"robot_id"`
	UserID      string `json:"user_id,omitempty"`
	CLIID       string `json:"cli_id,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`

	AgentProfile *EndpointProfile     `json:"agent_profile,omitempty"`
	CLIProfile   *EndpointProfile     `json:"cli_profile,omitempty"`
	Observation  *PlatformObservation `json:"platform_observation,omitempty"`

	Phase          string              `json:"phase,omitempty"`
	PhaseElapsedMs int                 `json:"phase_elapsed_ms,omitempty"`
	SelectedRoute  string              `json:"selected_route,omitempty"`
	WhyChosen      []string            `json:"why_chosen,omitempty"`
	WhyNot         map[string][]string `json:"why_not,omitempty"`
}

var (
	evidenceMu   sync.RWMutex
	agentProfile = map[string]EndpointProfile{}
	cliProfile   = map[string]EndpointProfile{}
	sessions     = map[string]BootstrapSession{}
)

func UpsertAgentProfile(robotID string, profile EndpointProfile, at time.Time) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return
	}
	profile = normalizeProfile(profile, at)

	evidenceMu.Lock()
	defer evidenceMu.Unlock()
	agentProfile[robotID] = profile
}

func AgentProfile(robotID string) (EndpointProfile, bool) {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return EndpointProfile{}, false
	}

	evidenceMu.RLock()
	defer evidenceMu.RUnlock()
	p, ok := agentProfile[robotID]
	if !ok {
		return EndpointProfile{}, false
	}
	return cloneProfile(p), true
}

func UpsertCLIProfile(cliID string, profile EndpointProfile, at time.Time) {
	cliID = strings.TrimSpace(cliID)
	if cliID == "" {
		return
	}
	profile = normalizeProfile(profile, at)

	evidenceMu.Lock()
	defer evidenceMu.Unlock()
	cliProfile[cliID] = profile
}

func CLIProfile(cliID string) (EndpointProfile, bool) {
	cliID = strings.TrimSpace(cliID)
	if cliID == "" {
		return EndpointProfile{}, false
	}

	evidenceMu.RLock()
	defer evidenceMu.RUnlock()
	p, ok := cliProfile[cliID]
	if !ok {
		return EndpointProfile{}, false
	}
	return cloneProfile(p), true
}

func UpsertSession(session BootstrapSession, at time.Time) BootstrapSession {
	sessionID := strings.TrimSpace(session.SessionID)
	if sessionID == "" {
		return BootstrapSession{}
	}
	now := normalizeAt(at)

	evidenceMu.Lock()
	defer evidenceMu.Unlock()
	pruneExpiredSessionsLocked(now)

	prev, ok := sessions[sessionID]
	if ok {
		session.StartedAt = firstNonEmpty(session.StartedAt, prev.StartedAt)
	}
	if strings.TrimSpace(session.StartedAt) == "" {
		session.StartedAt = now.Format(time.RFC3339)
	}
	session.UpdatedAt = now.Format(time.RFC3339)
	session.SessionID = sessionID
	session.BootstrapID = strings.TrimSpace(session.BootstrapID)
	session.RobotID = strings.TrimSpace(session.RobotID)
	session.UserID = strings.TrimSpace(session.UserID)
	session.CLIID = strings.TrimSpace(session.CLIID)
	session.Phase = strings.TrimSpace(session.Phase)
	session.SelectedRoute = strings.TrimSpace(session.SelectedRoute)
	session.WhyChosen = cleanStrings(session.WhyChosen)
	session.WhyNot = normalizeWhyNot(session.WhyNot)
	if session.AgentProfile != nil {
		p := normalizeProfile(*session.AgentProfile, now)
		session.AgentProfile = &p
	}
	if session.CLIProfile != nil {
		p := normalizeProfile(*session.CLIProfile, now)
		session.CLIProfile = &p
	}
	if session.Observation != nil {
		o := normalizeObservation(*session.Observation, now)
		session.Observation = &o
	}

	sessions[sessionID] = cloneSession(session)
	return cloneSession(session)
}

func SessionSnapshot(sessionID string) (BootstrapSession, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return BootstrapSession{}, false
	}

	evidenceMu.RLock()
	defer evidenceMu.RUnlock()
	s, ok := sessions[sessionID]
	if !ok {
		return BootstrapSession{}, false
	}
	return cloneSession(s), true
}

func resetEvidenceForTest() {
	evidenceMu.Lock()
	defer evidenceMu.Unlock()
	agentProfile = map[string]EndpointProfile{}
	cliProfile = map[string]EndpointProfile{}
	sessions = map[string]BootstrapSession{}
}

func pruneExpiredSessionsLocked(now time.Time) {
	cutoff := now.Add(-sessionRetention)
	for key, s := range sessions {
		ref := parseRFC3339Safe(s.UpdatedAt)
		if ref.IsZero() {
			ref = parseRFC3339Safe(s.StartedAt)
		}
		if !ref.IsZero() && ref.Before(cutoff) {
			delete(sessions, key)
		}
	}
}

func normalizeProfile(profile EndpointProfile, at time.Time) EndpointProfile {
	profile.PrivateAddrs = cleanStrings(profile.PrivateAddrs)
	profile.TransportCaps = cleanStrings(profile.TransportCaps)
	profile.NetworkTags = cleanStrings(profile.NetworkTags)
	profile.DefaultIface = strings.TrimSpace(profile.DefaultIface)
	profile.DefaultGateway = strings.TrimSpace(profile.DefaultGateway)
	profile.PublicIPHint = strings.TrimSpace(profile.PublicIPHint)
	profile.Source = strings.TrimSpace(profile.Source)
	profile.ObservedAt = firstNonEmpty(strings.TrimSpace(profile.ObservedAt), normalizeAt(at).Format(time.RFC3339))
	if profile.Confidence < 0 {
		profile.Confidence = 0
	}
	if profile.Confidence > 1 {
		profile.Confidence = 1
	}
	if profile.TTLSeconds < 0 {
		profile.TTLSeconds = 0
	}
	if profile.LastKnownGood != nil {
		lkg := *profile.LastKnownGood
		lkg.RouteType = strings.TrimSpace(lkg.RouteType)
		lkg.PublicIP = strings.TrimSpace(lkg.PublicIP)
		lkg.PrivateAddrs = cleanStrings(lkg.PrivateAddrs)
		lkg.ObservedAt = strings.TrimSpace(lkg.ObservedAt)
		profile.LastKnownGood = &lkg
	}
	return profile
}

func normalizeObservation(obs PlatformObservation, at time.Time) PlatformObservation {
	obs.ObservedSrcIP = strings.TrimSpace(obs.ObservedSrcIP)
	obs.ObservedVia = strings.TrimSpace(obs.ObservedVia)
	obs.ConnectedAt = firstNonEmpty(strings.TrimSpace(obs.ConnectedAt), normalizeAt(at).Format(time.RFC3339))
	obs.LastHeartbeatAt = strings.TrimSpace(obs.LastHeartbeatAt)
	obs.BootstrapID = strings.TrimSpace(obs.BootstrapID)
	obs.Phase = strings.TrimSpace(obs.Phase)
	if obs.CPRTTMs < 0 {
		obs.CPRTTMs = 0
	}
	if obs.PhaseElapsedMs < 0 {
		obs.PhaseElapsedMs = 0
	}
	return obs
}

func normalizeWhyNot(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for route, reasons := range in {
		route = strings.TrimSpace(route)
		if route == "" {
			continue
		}
		clean := cleanStrings(reasons)
		if len(clean) == 0 {
			continue
		}
		out[route] = clean
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cleanStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func parseRFC3339Safe(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cloneProfile(in EndpointProfile) EndpointProfile {
	out := in
	if len(in.PrivateAddrs) > 0 {
		out.PrivateAddrs = append([]string(nil), in.PrivateAddrs...)
	}
	if len(in.TransportCaps) > 0 {
		out.TransportCaps = append([]string(nil), in.TransportCaps...)
	}
	if len(in.NetworkTags) > 0 {
		out.NetworkTags = append([]string(nil), in.NetworkTags...)
	}
	if in.LastKnownGood != nil {
		lkg := *in.LastKnownGood
		if len(lkg.PrivateAddrs) > 0 {
			lkg.PrivateAddrs = append([]string(nil), lkg.PrivateAddrs...)
		}
		out.LastKnownGood = &lkg
	}
	return out
}

func cloneSession(in BootstrapSession) BootstrapSession {
	out := in
	if in.AgentProfile != nil {
		p := cloneProfile(*in.AgentProfile)
		out.AgentProfile = &p
	}
	if in.CLIProfile != nil {
		p := cloneProfile(*in.CLIProfile)
		out.CLIProfile = &p
	}
	if in.Observation != nil {
		o := *in.Observation
		out.Observation = &o
	}
	if len(in.WhyChosen) > 0 {
		out.WhyChosen = append([]string(nil), in.WhyChosen...)
	}
	if len(in.WhyNot) > 0 {
		out.WhyNot = make(map[string][]string, len(in.WhyNot))
		for k, v := range in.WhyNot {
			out.WhyNot[k] = append([]string(nil), v...)
		}
	}
	return out
}
