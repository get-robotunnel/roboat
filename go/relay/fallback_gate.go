package relay

import (
	"strings"
	"sync"
	"time"
)

const (
	fallbackGateRetention      = 20 * time.Minute
	bootstrapFailureBackoffMin = 500 * time.Millisecond
	bootstrapFailureBackoffMax = 8 * time.Second
	teardownFailureBackoffMin  = 1 * time.Second
	teardownFailureBackoffMax  = 12 * time.Second
)

type fallbackTriggerGate struct {
	mu     sync.Mutex
	states map[string]fallbackTriggerState
}

type fallbackTriggerState struct {
	nextAllowedAt       time.Time
	consecutiveFailures int
	lastSeenAt          time.Time
}

func newFallbackTriggerGate() *fallbackTriggerGate {
	return &fallbackTriggerGate{
		states: map[string]fallbackTriggerState{},
	}
}

func (g *fallbackTriggerGate) allow(robotID string, isTeardown bool, now time.Time) bool {
	if g == nil {
		return true
	}
	key := gateKey(robotID, isTeardown)
	if key == "" {
		return true
	}
	now = normalizeGateNow(now)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneLocked(now)

	state := g.states[key]
	state.lastSeenAt = now
	g.states[key] = state
	return state.nextAllowedAt.IsZero() || !state.nextAllowedAt.After(now)
}

func (g *fallbackTriggerGate) recordFailure(robotID string, isTeardown bool, now time.Time) {
	if g == nil {
		return
	}
	key := gateKey(robotID, isTeardown)
	if key == "" {
		return
	}
	now = normalizeGateNow(now)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneLocked(now)

	state := g.states[key]
	state.lastSeenAt = now
	state.consecutiveFailures++
	backoff := fallbackBackoff(state.consecutiveFailures, isTeardown)
	state.nextAllowedAt = now.Add(backoff)
	g.states[key] = state
}

func (g *fallbackTriggerGate) recordSuccess(robotID string, isTeardown bool, now time.Time) {
	if g == nil {
		return
	}
	key := gateKey(robotID, isTeardown)
	if key == "" {
		return
	}
	now = normalizeGateNow(now)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneLocked(now)

	state := g.states[key]
	state.lastSeenAt = now
	state.consecutiveFailures = 0
	state.nextAllowedAt = time.Time{}
	g.states[key] = state
}

func (g *fallbackTriggerGate) pruneLocked(now time.Time) {
	for key, state := range g.states {
		if state.lastSeenAt.IsZero() {
			delete(g.states, key)
			continue
		}
		if now.Sub(state.lastSeenAt) > fallbackGateRetention {
			delete(g.states, key)
		}
	}
}

func gateKey(robotID string, isTeardown bool) string {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return ""
	}
	if isTeardown {
		return robotID + ":teardown"
	}
	return robotID + ":bootstrap"
}

func normalizeGateNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

func fallbackBackoff(failures int, isTeardown bool) time.Duration {
	base := bootstrapFailureBackoffMin
	maxBackoff := bootstrapFailureBackoffMax
	if isTeardown {
		base = teardownFailureBackoffMin
		maxBackoff = teardownFailureBackoffMax
	}
	if failures <= 0 {
		return base
	}

	backoff := base
	steps := failures - 1
	if steps > 8 {
		steps = 8
	}
	for i := 0; i < steps; i++ {
		backoff *= 2
		if backoff >= maxBackoff {
			return maxBackoff
		}
	}
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}
