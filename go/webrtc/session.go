// Package webrtc implements the WebRTC signaling server for the RoboTunnel
// tunnel.
//
// Architecture:
//   - Robots (agents) connect to /api/signal/{robot_id}?role=agent via WebSocket
//     with X-Robot-API-Key header
//   - CLI/clients connect to /api/signal/{robot_id}?role=client via WebSocket with Authorization Bearer token
//   - This server relays SDP offers/answers and ICE candidates between them
//   - The actual media/data channel (STUN direct or TURN relay) is peer-to-peer
//     and does NOT pass through this server
//
// Connection strategy prioritised in the agent:
//  1. STUN (direct ICE) — no server relay, zero bandwidth cost
//  2. TURN relay — only if STUN fails (coturn on VPS)
//  3. TCP tunnel fallback — existing mechanism if WebRTC fails entirely
package webrtc

import (
	"log"
	"sync"
	"time"
)

// SignalMessage is the JSON envelope exchanged between peers via this signaling server.
// Both SDP and ICE candidates use this structure.
type SignalMessage struct {
	// Type: "offer", "answer", "ice-candidate", "ready", "bye"
	Type    string      `json:"type"`
	Payload interface{} `json:"payload,omitempty"`
	// RobotID identifies which robot session this message belongs to
	RobotID string `json:"robot_id"`
	// BootstrapID is the per-session correlation key propagated across components.
	BootstrapID string `json:"bootstrap_id,omitempty"`
}

// PeerConn represents one WebSocket connection (either agent or client side).
type PeerConn struct {
	send          chan SignalMessage
	done          chan struct{}
	once          sync.Once
	connectedAt   time.Time
	observedSrcIP string
	bootstrapID   string
}

func (p *PeerConn) closeDone() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.done)
	})
}

// Session holds the two sides of a WebRTC signaling exchange for one robot.
type Session struct {
	mu     sync.Mutex
	agent  *PeerConn // Robot agent connection
	client *PeerConn // CLI / browser connection
}

// relay forwards a message from one side to the other.
// Returns false if the destination is not connected.
func (s *Session) relay(msg SignalMessage, toAgent bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dest *PeerConn
	if toAgent {
		dest = s.agent
	} else {
		dest = s.client
	}

	if dest == nil {
		return false
	}

	select {
	case dest.send <- msg:
		return true
	default:
		// Destination buffer full — drop the message
		log.Printf("[webrtc] relay drop: robot=%s toAgent=%v", msg.RobotID, toAgent)
		return false
	}
}

// SessionRegistry manages all active signaling sessions.
type SessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

var registry = &SessionRegistry{
	sessions: make(map[string]*Session),
}

// GetOrCreate returns an existing session or creates a new empty one.
func (r *SessionRegistry) GetOrCreate(robotID string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.sessions[robotID]; ok {
		return s
	}
	s := &Session{}
	r.sessions[robotID] = s
	return s
}

// Remove deletes a session from the registry.
func (r *SessionRegistry) Remove(robotID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, robotID)
}

// Registry is the package-level signal session registry.
var Registry = registry
