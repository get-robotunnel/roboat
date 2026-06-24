package relay

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// RobotResolver provides the data-backed lookups the relay needs. In the
// standalone tunnel it is backed by the tunnel identity store plus the ops
// internal API (which owns platform_token / robot ownership). The relay code
// never imports ops internals — it depends only on this interface.
type RobotResolver interface {
	// ResolveControl verifies a robot's control-plane credentials (X-Robot-API-Key).
	// Returns nil (no error) when the credentials are invalid.
	ResolveControl(robotID, apiKey string) (*AuthRecord, error)
	// TouchLastSeen records control-plane liveness for robotID.
	TouchLastSeen(robotID string) error
	// RelayUser extracts the authenticated user for a user-side relay request.
	RelayUser(c *gin.Context) (userID string, err error)
	// RobotOwnedBy reports whether robotID exists and is owned by userID.
	RobotOwnedBy(robotID, userID string) (bool, error)
	// RobotBootstrapTarget returns the robot's reachable host for the legacy TCP
	// bootstrap fallback (empty host disables the fallback).
	RobotBootstrapTarget(robotID string) (host string, err error)
}

// Server hosts the CP/DP relay: the agent control hub plus the agent
// control-plane, agent data-plane relay, and user relay endpoints.
type Server struct {
	hub          *AgentControlHub
	resolver     RobotResolver
	seed         []byte // ed25519 seed for platform->agent bootstrap auth
	fallbackGate *fallbackTriggerGate
	nowFn        func() time.Time
}

// NewServer builds a relay server with a fresh control hub.
func NewServer(resolver RobotResolver, seed []byte) *Server {
	return &Server{
		hub:          NewAgentControlHub(),
		resolver:     resolver,
		seed:         seed,
		fallbackGate: newFallbackTriggerGate(),
	}
}

// Hub exposes the control hub (used by the internal command API).
func (s *Server) Hub() *AgentControlHub { return s.hub }

// Routes registers the relay endpoints on the given gin engine.
func (s *Server) Routes(r *gin.Engine) {
	r.GET("/api/agent/connect", s.handleAgentControlConnect)
	r.GET("/v1/agent/connect", s.handleAgentControlConnect)
	r.GET("/api/agent/relay", s.handleAgentRelayConnect)
	r.GET("/v1/agent/relay", s.handleAgentRelayConnect)
	r.GET("/api/relay/ws", s.handleRelayWS)
}

func (s *Server) now() time.Time {
	if s != nil && s.nowFn != nil {
		return s.nowFn().UTC()
	}
	return time.Now().UTC()
}

// SetActiveSession is a passthrough used as the signaling SessionActivator.
func (s *Server) SetActiveSession(robotID, sessionKey string) {
	s.hub.SetActiveSession(robotID, sessionKey)
}

// TriggerBootstrap fires (or tears down) a WebRTC bootstrap on the agent: first
// over the active control channel, then falling back to the legacy TCP trigger.
// Its signature matches webrtc.BootstrapTrigger, so it can be wired directly as
// the signaling handler's trigger.
func (s *Server) TriggerBootstrap(robotID string, isTeardown bool, cliIP, bootstrapID, routeType string) error {
	sessionKey := strings.TrimSpace(bootstrapID)
	if sessionKey == "" {
		sessionKey = uuid.New().String()
	}
	now := s.now()

	if s.hub != nil {
		msg := agentControlMessage{
			BootstrapID: sessionKey,
			SessionKey:  sessionKey,
			RouteType:   strings.TrimSpace(routeType),
		}
		if isTeardown {
			msg.Type = "webrtc_teardown"
		} else {
			msg.Type = "webrtc_bootstrap"
			msg.CliPublicIP = cliIP
		}
		if s.hub.Send(robotID, msg) {
			if isTeardown {
				s.hub.ClearActiveSessionIfMatch(robotID, sessionKey)
			}
			if s.fallbackGate != nil {
				s.fallbackGate.recordSuccess(robotID, isTeardown, now)
			}
			return nil
		}
		if s.fallbackGate != nil && !s.fallbackGate.allow(robotID, isTeardown, now) {
			return nil
		}
		log.Printf("[control] no active control channel for robot=%s (type=%s session_key=%s), fallback to TCP trigger", robotID, msg.Type, sessionKey)
	}

	targetHost, err := s.resolver.RobotBootstrapTarget(robotID)
	if err != nil {
		return err
	}
	targetHost = strings.TrimSpace(targetHost)
	if targetHost == "" {
		return fmt.Errorf("robot has no IP for bootstrap trigger")
	}

	if isTeardown {
		err := TriggerWebRtcTeardown(targetHost, 11411, s.seed, WebRtcTeardownPayload{BootstrapID: sessionKey})
		if err == nil && s.hub != nil {
			s.hub.ClearActiveSessionIfMatch(robotID, sessionKey)
		}
		if s.fallbackGate != nil {
			if err == nil {
				s.fallbackGate.recordSuccess(robotID, true, now)
			} else {
				s.fallbackGate.recordFailure(robotID, true, now)
			}
		}
		return err
	}

	payload := WebRtcBootstrapPayload{
		BootstrapID: sessionKey,
		CliPublicIP: cliIP,
		RouteType:   strings.TrimSpace(routeType),
	}
	err = TriggerWebRtcBootstrap(targetHost, 11411, s.seed, payload)
	if s.fallbackGate != nil {
		if err == nil {
			s.fallbackGate.recordSuccess(robotID, false, now)
		} else {
			s.fallbackGate.recordFailure(robotID, false, now)
		}
	}
	return err
}
