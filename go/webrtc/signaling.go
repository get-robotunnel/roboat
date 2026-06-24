package webrtc

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/connauth"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 64 * 1024 // 64KB — ICE candidates and SDP are well within this
	// Attach semantics are exclusive per robot_id; newest client signaling
	// session preempts the previous one.
	webrtcSessionConcurrencyPolicy = "preempt_existing"
	webrtcPreemptDebounceMsEnv     = "ROBOTUNNEL_WEBRTC_PREEMPT_DEBOUNCE_MS"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type BootstrapTrigger func(robotID string, isTeardown bool, cliIP string, bootstrapID string, routeType string) error
type SessionActivator func(robotID string, bootstrapID string)

// SignalingHandler handles WebSocket connections for WebRTC signaling.
//
// URL: /api/signal/:robot_id?role=agent|client
//
// Auth (via the injected Authenticator):
//   - role=agent  -> header X-Robot-API-Key  (verified against tunnel identity)
//   - role=client -> header Authorization: Bearer <platform_token> (validated by ops)
func SignalingHandler(authn connauth.Authenticator, trigger BootstrapTrigger, activate SessionActivator) gin.HandlerFunc {
	return func(c *gin.Context) {
		robotID := strings.TrimSpace(c.Param("robot_id"))
		role := strings.TrimSpace(c.Query("role"))
		if robotID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id required"})
			return
		}
		if _, err := uuid.Parse(robotID); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id must be a valid UUID"})
			return
		}
		if role != "agent" && role != "client" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "role must be agent or client"})
			return
		}

		if role == "agent" {
			apiKey := strings.TrimSpace(c.GetHeader("X-Robot-API-Key"))
			if apiKey == "" {
				apiKey = strings.TrimSpace(c.Query("api_key"))
			}
			if apiKey == "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "api_key required for agent signaling"})
				return
			}
			ok, err := authn.VerifyRobotAPIKey(robotID, apiKey)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "auth lookup failed"})
				return
			}
			if !ok {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid robot credentials"})
				return
			}
		} else {
			if !authn.AuthorizeClient(c, robotID) {
				return // AuthorizeClient already wrote the error response
			}
		}

		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[webrtc] upgrade error: %v", err)
			return
		}

		bootstrapID := strings.TrimSpace(c.Query("bootstrap_id"))
		if bootstrapID == "" {
			bootstrapID = uuid.New().String()
		}
		routeType := strings.TrimSpace(c.Query("route_type"))
		incomingSrcIP := strings.TrimSpace(c.ClientIP())
		connectedAt := time.Now().UTC()
		debounceWindow := webrtcPreemptDebounceWindow()

		peer := &PeerConn{
			send:          make(chan SignalMessage, 32),
			done:          make(chan struct{}),
			connectedAt:   connectedAt,
			observedSrcIP: incomingSrcIP,
			bootstrapID:   bootstrapID,
		}

		session := Registry.GetOrCreate(robotID)
		var prev *PeerConn
		var counterpart *PeerConn
		rejectedByDebounce := false

		session.mu.Lock()
		if role == "agent" {
			prev = session.agent
			if shouldDebouncePeerReplacement(prev, peer, debounceWindow) {
				rejectedByDebounce = true
			} else {
				session.agent = peer
				counterpart = session.client
				log.Printf("[webrtc] agent connected: robot=%s [BOOTSTRAP:%s]", robotID, bootstrapID)
			}
		} else {
			prev = session.client
			if shouldDebouncePeerReplacement(prev, peer, debounceWindow) {
				rejectedByDebounce = true
			} else {
				session.client = peer
				counterpart = session.agent
				log.Printf("[webrtc] client connected: robot=%s [BOOTSTRAP:%s]", robotID, bootstrapID)
			}
		}
		session.mu.Unlock()
		if rejectedByDebounce {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "duplicate signaling reconnect debounced"),
				time.Now().Add(2*time.Second),
			)
			_ = conn.Close()
			if prev != nil {
				log.Printf(
					"[webrtc] debounced duplicate %s signaling peer: robot=%s [BOOTSTRAP:%s] incumbent_bootstrap=%s incoming_ip=%s incumbent_ip=%s window=%s",
					role,
					robotID,
					bootstrapID,
					strings.TrimSpace(prev.bootstrapID),
					incomingSrcIP,
					strings.TrimSpace(prev.observedSrcIP),
					debounceWindow,
				)
			} else {
				log.Printf(
					"[webrtc] debounced duplicate %s signaling peer: robot=%s [BOOTSTRAP:%s] incoming_ip=%s window=%s",
					role,
					robotID,
					bootstrapID,
					incomingSrcIP,
					debounceWindow,
				)
			}
			return
		}
		if prev != nil && prev != peer {
			if role == "client" {
				enqueueSignal(prev, SignalMessage{
					Type:        "session-preempted",
					RobotID:     robotID,
					BootstrapID: bootstrapID,
				})
			}
			prev.closeDone()
			log.Printf(
				"[webrtc] replaced stale %s signaling peer: robot=%s [BOOTSTRAP:%s] policy=%s",
				role,
				robotID,
				bootstrapID,
				webrtcSessionConcurrencyPolicy,
			)
		}
		if role == "client" {
			if activate != nil {
				activate(robotID, bootstrapID)
			}
			if counterpart != nil {
				enqueueSignal(counterpart, SignalMessage{
					Type:        "client-ready",
					RobotID:     robotID,
					BootstrapID: bootstrapID,
				})
			}
			if trigger != nil {
				// On-demand: always trigger bootstrap (Agent will decide if already connected)
				// Or if already connected, it helps ensure routing info is fresh.
				log.Printf("[webrtc] triggering on-demand bootstrap [BOOTSTRAP:%s] from IP:%s", bootstrapID, incomingSrcIP)
				go func() {
					if err := trigger(robotID, false, incomingSrcIP, bootstrapID, routeType); err != nil {
						log.Printf("[webrtc] on-demand trigger failed: %v", err)
					}
				}()
			}
		}

		go writePump(conn, peer)
		readPump(conn, peer, session, robotID, role == "agent", bootstrapID)

		var notifyPeer *PeerConn
		wasCurrent := false
		session.mu.Lock()
		if role == "agent" {
			if session.agent == peer {
				session.agent = nil
				notifyPeer = session.client
				wasCurrent = true
			}
		} else {
			if session.client == peer {
				session.client = nil
				notifyPeer = session.agent
				wasCurrent = true
			}
		}
		removeSession := session.agent == nil && session.client == nil
		session.mu.Unlock()
		if notifyPeer != nil {
			enqueueSignal(notifyPeer, SignalMessage{
				Type:        "bye",
				RobotID:     robotID,
				BootstrapID: bootstrapID,
			})
		}
		if role == "client" && wasCurrent && trigger != nil {
			log.Printf("[webrtc] triggering teardown on client disconnect [BOOTSTRAP:%s]", bootstrapID)
			go func() {
				if err := trigger(robotID, true, "", bootstrapID, routeType); err != nil {
					log.Printf("[webrtc] teardown trigger failed: %v", err)
				}
			}()
		}
		if removeSession {
			Registry.Remove(robotID)
		}

		log.Printf("[webrtc] %s disconnected: robot=%s [BOOTSTRAP:%s]", role, robotID, bootstrapID)
	}
}

func shouldDebouncePeerReplacement(existing, incoming *PeerConn, window time.Duration) bool {
	if existing == nil || incoming == nil || window <= 0 {
		return false
	}
	if existing.connectedAt.IsZero() || incoming.connectedAt.IsZero() {
		return false
	}
	if incoming.connectedAt.Sub(existing.connectedAt) > window {
		return false
	}

	existingIP := strings.TrimSpace(existing.observedSrcIP)
	incomingIP := strings.TrimSpace(incoming.observedSrcIP)
	if existingIP != "" && incomingIP != "" && strings.EqualFold(existingIP, incomingIP) {
		return true
	}

	existingBootstrap := strings.TrimSpace(existing.bootstrapID)
	incomingBootstrap := strings.TrimSpace(incoming.bootstrapID)
	return existingBootstrap != "" && incomingBootstrap != "" && strings.EqualFold(existingBootstrap, incomingBootstrap)
}

func webrtcPreemptDebounceWindow() time.Duration {
	ms := parseWebRTCEnvIntDefault(webrtcPreemptDebounceMsEnv, 3000, 0, 60000)
	return time.Duration(ms) * time.Millisecond
}

func parseWebRTCEnvIntDefault(key string, fallback, minValue, maxValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if v < minValue {
		return minValue
	}
	if v > maxValue {
		return maxValue
	}
	return v
}

func readPump(conn *websocket.Conn, peer *PeerConn, session *Session, robotID string, isAgent bool, bootstrapID string) {
	defer func() {
		peer.closeDone()
		conn.Close()
	}()

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg SignalMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[webrtc] read error robot=%s: %v", robotID, err)
			}
			break
		}
		msg.RobotID = robotID
		if strings.TrimSpace(msg.BootstrapID) == "" {
			msg.BootstrapID = bootstrapID
		}
		session.relay(msg, !isAgent)
	}
}

func writePump(conn *websocket.Conn, peer *PeerConn) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select {
		case msg, ok := <-peer.send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteJSON(msg); err != nil {
				log.Printf("[webrtc] write error: %v", err)
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-peer.done:
			return
		}
	}
}

func enqueueSignal(peer *PeerConn, msg SignalMessage) bool {
	if peer == nil {
		return false
	}
	select {
	case peer.send <- msg:
		return true
	default:
		log.Printf("[webrtc] signal queue full: robot=%s type=%s", msg.RobotID, msg.Type)
		return false
	}
}
