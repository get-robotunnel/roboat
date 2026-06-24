package relay

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/get-robotunnel/robotunnel-tunnel/go/connstate"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	agentControlWriteWait      = 10 * time.Second
	agentControlPongWait       = 60 * time.Second
	agentControlPingPeriod     = (agentControlPongWait * 9) / 10
	agentControlMaxMessageSize = 256 * 1024
	// Avoid log storms when stale/overloaded relay streams keep sending data.
	agentControlUnmatchedRelayLogEvery = 500
	agentControlRelayCloseCooldown     = 2 * time.Second
	// Control-plane concurrency policy is intentionally strict by default:
	// reject competing control channels for the same robot_id to avoid
	// preemption oscillation when duplicate agents are online.
	agentControlConcurrencyPolicyRejectNew       = "reject_new"
	agentControlConcurrencyPolicyPreemptExisting = "preempt_existing"
	agentControlConcurrencyPolicyEnv             = "ROBOTUNNEL_CONTROL_CONCURRENCY_POLICY"
	relayClientWriteTimeoutEnv                   = "ROBOTUNNEL_RELAY_CLIENT_WRITE_TIMEOUT_MS"
	relayClientSoftTimeoutBudgetEnv              = "ROBOTUNNEL_RELAY_CLIENT_SOFT_TIMEOUT_BUDGET"
)

var agentControlUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// AuthRecord is the minimal robot identity the relay needs after the control
// credentials are verified by a RobotResolver.
type AuthRecord struct {
	ID      string  `json:"id"`
	LocalIP *string `json:"local_ip"`
}

type agentControlMessage struct {
	Type        string                 `json:"type"`
	BootstrapID string                 `json:"bootstrap_id,omitempty"`
	SessionKey  string                 `json:"session_key,omitempty"`
	RouteType   string                 `json:"route_type,omitempty"`
	CliPublicIP string                 `json:"cli_public_ip,omitempty"`
	CliLanCIDR  string                 `json:"cli_lan_cidr,omitempty"`
	RequestID   string                 `json:"request_id,omitempty"`
	Command     map[string]interface{} `json:"command,omitempty"`
	StatusQuery map[string]interface{} `json:"status_query,omitempty"`
	Response    map[string]interface{} `json:"response,omitempty"`
	RelayID     string                 `json:"relay_id,omitempty"`
	Port        int                    `json:"port,omitempty"`
	Data        string                 `json:"data,omitempty"`
	Status      string                 `json:"status,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

type agentControlConn struct {
	robotID       string
	sessionID     string
	observedSrcIP string
	connectedAt   time.Time
	hub           *AgentControlHub
	ws            *websocket.Conn
	send          chan agentControlMessage
	done          chan struct{}
	once          sync.Once
	reasonMu      sync.Mutex
	reason        string
}

func (c *agentControlConn) close() {
	c.once.Do(func() {
		close(c.done)
		if c.ws != nil {
			_ = c.ws.Close()
		}
	})
}

type agentRelayConn struct {
	robotID       string
	sessionID     string
	observedSrcIP string
	connectedAt   time.Time
	hub           *AgentControlHub
	ws            *websocket.Conn
	send          chan agentControlMessage
	done          chan struct{}
	once          sync.Once
	reasonMu      sync.Mutex
	reason        string
}

func (c *agentRelayConn) close() {
	c.once.Do(func() {
		close(c.done)
		if c.ws != nil {
			_ = c.ws.Close()
		}
	})
}

func (c *agentControlConn) setDisconnectReason(reason string) {
	reason = strings.TrimSpace(reason)
	if c == nil || reason == "" {
		return
	}
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	if c.reason == "" {
		c.reason = reason
	}
}

func (c *agentControlConn) disconnectReason() string {
	if c == nil {
		return ""
	}
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return strings.TrimSpace(c.reason)
}

func (c *agentRelayConn) setDisconnectReason(reason string) {
	reason = strings.TrimSpace(reason)
	if c == nil || reason == "" {
		return
	}
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	if c.reason == "" {
		c.reason = reason
	}
}

func (c *agentRelayConn) disconnectReason() string {
	if c == nil {
		return ""
	}
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return strings.TrimSpace(c.reason)
}

type AgentControlHub struct {
	mu    sync.RWMutex
	conns map[string]*agentControlConn
	relay map[string]*agentRelayConn

	pendingMu sync.Mutex
	pending   map[string]chan map[string]interface{}

	sessionMu      sync.RWMutex
	activeSessions map[string]string

	relayMu sync.Mutex
	relays  map[string]relayRegistration
}

type relayRegistration struct {
	robotID    string
	sessionKey string
	ch         chan agentControlMessage
	enqueuedAt chan time.Time
	stats      *relayQueueStats
}

type relayQueueStats struct {
	enqueued           uint64
	droppedOldest      uint64
	droppedNoSlot      uint64
	maxQueueDepth      uint64
	enqueueWaitSamples uint64
	enqueueWaitTotalNs uint64
	enqueueWaitMaxNs   uint64
}

type relayQueueStatsSnapshot struct {
	Enqueued           uint64
	DroppedOldest      uint64
	DroppedNoSlot      uint64
	MaxQueueDepth      uint64
	EnqueueWaitSamples uint64
	EnqueueWaitAvg     time.Duration
	EnqueueWaitMax     time.Duration
}

func (r relayRegistration) enqueue(msg agentControlMessage, ts time.Time) bool {
	if r.ch == nil {
		return false
	}
	select {
	case r.ch <- msg:
		if r.enqueuedAt != nil {
			select {
			case r.enqueuedAt <- ts:
			default:
			}
		}
		if r.stats != nil {
			r.stats.recordEnqueue(len(r.ch))
		}
		return true
	default:
		return false
	}
}

func (r relayRegistration) dropOldestQueuedMessage() bool {
	if r.ch == nil {
		return false
	}
	select {
	case <-r.ch:
		if r.enqueuedAt != nil {
			select {
			case <-r.enqueuedAt:
			default:
			}
		}
		if r.stats != nil {
			r.stats.recordDropOldest()
		}
		return true
	default:
		return false
	}
}

func (r relayRegistration) popEnqueueTimestamp() (time.Time, bool) {
	if r.enqueuedAt == nil {
		return time.Time{}, false
	}
	select {
	case ts := <-r.enqueuedAt:
		return ts, true
	default:
		return time.Time{}, false
	}
}

func newRelayQueueStats() *relayQueueStats {
	return &relayQueueStats{}
}

func (s *relayQueueStats) recordEnqueue(queueDepth int) {
	if s == nil {
		return
	}
	atomic.AddUint64(&s.enqueued, 1)
	if queueDepth > 0 {
		atomicMaxUint64(&s.maxQueueDepth, uint64(queueDepth))
	}
}

func (s *relayQueueStats) recordDropOldest() {
	if s == nil {
		return
	}
	atomic.AddUint64(&s.droppedOldest, 1)
}

func (s *relayQueueStats) recordDropNoSlot() {
	if s == nil {
		return
	}
	atomic.AddUint64(&s.droppedNoSlot, 1)
}

func (s *relayQueueStats) recordEnqueueWait(wait time.Duration) {
	if s == nil || wait <= 0 {
		return
	}
	waitNs := uint64(wait.Nanoseconds())
	atomic.AddUint64(&s.enqueueWaitSamples, 1)
	atomic.AddUint64(&s.enqueueWaitTotalNs, waitNs)
	atomicMaxUint64(&s.enqueueWaitMaxNs, waitNs)
}

func (s *relayQueueStats) snapshot() relayQueueStatsSnapshot {
	if s == nil {
		return relayQueueStatsSnapshot{}
	}
	samples := atomic.LoadUint64(&s.enqueueWaitSamples)
	totalNs := atomic.LoadUint64(&s.enqueueWaitTotalNs)
	var avg time.Duration
	if samples > 0 {
		avg = time.Duration(totalNs / samples)
	}
	return relayQueueStatsSnapshot{
		Enqueued:           atomic.LoadUint64(&s.enqueued),
		DroppedOldest:      atomic.LoadUint64(&s.droppedOldest),
		DroppedNoSlot:      atomic.LoadUint64(&s.droppedNoSlot),
		MaxQueueDepth:      atomic.LoadUint64(&s.maxQueueDepth),
		EnqueueWaitSamples: samples,
		EnqueueWaitAvg:     avg,
		EnqueueWaitMax:     time.Duration(atomic.LoadUint64(&s.enqueueWaitMaxNs)),
	}
}

func atomicMaxUint64(target *uint64, value uint64) {
	for {
		current := atomic.LoadUint64(target)
		if value <= current {
			return
		}
		if atomic.CompareAndSwapUint64(target, current, value) {
			return
		}
	}
}

type relayDeliveryResult int

const (
	relayDeliveryDelivered relayDeliveryResult = iota
	relayDeliveryNoRegistration
	relayDeliveryChannelFull
)

func NewAgentControlHub() *AgentControlHub {
	return &AgentControlHub{
		conns:          make(map[string]*agentControlConn),
		relay:          make(map[string]*agentRelayConn),
		pending:        make(map[string]chan map[string]interface{}),
		activeSessions: make(map[string]string),
		relays:         make(map[string]relayRegistration),
	}
}

func (h *AgentControlHub) Register(conn *agentControlConn) *agentControlConn {
	accepted, prev := h.RegisterWithPolicy(conn, agentControlConcurrencyPolicyPreemptExisting)
	if !accepted {
		return prev
	}
	return prev
}

func (h *AgentControlHub) RegisterWithPolicy(conn *agentControlConn, policy string) (bool, *agentControlConn) {
	if conn == nil {
		return false, nil
	}
	robotID := strings.TrimSpace(conn.robotID)
	if robotID == "" {
		return false, nil
	}
	conn.robotID = robotID

	policy = normalizeAgentControlConcurrencyPolicy(policy)

	h.mu.Lock()
	defer h.mu.Unlock()
	prev := h.conns[robotID]
	if prev != nil && policy == agentControlConcurrencyPolicyRejectNew {
		return false, prev
	}
	h.conns[robotID] = conn
	return true, prev
}

func (h *AgentControlHub) Unregister(conn *agentControlConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current, ok := h.conns[conn.robotID]
	if ok && current == conn {
		delete(h.conns, conn.robotID)
		return true
	}
	return false
}

func (h *AgentControlHub) Send(robotID string, msg agentControlMessage) bool {
	h.mu.RLock()
	conn := h.conns[strings.TrimSpace(robotID)]
	h.mu.RUnlock()
	if conn == nil {
		return false
	}

	select {
	case conn.send <- msg:
		return true
	default:
		log.Printf("[control] send queue full: robot=%s type=%s", robotID, msg.Type)
		return false
	}
}

func (h *AgentControlHub) HasConnection(robotID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.conns[strings.TrimSpace(robotID)]
	return ok
}

func (h *AgentControlHub) RegisterRelayConn(conn *agentRelayConn) *agentRelayConn {
	if conn == nil {
		return nil
	}
	robotID := strings.TrimSpace(conn.robotID)
	if robotID == "" {
		return nil
	}
	conn.robotID = robotID

	h.mu.Lock()
	defer h.mu.Unlock()
	prev := h.relay[robotID]
	h.relay[robotID] = conn
	return prev
}

func (h *AgentControlHub) UnregisterRelayConn(conn *agentRelayConn) bool {
	if conn == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current, ok := h.relay[strings.TrimSpace(conn.robotID)]
	if ok && current == conn {
		delete(h.relay, strings.TrimSpace(conn.robotID))
		return true
	}
	return false
}

func (h *AgentControlHub) HasRelayConnection(robotID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.relay[strings.TrimSpace(robotID)]
	return ok
}

func (h *AgentControlHub) SendRelay(robotID string, msg agentControlMessage) bool {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return false
	}

	h.mu.RLock()
	relayConn := h.relay[robotID]
	h.mu.RUnlock()

	if relayConn == nil {
		if strings.TrimSpace(msg.Type) == "relay_open" {
			log.Printf("[relay] no dedicated relay channel: robot=%s type=%s", robotID, msg.Type)
		}
		return false
	}

	select {
	case relayConn.send <- msg:
		return true
	default:
		log.Printf("[relay] send queue full: robot=%s type=%s", robotID, msg.Type)
		return false
	}
}

func describeAgentWSDisconnect(err error) string {
	if err == nil {
		return ""
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr != nil {
		text := strings.TrimSpace(closeErr.Text)
		if text != "" {
			return fmt.Sprintf("websocket close code=%d text=%s", closeErr.Code, text)
		}
		return fmt.Sprintf("websocket close code=%d", closeErr.Code)
	}
	return strings.TrimSpace(err.Error())
}

func (h *AgentControlHub) SendCommand(robotID string, command map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	if timeout <= 0 {
		timeout = 35 * time.Second
	}
	robotID = strings.TrimSpace(robotID)
	sessionKey := h.ActiveSession(robotID)

	requestID := uuid.New().String()
	respCh := make(chan map[string]interface{}, 1)

	h.pendingMu.Lock()
	h.pending[requestID] = respCh
	h.pendingMu.Unlock()

	msg := agentControlMessage{
		Type:        "command_request",
		RequestID:   requestID,
		Command:     command,
		BootstrapID: sessionKey,
		SessionKey:  sessionKey,
	}
	if !h.Send(robotID, msg) {
		h.pendingMu.Lock()
		delete(h.pending, requestID)
		h.pendingMu.Unlock()
		return nil, fmt.Errorf("no active control channel")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(timeout):
		h.pendingMu.Lock()
		delete(h.pending, requestID)
		h.pendingMu.Unlock()
		return nil, fmt.Errorf("control command timeout after %s", timeout)
	}
}

func (h *AgentControlHub) SendStatusQuery(robotID string, query map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	robotID = strings.TrimSpace(robotID)
	sessionKey := h.ActiveSession(robotID)

	requestID := uuid.New().String()
	respCh := make(chan map[string]interface{}, 1)

	h.pendingMu.Lock()
	h.pending[requestID] = respCh
	h.pendingMu.Unlock()

	msg := agentControlMessage{
		Type:        "status_request",
		RequestID:   requestID,
		StatusQuery: query,
		BootstrapID: sessionKey,
		SessionKey:  sessionKey,
	}
	if !h.Send(robotID, msg) {
		h.pendingMu.Lock()
		delete(h.pending, requestID)
		h.pendingMu.Unlock()
		return nil, fmt.Errorf("no active control channel")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(timeout):
		h.pendingMu.Lock()
		delete(h.pending, requestID)
		h.pendingMu.Unlock()
		return nil, fmt.Errorf("status query timeout after %s", timeout)
	}
}

func (h *AgentControlHub) SetActiveSession(robotID, sessionKey string) {
	robotID = strings.TrimSpace(robotID)
	sessionKey = strings.TrimSpace(sessionKey)
	if robotID == "" || sessionKey == "" {
		return
	}

	var changed bool
	h.sessionMu.Lock()
	if strings.TrimSpace(h.activeSessions[robotID]) != sessionKey {
		changed = true
	}
	h.activeSessions[robotID] = sessionKey
	h.sessionMu.Unlock()

	if changed {
		h.preemptRelaysForSession(robotID, sessionKey)
	}
}

func (h *AgentControlHub) ActiveSession(robotID string) string {
	robotID = strings.TrimSpace(robotID)
	if robotID == "" {
		return ""
	}
	h.sessionMu.RLock()
	defer h.sessionMu.RUnlock()
	return strings.TrimSpace(h.activeSessions[robotID])
}

func (h *AgentControlHub) MatchesActiveSession(robotID, sessionKey string) bool {
	robotID = strings.TrimSpace(robotID)
	sessionKey = strings.TrimSpace(sessionKey)
	if robotID == "" || sessionKey == "" {
		return false
	}

	h.sessionMu.RLock()
	defer h.sessionMu.RUnlock()
	return strings.TrimSpace(h.activeSessions[robotID]) == sessionKey
}

func (h *AgentControlHub) ClearActiveSessionIfMatch(robotID, sessionKey string) bool {
	robotID = strings.TrimSpace(robotID)
	sessionKey = strings.TrimSpace(sessionKey)
	if robotID == "" {
		return false
	}

	h.sessionMu.Lock()
	defer h.sessionMu.Unlock()

	current := strings.TrimSpace(h.activeSessions[robotID])
	if current == "" {
		return false
	}
	if sessionKey != "" && current != sessionKey {
		return false
	}
	delete(h.activeSessions, robotID)
	return true
}

func (h *AgentControlHub) deliverResponse(requestID string, response map[string]interface{}) bool {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false
	}

	h.pendingMu.Lock()
	ch, ok := h.pending[requestID]
	if ok {
		delete(h.pending, requestID)
	}
	h.pendingMu.Unlock()
	if !ok {
		return false
	}

	select {
	case ch <- response:
	default:
	}
	return true
}

func (h *AgentControlHub) RegisterRelay(relayID, robotID, sessionKey string, ch chan agentControlMessage) {
	relayID = strings.TrimSpace(relayID)
	robotID = strings.TrimSpace(robotID)
	sessionKey = strings.TrimSpace(sessionKey)
	if relayID == "" || ch == nil {
		return
	}
	capacity := cap(ch)
	if capacity <= 0 {
		capacity = 1
	}
	h.relayMu.Lock()
	defer h.relayMu.Unlock()
	h.relays[relayID] = relayRegistration{
		robotID:    robotID,
		sessionKey: sessionKey,
		ch:         ch,
		enqueuedAt: make(chan time.Time, capacity),
		stats:      newRelayQueueStats(),
	}
}

func (h *AgentControlHub) UnregisterRelay(relayID string) {
	relayID = strings.TrimSpace(relayID)
	if relayID == "" {
		return
	}
	h.relayMu.Lock()
	defer h.relayMu.Unlock()
	delete(h.relays, relayID)
}

func (h *AgentControlHub) RelayStatsSnapshot(relayID string) relayQueueStatsSnapshot {
	relayID = strings.TrimSpace(relayID)
	if relayID == "" {
		return relayQueueStatsSnapshot{}
	}
	h.relayMu.Lock()
	reg := h.relays[relayID]
	h.relayMu.Unlock()
	return reg.stats.snapshot()
}

func (h *AgentControlHub) consumeRelayEnqueueWait(relayID string) time.Duration {
	relayID = strings.TrimSpace(relayID)
	if relayID == "" {
		return 0
	}
	h.relayMu.Lock()
	reg := h.relays[relayID]
	h.relayMu.Unlock()
	ts, ok := reg.popEnqueueTimestamp()
	if !ok || ts.IsZero() {
		return 0
	}
	wait := time.Since(ts)
	if wait < 0 {
		wait = 0
	}
	if reg.stats != nil {
		reg.stats.recordEnqueueWait(wait)
	}
	return wait
}

func (h *AgentControlHub) deliverRelay(msg agentControlMessage) relayDeliveryResult {
	relayID := strings.TrimSpace(msg.RelayID)
	if relayID == "" {
		return relayDeliveryNoRegistration
	}
	h.relayMu.Lock()
	reg := h.relays[relayID]
	h.relayMu.Unlock()
	if reg.ch == nil {
		return relayDeliveryNoRegistration
	}
	now := time.Now().UTC()
	if reg.enqueue(msg, now) {
		return relayDeliveryDelivered
	}
	// For high-rate relay_data, prefer dropping one stale buffered frame and
	// enqueueing the newest payload instead of forcing relay_close. This keeps
	// interactive streams (Foxglove image panes) stable under transient bursts.
	if strings.TrimSpace(msg.Type) == "relay_data" {
		_ = reg.dropOldestQueuedMessage()
		if reg.enqueue(msg, now) {
			return relayDeliveryDelivered
		}
	}
	if reg.stats != nil {
		reg.stats.recordDropNoSlot()
	}
	return relayDeliveryChannelFull
}

func (h *AgentControlHub) preemptRelaysForSession(robotID, activeSessionKey string) {
	robotID = strings.TrimSpace(robotID)
	activeSessionKey = strings.TrimSpace(activeSessionKey)
	if robotID == "" || activeSessionKey == "" {
		return
	}

	type staleRelay struct {
		relayID string
		reg     relayRegistration
	}

	stale := make([]staleRelay, 0, 4)

	h.relayMu.Lock()
	for relayID, reg := range h.relays {
		if !strings.EqualFold(strings.TrimSpace(reg.robotID), robotID) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(reg.sessionKey), activeSessionKey) {
			continue
		}
		if reg.ch == nil {
			continue
		}
		stale = append(stale, staleRelay{relayID: relayID, reg: reg})
	}
	h.relayMu.Unlock()

	for _, relay := range stale {
		msg := agentControlMessage{
			Type:       "relay_close",
			RelayID:    relay.relayID,
			SessionKey: activeSessionKey,
			Error:      "attach preempted by newer attach",
		}
		now := time.Now().UTC()
		if relay.reg.enqueue(msg, now) {
			continue
		}
		if relay.reg.dropOldestQueuedMessage() && relay.reg.enqueue(msg, now) {
			continue
		}
		if relay.reg.stats != nil {
			relay.reg.stats.recordDropNoSlot()
		}
	}
}

func (s *Server) handleAgentControlConnect(c *gin.Context) {
	robotID := strings.TrimSpace(c.Query("robot_id"))
	sessionID := strings.TrimSpace(c.Query("session_id"))
	if sessionID == "" {
		sessionID = uuid.New().String()
	}
	apiKey := strings.TrimSpace(c.GetHeader("X-Robot-API-Key"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(c.Query("api_key"))
	}
	if robotID == "" || apiKey == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id and api_key are required"})
		return
	}
	if _, err := uuid.Parse(robotID); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id must be a valid UUID"})
		return
	}

	authRecord, err := s.resolver.ResolveControl(robotID, apiKey)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "database query failed"})
		return
	}
	if authRecord == nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid robot credentials"})
		return
	}

	wsConn, err := agentControlUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[control] upgrade error: %v", err)
		return
	}

	conn := &agentControlConn{
		robotID:       robotID,
		sessionID:     sessionID,
		observedSrcIP: strings.TrimSpace(c.ClientIP()),
		connectedAt:   time.Now().UTC(),
		hub:           s.hub,
		ws:            wsConn,
		send:          make(chan agentControlMessage, 256),
		done:          make(chan struct{}),
	}

	policy := currentAgentControlConcurrencyPolicy()
	accepted, prev := s.hub.RegisterWithPolicy(conn, policy)
	if !accepted {
		incumbentIP := "<unknown>"
		incumbentAt := "<unknown>"
		if prev != nil {
			if ip := strings.TrimSpace(prev.observedSrcIP); ip != "" {
				incumbentIP = ip
			}
			if !prev.connectedAt.IsZero() {
				incumbentAt = prev.connectedAt.UTC().Format(time.RFC3339)
			}
		}
		log.Printf(
			"[control] rejected competing connection: robot=%s policy=%s incoming_src_ip=%s incumbent_src_ip=%s incumbent_connected_at=%s",
			robotID,
			policy,
			conn.observedSrcIP,
			incumbentIP,
			incumbentAt,
		)
		connstate.RecordControlContention(
			robotID,
			conn.observedSrcIP,
			incumbentIP,
			policy,
			"competing_control_connection",
			time.Now().UTC(),
		)
		_ = wsConn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "competing control connection for robot_id"),
			time.Now().Add(2*time.Second),
		)
		conn.close()
		return
	}
	if prev != nil {
		prev.close()
		log.Printf(
			"[control] replaced active connection: robot=%s policy=%s old_src_ip=%s new_src_ip=%s",
			robotID,
			policy,
			strings.TrimSpace(prev.observedSrcIP),
			conn.observedSrcIP,
		)
	}
	now := time.Now().UTC()
	// ClientIP here is an observed control-plane source only.
	// It is intentionally not persisted as authoritative robot_ip.
	observedSrcIP := conn.observedSrcIP
	localIP := ""
	if authRecord.LocalIP != nil {
		localIP = strings.TrimSpace(*authRecord.LocalIP)
	}
	_ = s.resolver.TouchLastSeen(authRecord.ID)
	connstate.UpsertControlConnected(
		authRecord.ID,
		observedSrcIP,
		localIP,
		"unknown",
		now,
	)
	log.Printf(
		"[control] agent connected: robot=%s session_id=%s observed_src_ip=%s",
		robotID,
		sessionID,
		observedSrcIP,
	)

	go writeAgentControlPump(conn)
	readAgentControlPump(conn)

	removed := s.hub.Unregister(conn)
	if removed {
		connstate.SetControlDisconnected(conn.robotID, time.Now().UTC())
	}
	conn.close()
	reason := conn.disconnectReason()
	if reason == "" {
		reason = "unknown"
	}
	log.Printf("[control] agent disconnected: robot=%s session_id=%s reason=%s", robotID, sessionID, reason)
}

func (s *Server) handleAgentRelayConnect(c *gin.Context) {
	robotID := strings.TrimSpace(c.Query("robot_id"))
	sessionID := strings.TrimSpace(c.Query("session_id"))
	if sessionID == "" {
		sessionID = uuid.New().String()
	}
	apiKey := strings.TrimSpace(c.GetHeader("X-Robot-API-Key"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(c.Query("api_key"))
	}
	if robotID == "" || apiKey == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id and api_key are required"})
		return
	}
	if _, err := uuid.Parse(robotID); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id must be a valid UUID"})
		return
	}

	authRecord, err := s.resolver.ResolveControl(robotID, apiKey)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "database query failed"})
		return
	}
	if authRecord == nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid robot credentials"})
		return
	}

	wsConn, err := agentControlUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[relay] upgrade error: %v", err)
		return
	}

	conn := &agentRelayConn{
		robotID:       robotID,
		sessionID:     sessionID,
		observedSrcIP: strings.TrimSpace(c.ClientIP()),
		connectedAt:   time.Now().UTC(),
		hub:           s.hub,
		ws:            wsConn,
		send:          make(chan agentControlMessage, 2048),
		done:          make(chan struct{}),
	}

	prev := s.hub.RegisterRelayConn(conn)
	if prev != nil {
		prev.close()
		log.Printf(
			"[relay] replaced relay connection: robot=%s old_session=%s new_session=%s",
			robotID,
			strings.TrimSpace(prev.sessionID),
			sessionID,
		)
	}
	log.Printf(
		"[relay] agent relay connected: robot=%s session_id=%s observed_src_ip=%s",
		robotID,
		sessionID,
		conn.observedSrcIP,
	)

	go writeAgentRelayPump(conn)
	readAgentRelayPump(conn)

	_ = s.hub.UnregisterRelayConn(conn)
	conn.close()
	reason := conn.disconnectReason()
	if reason == "" {
		reason = "unknown"
	}
	log.Printf("[relay] agent relay disconnected: robot=%s session_id=%s reason=%s", robotID, sessionID, reason)
}

func currentAgentControlConcurrencyPolicy() string {
	raw := strings.TrimSpace(os.Getenv(agentControlConcurrencyPolicyEnv))
	return normalizeAgentControlConcurrencyPolicy(raw)
}

func normalizeAgentControlConcurrencyPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "", agentControlConcurrencyPolicyRejectNew:
		return agentControlConcurrencyPolicyRejectNew
	case agentControlConcurrencyPolicyPreemptExisting:
		return agentControlConcurrencyPolicyPreemptExisting
	default:
		log.Printf("WARNING: invalid %s=%q, fallback=%s", agentControlConcurrencyPolicyEnv, policy, agentControlConcurrencyPolicyRejectNew)
		return agentControlConcurrencyPolicyRejectNew
	}
}


func readAgentControlPump(conn *agentControlConn) {
	defer conn.close()

	conn.ws.SetReadLimit(agentControlMaxMessageSize)
	relayUnmatchedCounts := map[string]uint64{}
	relayCloseSentAt := map[string]time.Time{}
	recordRelayMiss := func(messageType, relayID, reason string) {
		key := strings.TrimSpace(relayID)
		if key == "" {
			key = "<empty>"
		}
		relayUnmatchedCounts[key]++
		count := relayUnmatchedCounts[key]
		if count == 1 || count%agentControlUnmatchedRelayLogEvery == 0 {
			log.Printf(
				"[control] unmatched %s robot=%s relay_id=%s reason=%s count=%d",
				messageType,
				conn.robotID,
				key,
				strings.TrimSpace(reason),
				count,
			)
		}
	}
	sendRelayClose := func(relayID, reason string) {
		relayID = strings.TrimSpace(relayID)
		reason = strings.TrimSpace(reason)
		if relayID == "" || conn.hub == nil {
			return
		}
		now := time.Now()
		if last, ok := relayCloseSentAt[relayID]; ok && now.Sub(last) < agentControlRelayCloseCooldown {
			return
		}
		relayCloseSentAt[relayID] = now
		sessionKey := strings.TrimSpace(conn.hub.ActiveSession(conn.robotID))
		_ = conn.hub.SendRelay(conn.robotID, agentControlMessage{
			Type:       "relay_close",
			RelayID:    relayID,
			SessionKey: sessionKey,
			Error:      reason,
		})
	}
	refreshReadDeadline := func() {
		_ = conn.ws.SetReadDeadline(time.Now().Add(agentControlPongWait))
	}
	refreshReadDeadline()
	conn.ws.SetPongHandler(func(string) error {
		refreshReadDeadline()
		connstate.TouchControlActivity(conn.robotID, time.Now().UTC())
		return nil
	})
	conn.ws.SetPingHandler(func(appData string) error {
		refreshReadDeadline()
		_ = conn.ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(agentControlWriteWait))
		connstate.TouchControlActivity(conn.robotID, time.Now().UTC())
		return nil
	})

	for {
		msgType, payload, err := conn.ws.ReadMessage()
		if err != nil {
			conn.setDisconnectReason(describeAgentWSDisconnect(err))
			return
		}
		refreshReadDeadline()
		connstate.TouchControlActivity(conn.robotID, time.Now().UTC())
		if msgType != websocket.TextMessage {
			continue
		}

		var msg agentControlMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("[control] invalid inbound message robot=%s err=%v", conn.robotID, err)
			continue
		}
		messageType := strings.TrimSpace(msg.Type)
		if messageType == "command_response" || messageType == "status_response" {
			if conn.hub != nil && conn.hub.deliverResponse(msg.RequestID, msg.Response) {
				continue
			}
			log.Printf(
				"[control] unmatched %s robot=%s request_id=%s session_key=%s",
				messageType,
				conn.robotID,
				strings.TrimSpace(msg.RequestID),
				controlMessageSessionKey(msg),
			)
			continue
		}
		if messageType == "relay_open_ack" || messageType == "relay_data" || messageType == "relay_close" {
			relayID := strings.TrimSpace(msg.RelayID)
			if conn.hub != nil {
				switch conn.hub.deliverRelay(msg) {
				case relayDeliveryDelivered:
					continue
				case relayDeliveryChannelFull:
					// Channel pressure is handled by dropping stale relay_data in
					// deliverRelay(); keep stream alive to avoid periodic reconnects.
					recordRelayMiss(messageType, relayID, "channel_full")
					continue
				case relayDeliveryNoRegistration:
					// Ask agent to stop sending unknown relay stream ids.
					sendRelayClose(relayID, "relay_not_registered")
					recordRelayMiss(messageType, relayID, "missing_registration")
					continue
				}
			}
			recordRelayMiss(messageType, relayID, "hub_unavailable")
		}
	}
}

func readAgentRelayPump(conn *agentRelayConn) {
	defer conn.close()

	conn.ws.SetReadLimit(agentControlMaxMessageSize)
	relayUnmatchedCounts := map[string]uint64{}
	relayCloseSentAt := map[string]time.Time{}
	recordRelayMiss := func(messageType, relayID, reason string) {
		key := strings.TrimSpace(relayID)
		if key == "" {
			key = "<empty>"
		}
		relayUnmatchedCounts[key]++
		count := relayUnmatchedCounts[key]
		if count == 1 || count%agentControlUnmatchedRelayLogEvery == 0 {
			log.Printf(
				"[relay] unmatched %s robot=%s relay_id=%s reason=%s count=%d",
				messageType,
				conn.robotID,
				key,
				strings.TrimSpace(reason),
				count,
			)
		}
	}
	sendRelayClose := func(relayID, reason string) {
		relayID = strings.TrimSpace(relayID)
		reason = strings.TrimSpace(reason)
		if relayID == "" || conn.hub == nil {
			return
		}
		now := time.Now()
		if last, ok := relayCloseSentAt[relayID]; ok && now.Sub(last) < agentControlRelayCloseCooldown {
			return
		}
		relayCloseSentAt[relayID] = now
		sessionKey := strings.TrimSpace(conn.hub.ActiveSession(conn.robotID))
		_ = conn.hub.SendRelay(conn.robotID, agentControlMessage{
			Type:       "relay_close",
			RelayID:    relayID,
			SessionKey: sessionKey,
			Error:      reason,
		})
	}
	refreshReadDeadline := func() {
		_ = conn.ws.SetReadDeadline(time.Now().Add(agentControlPongWait))
	}
	refreshReadDeadline()
	conn.ws.SetPongHandler(func(string) error {
		refreshReadDeadline()
		return nil
	})
	conn.ws.SetPingHandler(func(appData string) error {
		refreshReadDeadline()
		_ = conn.ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(agentControlWriteWait))
		return nil
	})

	for {
		msgType, payload, err := conn.ws.ReadMessage()
		if err != nil {
			conn.setDisconnectReason(describeAgentWSDisconnect(err))
			return
		}
		refreshReadDeadline()
		if msgType != websocket.TextMessage {
			continue
		}

		var msg agentControlMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("[relay] invalid inbound message robot=%s err=%v", conn.robotID, err)
			continue
		}
		messageType := strings.TrimSpace(msg.Type)
		if messageType == "relay_open_ack" || messageType == "relay_data" || messageType == "relay_close" {
			relayID := strings.TrimSpace(msg.RelayID)
			if conn.hub != nil {
				switch conn.hub.deliverRelay(msg) {
				case relayDeliveryDelivered:
					continue
				case relayDeliveryChannelFull:
					recordRelayMiss(messageType, relayID, "channel_full")
					continue
				case relayDeliveryNoRegistration:
					sendRelayClose(relayID, "relay_not_registered")
					recordRelayMiss(messageType, relayID, "missing_registration")
					continue
				}
			}
			recordRelayMiss(messageType, relayID, "hub_unavailable")
			continue
		}
		log.Printf(
			"[relay] ignoring message type robot=%s session_id=%s type=%s",
			conn.robotID,
			strings.TrimSpace(conn.sessionID),
			messageType,
		)
	}
}

func writeAgentControlPump(conn *agentControlConn) {
	ticker := time.NewTicker(agentControlPingPeriod)
	defer func() {
		ticker.Stop()
		conn.close()
	}()

	for {
		select {
		case msg := <-conn.send:
			_ = conn.ws.SetWriteDeadline(time.Now().Add(agentControlWriteWait))
			if err := conn.ws.WriteJSON(msg); err != nil {
				conn.setDisconnectReason("control write failed: " + strings.TrimSpace(err.Error()))
				log.Printf("[control] write error: robot=%s err=%v", conn.robotID, err)
				return
			}
			connstate.TouchControlActivity(conn.robotID, time.Now().UTC())
		case <-ticker.C:
			_ = conn.ws.SetWriteDeadline(time.Now().Add(agentControlWriteWait))
			if err := conn.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				conn.setDisconnectReason("control ping failed: " + strings.TrimSpace(err.Error()))
				return
			}
			connstate.TouchControlActivity(conn.robotID, time.Now().UTC())
		case <-conn.done:
			return
		}
	}
}

func writeAgentRelayPump(conn *agentRelayConn) {
	ticker := time.NewTicker(agentControlPingPeriod)
	defer func() {
		ticker.Stop()
		conn.close()
	}()

	for {
		select {
		case msg := <-conn.send:
			_ = conn.ws.SetWriteDeadline(time.Now().Add(agentControlWriteWait))
			if err := conn.ws.WriteJSON(msg); err != nil {
				conn.setDisconnectReason("relay write failed: " + strings.TrimSpace(err.Error()))
				log.Printf(
					"[relay] write error: robot=%s session_id=%s err=%v",
					conn.robotID,
					strings.TrimSpace(conn.sessionID),
					err,
				)
				return
			}
		case <-ticker.C:
			_ = conn.ws.SetWriteDeadline(time.Now().Add(agentControlWriteWait))
			if err := conn.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				conn.setDisconnectReason("relay ping failed: " + strings.TrimSpace(err.Error()))
				return
			}
		case <-conn.done:
			return
		}
	}
}

func controlMessageSessionKey(msg agentControlMessage) string {
	if v := strings.TrimSpace(msg.SessionKey); v != "" {
		return v
	}
	return strings.TrimSpace(msg.BootstrapID)
}

func attachSessionSnapshot(sessionKey, robotID, userID string) (connstate.BootstrapSession, bool) {
	sessionKey = strings.TrimSpace(sessionKey)
	robotID = strings.TrimSpace(robotID)
	userID = strings.TrimSpace(userID)
	if sessionKey == "" || robotID == "" {
		return connstate.BootstrapSession{}, false
	}
	snapshot, ok := connstate.SessionSnapshot(sessionKey)
	if !ok {
		return connstate.BootstrapSession{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(snapshot.RobotID), robotID) {
		return connstate.BootstrapSession{}, false
	}
	if userID != "" && strings.TrimSpace(snapshot.UserID) != "" &&
		!strings.EqualFold(strings.TrimSpace(snapshot.UserID), userID) {
		return connstate.BootstrapSession{}, false
	}
	return snapshot, true
}

func attachSessionRefTime(snapshot connstate.BootstrapSession) time.Time {
	for _, raw := range []string{snapshot.UpdatedAt, snapshot.StartedAt} {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func shouldPromoteAttachSession(activeSessionKey, requestedSessionKey, robotID, userID string) bool {
	requested, ok := attachSessionSnapshot(requestedSessionKey, robotID, userID)
	if !ok {
		return false
	}
	active, ok := attachSessionSnapshot(activeSessionKey, robotID, userID)
	if !ok {
		return true
	}
	requestedAt := attachSessionRefTime(requested)
	activeAt := attachSessionRefTime(active)
	if requestedAt.IsZero() || activeAt.IsZero() {
		return false
	}
	return requestedAt.After(activeAt)
}

var userRelayUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (s *Server) handleRelayWS(c *gin.Context) {
	userID, err := s.resolver.RelayUser(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	if s.hub == nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "control channel unavailable"})
		return
	}

	robotID := strings.TrimSpace(c.Query("robot_id"))
	portRaw := strings.TrimSpace(c.Query("port"))
	if robotID == "" || portRaw == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id and port are required"})
		return
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "port must be between 1 and 65535"})
		return
	}

	owned, err := s.resolver.RobotOwnedBy(robotID, userID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Database query failed"})
		return
	}
	if !owned {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "robot not found"})
		return
	}
	if !s.hub.HasRelayConnection(robotID) {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "robot relay channel offline"})
		return
	}

	sessionKey := strings.TrimSpace(c.Query("session_key"))
	activeSession := ""
	if s.hub != nil {
		activeSession = strings.TrimSpace(s.hub.ActiveSession(robotID))
	}
	if sessionKey == "" {
		sessionKey = activeSession
	}
	if sessionKey == "" {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "robot attach session unavailable"})
		return
	}
	if activeSession != "" && !strings.EqualFold(activeSession, sessionKey) {
		if snapshot, ok := attachSessionSnapshot(sessionKey, robotID, userID); !ok || snapshot.SessionID == "" {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "attach session is stale; reconnect required"})
			return
		}
		if !shouldPromoteAttachSession(activeSession, sessionKey, robotID, userID) {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "attach session is stale; reconnect required"})
			return
		}
		s.hub.SetActiveSession(robotID, sessionKey)
	} else if activeSession == "" {
		if snapshot, ok := attachSessionSnapshot(sessionKey, robotID, userID); !ok || snapshot.SessionID == "" {
			// Single-instance recovery path: allow relay reconnect by adopting the
			// provided session key even if connstate retention has expired.
			log.Printf(
				"[relay] adopting session without connstate snapshot: robot=%s session_key=%s user=%s",
				robotID,
				sessionKey,
				userID,
			)
		}
		s.hub.SetActiveSession(robotID, sessionKey)
	}

	wsConn, err := userRelayUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer wsConn.Close()

	relayID := uuid.New().String()
	startedAt := time.Now()
	relayClientWriteTimeout := relayClientWriteTimeoutDuration()
	relayClientSoftTimeoutBudget := relayClientSoftTimeoutBudget()
	closeReason := "unknown"
	var closeReasonMu sync.Mutex
	closeReasonSet := false
	setCloseReason := func(reason string) {
		if v := strings.TrimSpace(reason); v != "" {
			closeReasonMu.Lock()
			if !closeReasonSet {
				closeReason = v
				closeReasonSet = true
			}
			closeReasonMu.Unlock()
		}
	}
	var (
		relayDataSent                  uint64
		relayDataDecodeErrors          uint64
		writerTimeoutDrops             uint64
		writerTimeoutBudgetExhausted   uint64
		writerHardErrors               uint64
		consecutiveWriterTimeoutStreak int
	)
	relayCh := make(chan agentControlMessage, 512)
	s.hub.RegisterRelay(relayID, robotID, sessionKey, relayCh)
	defer func() {
		queueStats := s.hub.RelayStatsSnapshot(relayID)
		s.hub.UnregisterRelay(relayID)
		duration := time.Since(startedAt).Round(time.Millisecond)
		closeReasonMu.Lock()
		reason := strings.TrimSpace(closeReason)
		closeReasonMu.Unlock()
		log.Printf(
			"[relay] closed robot=%s relay_id=%s port=%d session_key=%s reason=%s duration=%s writer_timeout_drops=%d writer_timeout_budget_exhausted=%d writer_hard_errors=%d relay_data_sent=%d relay_data_decode_errors=%d queue_enqueued=%d queue_drop_oldest=%d queue_drop_no_slot=%d queue_depth_max=%d enqueue_wait_avg=%s enqueue_wait_max=%s enqueue_wait_samples=%d",
			robotID,
			relayID,
			port,
			sessionKey,
			reason,
			duration,
			writerTimeoutDrops,
			writerTimeoutBudgetExhausted,
			writerHardErrors,
			relayDataSent,
			relayDataDecodeErrors,
			queueStats.Enqueued,
			queueStats.DroppedOldest,
			queueStats.DroppedNoSlot,
			queueStats.MaxQueueDepth,
			queueStats.EnqueueWaitAvg.Round(time.Millisecond),
			queueStats.EnqueueWaitMax.Round(time.Millisecond),
			queueStats.EnqueueWaitSamples,
		)
	}()
	openMsg := agentControlMessage{
		Type:       "relay_open",
		RelayID:    relayID,
		Port:       port,
		SessionKey: sessionKey,
	}
	if !s.hub.SendRelay(robotID, openMsg) {
		setCloseReason("relay_open_send_failed")
		_ = wsConn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "relay_open send failed"),
			time.Now().Add(2*time.Second),
		)
		return
	}

	pending := make([]agentControlMessage, 0, 4)
	openTimeout := time.NewTimer(8 * time.Second)
	defer openTimeout.Stop()
	for {
		select {
		case inbound := <-relayCh:
			_ = s.hub.consumeRelayEnqueueWait(relayID)
			t := strings.TrimSpace(inbound.Type)
			if t == "relay_open_ack" {
				if strings.EqualFold(strings.TrimSpace(inbound.Status), "ok") {
					goto relayEstablished
				}
				msg := strings.TrimSpace(inbound.Error)
				if msg == "" {
					msg = "relay_open rejected"
				}
				setCloseReason("relay_open_rejected:" + msg)
				_ = wsConn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, msg),
					time.Now().Add(2*time.Second),
				)
				return
			}
			pending = append(pending, inbound)
		case <-openTimeout.C:
			setCloseReason("relay_open_timeout")
			_ = wsConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "relay_open timeout"),
				time.Now().Add(2*time.Second),
			)
			return
		}
	}

relayEstablished:
	done := make(chan struct{})
	var closeOnce sync.Once
	closeRelay := func(reason string) {
		closeOnce.Do(func() {
			setCloseReason(reason)
			msg := agentControlMessage{
				Type:       "relay_close",
				RelayID:    relayID,
				SessionKey: sessionKey,
			}
			if strings.TrimSpace(reason) != "" {
				msg.Error = strings.TrimSpace(reason)
			}
			_ = s.hub.SendRelay(robotID, msg)
			close(done)
		})
	}
	defer closeRelay("client_disconnected")

	wsConn.SetReadLimit(2 * 1024 * 1024)
	_ = wsConn.SetReadDeadline(time.Now().Add(90 * time.Second))
	wsConn.SetPongHandler(func(string) error {
		_ = wsConn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	go func() {
		defer closeRelay("client_reader_closed")
		for {
			msgType, payload, err := wsConn.ReadMessage()
			if err != nil {
				setCloseReason(fmt.Sprintf("client_reader_error:%s", strings.TrimSpace(err.Error())))
				return
			}
			if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
				continue
			}
			out := agentControlMessage{
				Type:       "relay_data",
				RelayID:    relayID,
				SessionKey: sessionKey,
				Data:       base64.StdEncoding.EncodeToString(payload),
			}
			if !s.hub.SendRelay(robotID, out) {
				setCloseReason("relay_data_send_failed")
				return
			}
		}
	}()

	relayTicker := time.NewTicker(25 * time.Second)
	defer relayTicker.Stop()

	sendToClient := func(inbound agentControlMessage) bool {
		switch strings.TrimSpace(inbound.Type) {
		case "relay_data":
			raw := strings.TrimSpace(inbound.Data)
			if raw == "" {
				return true
			}
			data, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				relayDataDecodeErrors++
				closeRelay("invalid_relay_payload")
				return false
			}
			_ = wsConn.SetWriteDeadline(time.Now().Add(relayClientWriteTimeout))
			if err := wsConn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				if relayClientSoftTimeoutBudget > 0 && isRelayWriterTimeoutLikeError(err) {
					writerTimeoutDrops++
					consecutiveWriterTimeoutStreak++
					if consecutiveWriterTimeoutStreak <= relayClientSoftTimeoutBudget {
						if writerTimeoutDrops == 1 || writerTimeoutDrops%25 == 0 {
							log.Printf(
								"[relay] soft-drop client writer timeout robot=%s relay_id=%s port=%d drops=%d streak=%d budget=%d err=%s",
								robotID,
								relayID,
								port,
								writerTimeoutDrops,
								consecutiveWriterTimeoutStreak,
								relayClientSoftTimeoutBudget,
								strings.TrimSpace(err.Error()),
							)
						}
						return true
					}
					writerTimeoutBudgetExhausted++
					log.Printf(
						"[relay] writer timeout budget exhausted robot=%s relay_id=%s port=%d drops=%d streak=%d budget=%d",
						robotID,
						relayID,
						port,
						writerTimeoutDrops,
						consecutiveWriterTimeoutStreak,
						relayClientSoftTimeoutBudget,
					)
				}
				writerHardErrors++
				setCloseReason(fmt.Sprintf("client_writer_error:%s", strings.TrimSpace(err.Error())))
				return false
			}
			consecutiveWriterTimeoutStreak = 0
			relayDataSent++
			return true
		case "relay_close":
			msg := strings.TrimSpace(inbound.Error)
			if msg == "" {
				msg = "relay closed"
			}
			setCloseReason("upstream_relay_close:" + msg)
			_ = wsConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, msg),
				time.Now().Add(2*time.Second),
			)
			return false
		default:
			return true
		}
	}

	for _, msg := range pending {
		if !sendToClient(msg) {
			return
		}
	}
	for {
		select {
		case <-done:
			return
		case inbound := <-relayCh:
			_ = s.hub.consumeRelayEnqueueWait(relayID)
			if !sendToClient(inbound) {
				return
			}
		case <-relayTicker.C:
			_ = wsConn.SetWriteDeadline(time.Now().Add(relayClientWriteTimeout))
			if err := wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				setCloseReason(fmt.Sprintf("client_ping_error:%s", strings.TrimSpace(err.Error())))
				return
			}
		}
	}
}

func relayClientWriteTimeoutDuration() time.Duration {
	ms := parseGatewayEnvIntDefault(relayClientWriteTimeoutEnv, 20000, 1000, 120000)
	return time.Duration(ms) * time.Millisecond
}

func relayClientSoftTimeoutBudget() int {
	return parseGatewayEnvIntDefault(relayClientSoftTimeoutBudgetEnv, 6, 0, 256)
}

func parseGatewayEnvIntDefault(key string, fallback, minValue, maxValue int) int {
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

func isRelayWriterTimeoutLikeError(err error) bool {
	if err == nil {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "deadline exceeded")
}
