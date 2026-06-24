package relay

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	FrameCommandRequest  = 0x02
	FrameCommandResponse = 0x03
	FrameWebRtcBootstrap = 0x20
	FrameWebRtcTeardown  = 0x21
)

type WebRtcBootstrapPayload struct {
	BootstrapID string `json:"bootstrap_id"`
	CliPublicIP string `json:"cli_public_ip,omitempty"`
	CliLanCIDR  string `json:"cli_lan_cidr,omitempty"`
	RouteType   string `json:"route_type,omitempty"`
}

type WebRtcTeardownPayload struct {
	BootstrapID string `json:"bootstrap_id,omitempty"`
}

func SendCommand(agentIP string, agentPort int, keySeed []byte, request map[string]interface{}) (map[string]interface{}, error) {
	addr := net.JoinHostPort(agentIP, fmt.Sprintf("%d", agentPort))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial agent: %w", err)
	}
	defer conn.Close()

	if err := authenticate(conn, keySeed); err != nil {
		return nil, fmt.Errorf("auth agent: %w", err)
	}

	reqBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	if err := writeFrame(conn, FrameCommandRequest, reqBytes); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(35 * time.Second))
	frameType, respBytes, err := readFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if frameType != FrameCommandResponse {
		return nil, fmt.Errorf("unexpected frame type: 0x%02x", frameType)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(respBytes, &response); err != nil {
		return nil, err
	}

	return response, nil
}

// TriggerWebRtcBootstrap sends a special frame to the agent over TCP to trigger WebRTC bootstrap.
// This is used for on-demand WebRTC connectivity.
func TriggerWebRtcBootstrap(agentIP string, agentPort int, keySeed []byte, payload WebRtcBootstrapPayload) error {
	addr := net.JoinHostPort(agentIP, fmt.Sprintf("%d", agentPort))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial agent for bootstrap trigger: %w", err)
	}
	defer conn.Close()

	if err := authenticate(conn, keySeed); err != nil {
		return fmt.Errorf("auth agent for bootstrap trigger: %w", err)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal bootstrap payload: %w", err)
	}

	if err := writeFrame(conn, FrameWebRtcBootstrap, data); err != nil {
		return fmt.Errorf("write bootstrap trigger: %w", err)
	}

	return nil
}

// TriggerWebRtcTeardown sends a special frame to the agent over TCP to terminate WebRTC resources.
func TriggerWebRtcTeardown(agentIP string, agentPort int, keySeed []byte, payload WebRtcTeardownPayload) error {
	addr := net.JoinHostPort(agentIP, fmt.Sprintf("%d", agentPort))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial agent for teardown trigger: %w", err)
	}
	defer conn.Close()

	if err := authenticate(conn, keySeed); err != nil {
		return fmt.Errorf("auth agent for teardown trigger: %w", err)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal teardown payload: %w", err)
	}

	if err := writeFrame(conn, FrameWebRtcTeardown, data); err != nil {
		return fmt.Errorf("write teardown trigger: %w", err)
	}

	return nil
}

func authenticate(conn net.Conn, seed []byte) error {
	if len(seed) != ed25519.SeedSize {
		return errors.New("invalid seed size")
	}
	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	nonce := make([]byte, 32)
	if _, err := io.ReadFull(conn, nonce); err != nil {
		return err
	}

	sig := ed25519.Sign(privKey, nonce)

	if _, err := conn.Write(pubKey); err != nil {
		return err
	}
	if _, err := conn.Write(sig); err != nil {
		return err
	}

	res := make([]byte, 1)
	if _, err := io.ReadFull(conn, res); err != nil {
		return err
	}
	if res[0] != 0x01 {
		return errors.New("unauthorized by agent")
	}

	return nil
}

func writeFrame(conn net.Conn, frameType byte, data []byte) error {
	if err := writeByte(conn, frameType); err != nil {
		return err
	}
	length := uint32(len(data))
	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func readFrame(conn net.Conn) (byte, []byte, error) {
	frameType, err := readByte(conn)
	if err != nil {
		return 0, nil, err
	}

	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return 0, nil, err
	}

	if length > 64*1024*1024 {
		return 0, nil, errors.New("frame too large")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, nil, err
	}

	return frameType, data, nil
}

func writeByte(conn net.Conn, b byte) error {
	_, err := conn.Write([]byte{b})
	return err
}

func readByte(conn net.Conn) (byte, error) {
	b := make([]byte, 1)
	_, err := io.ReadFull(conn, b)
	return b[0], err
}
