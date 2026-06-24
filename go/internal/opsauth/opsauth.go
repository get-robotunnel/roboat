// Package opsauth implements the client-side authorization seam against the
// Operations internal API. The tunnel does NOT know about platform_token or
// robot ownership — those are owned by ops. For client (CLI/browser) requests
// the tunnel forwards the inbound Authorization header to ops, which validates
// the platform_token and robot ownership and answers yes/no.
//
// During the transition (while the CP/DP relay still lives in the ops binary),
// the same client also fires the WebRTC bootstrap trigger through ops, since
// the agent's control-plane connection is held by the ops process.
package opsauth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Client talks to the ops internal API over localhost.
type Client struct {
	BaseURL  string // e.g. http://127.0.0.1:8080
	Secret   string // shared secret, sent as X-Internal-Secret
	HTTP     *http.Client
}

func New(baseURL, secret string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Secret:  secret,
		HTTP:    &http.Client{Timeout: 5 * time.Second},
	}
}

type authzClientRequest struct {
	RobotID       string `json:"robot_id"`
	Authorization string `json:"authorization"`
}

// AuthorizeClient validates the client and returns true if authorized. On
// failure it writes the appropriate error response to c and returns false.
// Implements connauth.ClientAuthorizer.
func (cl *Client) AuthorizeClient(c *gin.Context, robotID string) bool {
	authz := strings.TrimSpace(c.GetHeader("Authorization"))
	if authz == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
		return false
	}
	body, _ := json.Marshal(authzClientRequest{RobotID: robotID, Authorization: authz})
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost,
		cl.BaseURL+"/internal/authz/client", bytes.NewReader(body))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "authz request build failed"})
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", cl.Secret)

	resp, err := cl.HTTP.Do(req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{"error": "ops authz unreachable"})
		return false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusUnauthorized:
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid platform token"})
	case http.StatusForbidden:
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "robot does not belong to current user"})
	default:
		c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{"error": "ops authz error"})
	}
	return false
}

type authzRobotRequest struct {
	RobotID string `json:"robot_id"`
	APIKey  string `json:"api_key"`
}

// VerifyRobotAPIKey asks ops whether apiKey is valid for robotID. Used as the
// transition authority for robot (agent) auth before the tunnel's own
// robot_conn identity store is populated. Implements connauth.RobotAuthenticator.
func (cl *Client) VerifyRobotAPIKey(robotID, apiKey string) (bool, error) {
	body, _ := json.Marshal(authzRobotRequest{RobotID: robotID, APIKey: apiKey})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cl.BaseURL+"/internal/authz/robot", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", cl.Secret)
	resp, err := cl.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return false, nil
	default:
		return false, fmt.Errorf("ops robot authz status %d", resp.StatusCode)
	}
}

type authzUserRequest struct {
	Authorization string `json:"authorization"`
}
type authzUserResponse struct {
	UserID string `json:"user_id"`
}

// RelayUser validates the bearer authorization header via ops and returns the
// authenticated user id. Used for the user-side relay (handleRelayWS).
func (cl *Client) RelayUser(authHeader string) (string, error) {
	body, _ := json.Marshal(authzUserRequest{Authorization: authHeader})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cl.BaseURL+"/internal/authz/user", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", cl.Secret)
	resp, err := cl.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ops user authz status %d", resp.StatusCode)
	}
	var out authzUserResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.UserID, nil
}

type authzOwnsRequest struct {
	RobotID string `json:"robot_id"`
	UserID  string `json:"user_id"`
}

// RobotOwnedBy reports whether robotID is owned by userID (ops authority).
func (cl *Client) RobotOwnedBy(robotID, userID string) (bool, error) {
	body, _ := json.Marshal(authzOwnsRequest{RobotID: robotID, UserID: userID})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cl.BaseURL+"/internal/authz/owns", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", cl.Secret)
	resp, err := cl.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound, http.StatusForbidden:
		return false, nil
	default:
		return false, fmt.Errorf("ops owns authz status %d", resp.StatusCode)
	}
}

type bootstrapRequest struct {
	RobotID     string `json:"robot_id"`
	IsTeardown  bool   `json:"is_teardown"`
	CliIP       string `json:"cli_ip"`
	BootstrapID string `json:"bootstrap_id"`
	RouteType   string `json:"route_type"`
}

// TriggerBootstrap fires (or tears down) a WebRTC bootstrap on the agent via the
// ops control plane. Signature matches webrtc.BootstrapTrigger. Used while the
// CP relay still lives in the ops binary.
func (cl *Client) TriggerBootstrap(robotID string, isTeardown bool, cliIP, bootstrapID, routeType string) error {
	body, _ := json.Marshal(bootstrapRequest{
		RobotID: robotID, IsTeardown: isTeardown, CliIP: cliIP,
		BootstrapID: bootstrapID, RouteType: routeType,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cl.BaseURL+"/internal/agent/bootstrap", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", cl.Secret)
	resp, err := cl.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ops bootstrap trigger status %d", resp.StatusCode)
	}
	return nil
}
