// Package connauth defines the authentication seam for the tunnel connection
// endpoints. It exists so the connection code (webrtc signaling, TURN, relay)
// depends only on small interfaces — never on the Operations platform's
// internal auth/db packages. The tunnel supplies a robot authenticator backed
// by its own identity store; the client authorizer is backed by the ops
// internal API (which owns platform_token + robot ownership).
package connauth

import "github.com/gin-gonic/gin"

// RobotAuthenticator verifies a robot's connection credential (robot_api_key)
// against the tunnel's own identity store.
type RobotAuthenticator interface {
	// VerifyRobotAPIKey reports whether apiKey is valid for robotID.
	VerifyRobotAPIKey(robotID, apiKey string) (bool, error)
}

// ClientAuthorizer authorizes a client (CLI / browser) request for a robot.
// It is backed by the Operations layer, which owns platform_token validation
// and robot ownership. On failure the implementation writes the appropriate
// error response to c and returns false.
type ClientAuthorizer interface {
	AuthorizeClient(c *gin.Context, robotID string) bool
}

// Authenticator combines both auth paths used by the connection endpoints.
type Authenticator interface {
	RobotAuthenticator
	ClientAuthorizer
}
