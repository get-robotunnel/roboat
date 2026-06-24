package webrtc

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/connauth"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TURNCredentials struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username"`
	Credential string   `json:"credential"`
	TTL        int      `json:"ttl_seconds"`
}

// TURNCredentialHandler returns short-lived TURN credentials.
//
// Auth modes (via the injected Authenticator):
//  1. Agent:  robot_id + X-Robot-API-Key header (query api_key kept for compatibility)
//  2. Client: Authorization Bearer <platform_token> + robot_id query (validated by ops)
func TURNCredentialHandler(authn connauth.Authenticator, turnHost, turnSecret string, advertiseTLS bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		robotID := strings.TrimSpace(c.Query("robot_id"))
		if robotID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id required"})
			return
		}
		if _, err := uuid.Parse(robotID); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id must be a valid UUID"})
			return
		}

		apiKey := strings.TrimSpace(c.GetHeader("X-Robot-API-Key"))
		if apiKey == "" {
			apiKey = strings.TrimSpace(c.Query("api_key"))
		}
		if apiKey != "" {
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

		bootstrapID := strings.TrimSpace(c.Query("bootstrap_id"))
		if bootstrapID == "" {
			bootstrapID = "none"
		}

		if turnHost == "" || turnSecret == "" {
			log.Printf("[webrtc] TURN service disabled [BOOTSTRAP:%s]", bootstrapID)
			c.JSON(http.StatusOK, gin.H{"turn_available": false})
			return
		}

		ttl := 3600
		expiry := time.Now().Unix() + int64(ttl)
		username := fmt.Sprintf("%d:%s", expiry, robotID)

		mac := hmac.New(sha1.New, []byte(turnSecret))
		mac.Write([]byte(username))
		credential := base64.StdEncoding.EncodeToString(mac.Sum(nil))

		log.Printf("[webrtc] TURN credentials issued: robot=%s [BOOTSTRAP:%s]", robotID, bootstrapID)

		urls := []string{
			fmt.Sprintf("turn:%s:3478", turnHost),
		}
		if advertiseTLS {
			urls = append(urls, fmt.Sprintf("turns:%s:5349", turnHost))
		}

		creds := TURNCredentials{
			URLs:       urls,
			Username:   username,
			Credential: credential,
			TTL:        ttl,
		}

		c.JSON(http.StatusOK, gin.H{
			"turn_available": true,
			"stun_urls":      []string{"stun:stun.l.google.com:19302"},
			"turn":           creds,
		})
	}
}
