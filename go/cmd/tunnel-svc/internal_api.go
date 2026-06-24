package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/get-robotunnel/robotunnel-tunnel/go/connstate"
	"github.com/get-robotunnel/robotunnel-tunnel/go/relay"
	"github.com/gin-gonic/gin"
)

// registerInternalAPI exposes the localhost-only API consumed by ops once the
// CP/DP relay lives here: ops sends robot commands and reads connection
// liveness through the tunnel instead of an in-process hub. Gated by the shared
// INTERNAL_API_SECRET.
func registerInternalAPI(r *gin.Engine, secret string, hub *relay.AgentControlHub) {
	grp := r.Group("/internal")
	grp.Use(func(c *gin.Context) {
		if secret == "" || c.GetHeader("X-Internal-Secret") != secret {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "internal auth required"})
			return
		}
		c.Next()
	})

	grp.POST("/command", func(c *gin.Context) {
		var req struct {
			RobotID   string                 `json:"robot_id"`
			Command   map[string]interface{} `json:"command"`
			TimeoutMS int                    `json:"timeout_ms"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.RobotID) == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id and command required"})
			return
		}
		timeout := time.Duration(req.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		resp, err := hub.SendCommand(req.RobotID, req.Command, timeout)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, resp)
	})

	grp.POST("/status", func(c *gin.Context) {
		var req struct {
			RobotID   string                 `json:"robot_id"`
			Query     map[string]interface{} `json:"query"`
			TimeoutMS int                    `json:"timeout_ms"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.RobotID) == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "robot_id required"})
			return
		}
		timeout := time.Duration(req.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		resp, err := hub.SendStatusQuery(req.RobotID, req.Query, timeout)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, resp)
	})

	grp.GET("/liveness/:robot_id", func(c *gin.Context) {
		robotID := strings.TrimSpace(c.Param("robot_id"))
		snap, ok := connstate.Snapshot(robotID)
		if !ok {
			c.JSON(http.StatusOK, gin.H{"robot_id": robotID, "online": false, "control_connected": false})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"robot_id":          robotID,
			"online":            connstate.IsOnline(snap),
			"control_connected": hub.HasConnection(robotID),
			"relay_connected":   hub.HasRelayConnection(robotID),
		})
	})
}
