package main

import (
	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/opsauth"
	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/store"
	"github.com/get-robotunnel/robotunnel-tunnel/go/relay"
	"github.com/gin-gonic/gin"
)

// robotResolver implements relay.RobotResolver. Robot (agent) identity is
// local-first (tunnel robot_conn store), falling back to the ops authority
// while the store is unpopulated. User-side relay auth (platform_token +
// ownership) is always delegated to ops, which owns it.
type robotResolver struct {
	store *store.Store // nil in Phase 1 (no dedicated tunnel DB yet)
	ops   *opsauth.Client
}

func (r *robotResolver) ResolveControl(robotID, apiKey string) (*relay.AuthRecord, error) {
	if r.store != nil {
		hash, localIP, found, err := r.store.LookupConn(robotID)
		if err != nil {
			return nil, err
		}
		if found {
			if hash != "" && hash == store.HashAPIKey(apiKey) {
				return &relay.AuthRecord{ID: robotID, LocalIP: localIP}, nil
			}
			return nil, nil
		}
	}
	ok, err := r.ops.VerifyRobotAPIKey(robotID, apiKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &relay.AuthRecord{ID: robotID}, nil
}

func (r *robotResolver) TouchLastSeen(robotID string) error {
	if r.store != nil {
		return r.store.TouchLastSeen(robotID)
	}
	return nil
}

func (r *robotResolver) RelayUser(c *gin.Context) (string, error) {
	return r.ops.RelayUser(c.GetHeader("Authorization"))
}

func (r *robotResolver) RobotOwnedBy(robotID, userID string) (bool, error) {
	return r.ops.RobotOwnedBy(robotID, userID)
}

func (r *robotResolver) RobotBootstrapTarget(robotID string) (string, error) {
	if r.store != nil {
		return r.store.RobotIP(robotID)
	}
	return "", nil
}
