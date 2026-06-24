-- RoboTunnel tunnel service schema.
--
-- The tunnel owns robot *connection identity* and connection runtime state.
-- Robot *business metadata* (owner, name, role, tier) stays in the Operations
-- database; the two join on robot_id. Connection identity is provisioned by ops
-- via the internal API (ProvisionRobot) at agent-registration time.

-- Connection identity: one row per robot that may connect through the tunnel.
CREATE TABLE IF NOT EXISTS robot_conn (
    robot_id      UUID PRIMARY KEY,
    agent_id      TEXT,
    api_key_hash  TEXT NOT NULL,           -- sha256(robot_api_key), hex
    robot_ip      TEXT,                    -- last observed public IP
    local_ip      TEXT,                    -- agent-reported LAN IP
    nat_type      TEXT,
    last_seen_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Route diagnostics (persisted so CP decisions survive restarts / scale-out).
CREATE TABLE IF NOT EXISTS robot_route_health (
    robot_id           UUID PRIMARY KEY REFERENCES robot_conn(robot_id) ON DELETE CASCADE,
    last_good_route    TEXT,
    attempt_count      BIGINT NOT NULL DEFAULT 0,
    success_count      BIGINT NOT NULL DEFAULT 0,
    tcp_fallback_count BIGINT NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Connection liveness snapshot (in-memory cache is authoritative at runtime;
-- this row lets a restarted / second instance recover recent state without a
-- dedicated Redis. Online is derived from last_heartbeat_at recency.)
CREATE TABLE IF NOT EXISTS conn_state (
    robot_id          UUID PRIMARY KEY REFERENCES robot_conn(robot_id) ON DELETE CASCADE,
    control_connected BOOLEAN NOT NULL DEFAULT FALSE,
    last_heartbeat_at TIMESTAMPTZ,
    nat_type          TEXT,
    public_ip         TEXT,
    local_ip          TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-bootstrap session diagnostics (short retention, for forensics).
CREATE TABLE IF NOT EXISTS conn_evidence (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    robot_id     UUID NOT NULL,
    bootstrap_id TEXT,
    phase        TEXT,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conn_evidence_robot ON conn_evidence (robot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_conn_state_heartbeat ON conn_state (last_heartbeat_at DESC);
