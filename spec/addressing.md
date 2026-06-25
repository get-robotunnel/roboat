# RoboTunnel Addressing — agent_id → tunnel endpoint resolution

Status: **Planned (Phase B)** · License: Apache-2.0

This document defines how a tunnel **initiator** resolves a registry `agent_id`
(`agt_xxx`) to a tunnel-addressable endpoint, bridging the registry identity layer
and the tunnel routing layer.

---

## 1. Problem

The tunnel protocol (v0.2) uses an internal `robot_id` for routing. The Robot Agent
Registry uses `agent_id` (`agt_xxx`) as the stable identity. An initiator that
knows only `agt_B` cannot connect without a lookup step.

---

## 2. Resolution flow (Phase B)

```
initiator daemon                registry API                tunnel-svc
     │                               │                          │
     │  1. dial("agt_B")             │                          │
     │                               │                          │
     │  2. GET /v1/discover/agents/agt_B                        │
     │  ─────────────────────────►   │                          │
     │                               │                          │
     │  3. { tunnel_endpoint: "rob_xxx" }                       │
     │  ◄─────────────────────────   │                          │
     │                               │                          │
     │  4. connect to tunnel-svc with robot_id="rob_xxx"        │
     │  ────────────────────────────────────────────────────►   │
     │                                                          │
     │  5. WebRTC / relay (existing v0.2 signaling)             │
```

---

## 3. Registry fields involved

The registry's `agents` table stores a `tunnel_endpoint` field (string). This is
the stable tunnel routing key for the agent — in Phase A this is the `robot_id`
used with `tunnel-svc`; in Phase B it may encode additional routing metadata.

**Invariant**: `tunnel_endpoint` is set/refreshed by the responder daemon on startup
via heartbeat. It is **not** a real-time IP address — live connection setup is
handled by `tunnel-svc` signaling.

---

## 4. rt-resolver crate (Phase B)

`rust/crates/rt-resolver` will implement:

```rust
pub struct Resolver {
    registry_url: String,
    cache: Arc<Mutex<HashMap<String, CachedEndpoint>>>,
}

impl Resolver {
    /// Resolve agent_id → tunnel routing key.
    /// Caches positive results for `cache_ttl_secs`.
    pub async fn resolve(&self, agent_id: &str) -> Result<String, ResolveError>;
}
```

Cache TTL: 60s positive, 5s negative. Cache is invalidated on dial failure.

---

## 5. Fallback (Phase A)

In Phase A, `dial`'s `target_agent_id` is passed directly as `host:port`. The
daemon does no registry lookup. This means Phase A requires the initiator to know
the responder's IP address and port — acceptable for local development and testing.
