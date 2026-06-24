# RoboTunnel Tunnel

**The open, shared connection layer for robots.** RoboTunnel Tunnel links a
robot-side agent to a client (CLI, browser, or another service) over the public
internet and automatically picks the best path — LAN → public TCP → STUN P2P →
TURN → platform relay.

It is the base infrastructure shared by two products:

- **[Robot Operations](https://ops.robotunnel.io)** — commercial robot ops
  platform (remote ROS 2 debugging, monitoring, logs).
- **[Robot Agent Registry](https://reg.robotunnel.io)** — open-source robot
  agent registration & discovery.

Both call the tunnel; the tunnel knows nothing about either's business logic.

## Why open source

The connection protocol is meant to become a de-facto standard. Robot Operations
monetizes hosting, metering, and the ops experience — not code secrecy. The wire
protocol, the Rust client, and the Go relay are all Apache-2.0.

## Layout

```
spec/    Authoritative wire-protocol spec (tunnel-protocol.md)
rust/    Agent/client reference impl — crates rt-connect-core, rt-connect-webrtc
go/      Platform/relay reference impl — cmd/tunnel-svc (served at tunnel.robotunnel.io)
deploy/  systemd unit, Caddy vhost, bootstrap/setup scripts
```

## Quick orientation

- Read [`spec/tunnel-protocol.md`](spec/tunnel-protocol.md) first — it is the
  contract. The `rust/` and `go/` trees are reference implementations of it.
- The relay (`go/cmd/tunnel-svc`) listens on `:8091` and serves the signaling,
  TURN-credential, control-plane (CP), and data-plane (DP) relay endpoints.
- Robots run the Rust agent built on `rust/` crates and connect to
  `tunnel.robotunnel.io`.

## Status

Reference implementation of protocol **v0.2**. The connection-layer reliability
redesign (DP state machine, backpressure, platform-relay repositioning, P2P
hardening) is a tracked future effort and is not in this release.

## License

Apache-2.0. See [LICENSE](LICENSE).
