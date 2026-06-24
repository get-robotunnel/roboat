# Deploying the RoboTunnel Tunnel Service

`tunnel-svc` runs on the shared VPS on port **8091**, behind Caddy at
`tunnel.robotunnel.io`, as a separate systemd service from Robot Operations
(`:8080`) and the Registry (`:8090`).

## Prerequisites (operator-provided)

1. **DNS:** `tunnel.robotunnel.io` A record → VPS IP.
2. **Dedicated database:** a Postgres/Supabase project for the tunnel, separate
   from ops and registry. Put its connection string in `DATABASE_URL`.
3. **GitHub `production` environment secrets** (shared with ops + registry):
   `PROD_SSH_HOST`, `PROD_SSH_PORT`, `PROD_SSH_USER`, `PROD_SSH_PRIVATE_KEY`,
   `PROD_SSH_KNOWN_HOSTS`.
4. **Connection secrets** moved from the ops `.env` into the tunnel `.env`:
   `TURN_SECRET`, `RT_AGENT_AUTH_SEED_HEX` (see the rotation note below — rotate
   while moving them), plus a fresh `INTERNAL_API_SECRET`
   (`openssl rand -hex 32`) shared with ops.

## First deploy

```bash
# On the VPS, as root — one-time prep (config template + tunnel vhost):
sudo ./bootstrap.sh
sudo vi /opt/robotunnel-tunnel/config/.env    # fill DATABASE_URL, TURN_SECRET, RT_AGENT_AUTH_SEED_HEX, INTERNAL_API_SECRET

# Then deploy the binary (from GitHub → Actions → "Deploy Tunnel"), or by hand:
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tunnel-svc ./cmd/tunnel-svc   (run in go/)
#   scp tunnel-svc tunnel.service setup.sh root@VPS:/tmp && ssh root@VPS 'cd /tmp && ./setup.sh'
```

Migrations apply automatically on boot. Health check:
`curl https://tunnel.robotunnel.io/health`.

## Zero-downtime cutover (the api.robotunnel.io strangler)

Already-deployed agents talk to `api.robotunnel.io`. To move them onto the
tunnel service **without a client change or downtime**, route the connection
path-prefixes on the existing `api.robotunnel.io` Caddy vhost to `:8091` (see
Part B in `Caddyfile.tunnel`). Stage it:

- **Phase 1** — once signaling + TURN are live on tunnel-svc *and* ops exposes
  the internal endpoints (`/internal/authz/client`, `/internal/agent/bootstrap`)
  and robots are provisioned into `robot_conn`: route `/api/signal/*` and
  `/api/turn-credentials*` to `:8091`. Verify with `val/route-acceptance.sh` /
  `val/route-matrix.sh` against a live agent.
- **Phase 2** — once the CP/DP relay is extracted: add the remaining prefixes
  (`/api/agent/connect*`, `/api/agent/relay*`, `/v1/agent/*`, `/api/relay/ws*`,
  `/api/heartbeat*`, `/api/agent-auth-public-key*`, `/api/agent/authorized-keys*`).

**Rollback:** remove the `@tunnel` matcher/route from the `api.robotunnel.io`
block and `systemctl reload caddy` — traffic returns to ops `:8080`, which still
contains the connection handlers until the extraction is finalized.

## Security / rotation

The tunnel now owns the connection secrets. When you move `TURN_SECRET` and
`RT_AGENT_AUTH_SEED_HEX` out of the ops `.env`, **rotate them** (they were
exposed in plaintext historically). Rotating `RT_AGENT_AUTH_SEED_HEX` changes
`/api/agent-auth-public-key`; agents re-fetch it, so do it in a maintenance
window. Keep all secrets in `/opt/robotunnel-tunnel/config/.env` (chmod 600),
never in git.
