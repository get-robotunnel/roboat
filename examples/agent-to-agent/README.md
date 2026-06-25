# Example: agent-to-agent via daemon

Demonstrates two agents — one Python, one Go — communicating through two
`robotunneld` daemon instances without writing a line of Rust.

## Architecture

```
Python responder agent
  └─► robotunneld (socket: /tmp/rt-responder.sock, TCP port: 11412)
          │
          │ (direct TCP tunnel connection)
          │
Go initiator agent
  └─► robotunneld (socket: /tmp/rt-initiator.sock, TCP port: 11411)
```

## Prerequisites

- Rust toolchain installed (`cargo build` from `rust/`)
- Python ≥ 3.10
- Go ≥ 1.22

## Steps

### 1. Build the daemon

```bash
cd ../../rust
cargo build -p robotunneld
```

The binary is at `../../rust/target/debug/robotunneld`.

### 2. Start the responder daemon

```bash
RT_DAEMON_SOCKET=/tmp/rt-responder.sock \
RT_DAEMON_LISTEN_PORT=11412 \
../../rust/target/debug/robotunneld
```

### 3. Start the initiator daemon

In another terminal:

```bash
RT_DAEMON_SOCKET=/tmp/rt-initiator.sock \
RT_DAEMON_LISTEN_PORT=11411 \
../../rust/target/debug/robotunneld
```

### 4. Run the Python responder

In another terminal:

```bash
RT_DAEMON_SOCKET=/tmp/rt-responder.sock \
python3 python_responder.py
```

Expected output:
```
responder: listening for incoming connections...
```

### 5. Run the Go initiator

In another terminal:

```bash
cd go_initiator
RT_DAEMON_SOCKET=/tmp/rt-initiator.sock \
go run . 127.0.0.1:11412
```

Expected output:
```
initiator: dialing 127.0.0.1:11412 ...
initiator: sent: "hello from go initiator!"
initiator: got reply: "hello from python responder!"
```

And the Python terminal should show:
```
responder: incoming stream 1 from <key-prefix>
responder: received: 'hello from go initiator!'
responder: done
```

## What this proves

Neither the Python nor the Go agent imports any Rust code. All tunnel complexity
(TCP connection, Ed25519 auth, RelayOpen handshake, data framing) is handled by
the two `robotunneld` daemon instances. The agents talk only to their local Unix
socket.
