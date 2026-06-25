//! Daemon connection manager — bridges IPC agent sessions with tunnel TCP connections.
//!
//! Phase A: direct TCP only (no registry lookup, no WebRTC).
//! Phase B will add rt-resolver for agent_id → tunnel endpoint resolution.

use crate::ipc::StreamClass;
use rt_core::{
    auth::{ClientAuthenticator, ServerAuthenticator},
    protocol::{read_frame, write_frame, FrameType, ProtocolError},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::{
    io::split,
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Mutex},
};
use tracing;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Auth error: {0}")]
    Auth(#[from] rt_core::auth::AuthError),
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("Stream {0} not found")]
    StreamNotFound(u32),
    #[error("Send failed: channel closed")]
    SendFailed,
}

/// Configuration for the daemon.
pub struct DaemonConfig {
    /// TCP port the daemon listens on for inbound tunnel connections.
    pub listen_port: u16,
    /// Unix socket path for local IPC (informational; IpcServer uses it).
    pub socket_path: String,
    /// Accept any valid Ed25519 signature without an allowlist (Phase A default: true).
    pub insecure_allow_any_client: bool,
    /// 32-byte seed for client-side Ed25519 auth (Phase A uses all-zeros).
    pub auth_seed: [u8; 32],
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen_port: 11411,
            socket_path: "/var/run/robotunnel/rt.sock".to_string(),
            insecure_allow_any_client: true,
            auth_seed: [0u8; 32],
        }
    }
}

/// Info about a newly-established inbound stream, broadcast to IPC subscribers.
#[derive(Debug, Clone)]
pub struct IncomingStreamInfo {
    pub stream_id: u32,
    pub from_agent_id: String,
    pub class: StreamClass,
}

struct StreamEntry {
    /// Send data TO the remote peer (IPC → wire).
    data_tx: mpsc::Sender<Vec<u8>>,
    /// Receive data FROM the remote peer (wire → IPC).
    /// Wrapped in Option so IpcServer can claim it exactly once.
    recv_rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>,
    #[allow(dead_code)]
    class: StreamClass,
}

/// Central manager: owns active streams, issues stream IDs, handles TCP connectivity.
pub struct DaemonManager {
    pub config: DaemonConfig,
    streams: Arc<Mutex<HashMap<u32, StreamEntry>>>,
    next_stream_id: Arc<AtomicU32>,
    incoming_tx: broadcast::Sender<IncomingStreamInfo>,
    listener_started: Arc<tokio::sync::Mutex<bool>>,
}

impl DaemonManager {
    pub fn new(config: DaemonConfig) -> Self {
        let (incoming_tx, _) = broadcast::channel(64);
        Self {
            config,
            streams: Arc::new(Mutex::new(HashMap::new())),
            next_stream_id: Arc::new(AtomicU32::new(1)),
            incoming_tx,
            listener_started: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }

    /// Subscribe to notifications of new inbound streams.
    pub fn subscribe_incoming(&self) -> broadcast::Receiver<IncomingStreamInfo> {
        self.incoming_tx.subscribe()
    }

    /// Claim the recv_rx for a stream (can only be claimed once).
    pub async fn take_recv_rx(&self, stream_id: u32) -> Option<mpsc::Receiver<Vec<u8>>> {
        let streams = self.streams.lock().await;
        if let Some(entry) = streams.get(&stream_id) {
            let mut rx = entry.recv_rx.lock().await;
            rx.take()
        } else {
            None
        }
    }

    /// Start the TCP listener for inbound tunnel connections (idempotent).
    pub async fn start_listener(self: Arc<Self>) -> Result<(), DaemonError> {
        let mut started = self.listener_started.lock().await;
        if *started {
            return Ok(());
        }
        *started = true;
        drop(started);

        let port = self.config.listen_port;
        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        tracing::info!("daemon: TCP listener on 0.0.0.0:{}", port);

        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        tracing::info!("daemon: inbound TCP connection from {}", addr);
                        let mgr = manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = mgr.handle_inbound(stream).await {
                                tracing::warn!("daemon: inbound connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("daemon: listener accept error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_inbound(self: Arc<Self>, mut stream: TcpStream) -> Result<(), DaemonError> {
        let authorized_keys = if self.config.insecure_allow_any_client {
            vec![]
        } else {
            vec![]
        };
        let server_auth = ServerAuthenticator::new(authorized_keys);
        let pub_key = server_auth.authenticate(&mut stream).await?;
        let from_agent_id = pub_key[..16].to_string();

        let (mut reader, mut writer) = split(stream);

        // Read RelayOpen frame from initiator
        let (frame_type, payload) = read_frame(&mut reader).await?;
        if frame_type != FrameType::RelayOpen {
            return Err(DaemonError::Protocol(rt_core::protocol::ProtocolError::InvalidPacket(
                "expected RelayOpen".into(),
            )));
        }
        if payload.len() < 5 {
            return Err(DaemonError::Protocol(rt_core::protocol::ProtocolError::InvalidPacket(
                "RelayOpen payload too short".into(),
            )));
        }
        let stream_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let class = StreamClass::from_byte(payload[4]);

        // Send RelayOpenAck
        write_frame(&mut writer, FrameType::RelayOpenAck, &payload).await?;

        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(64);
        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);

        let entry = StreamEntry {
            data_tx,
            recv_rx: Arc::new(Mutex::new(Some(recv_rx))),
            class: class.clone(),
        };

        self.streams.lock().await.insert(stream_id, entry);

        // Spawn bridge task
        let streams_clone = self.streams.clone();
        let incoming_tx = self.incoming_tx.clone();
        let info = IncomingStreamInfo {
            stream_id,
            from_agent_id,
            class,
        };

        // Broadcast the incoming notification
        let _ = incoming_tx.send(info);

        // Bridge task: relay data between TCP and channels
        tokio::spawn(async move {
            run_stream_bridge(stream_id, reader, writer, data_rx, recv_tx, streams_clone).await;
        });

        Ok(())
    }

    /// Connect to a remote peer and return (stream_id, recv_rx).
    pub async fn dial(
        self: Arc<Self>,
        target: String,
        class: StreamClass,
    ) -> Result<(u32, mpsc::Receiver<Vec<u8>>), DaemonError> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let mut tcp = TcpStream::connect(&target).await?;

        let client_auth = ClientAuthenticator::from_seed(&self.config.auth_seed);
        client_auth.authenticate(&mut tcp).await?;

        let (mut reader, mut writer) = split(tcp);

        // Send RelayOpen
        let mut relay_open_payload = Vec::with_capacity(5);
        relay_open_payload.extend_from_slice(&stream_id.to_be_bytes());
        relay_open_payload.push(class.as_byte());
        write_frame(&mut writer, FrameType::RelayOpen, &relay_open_payload).await?;

        // Wait for RelayOpenAck
        let (frame_type, _ack_payload) = read_frame(&mut reader).await?;
        if frame_type != FrameType::RelayOpenAck {
            return Err(DaemonError::Protocol(rt_core::protocol::ProtocolError::InvalidPacket(
                "expected RelayOpenAck".into(),
            )));
        }

        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(64);
        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);

        let entry = StreamEntry {
            data_tx,
            recv_rx: Arc::new(Mutex::new(None)), // claimed below immediately
            class,
        };
        self.streams.lock().await.insert(stream_id, entry);

        // Spawn bridge task
        let streams_clone = self.streams.clone();
        tokio::spawn(async move {
            run_stream_bridge(stream_id, reader, writer, data_rx, recv_tx, streams_clone).await;
        });

        Ok((stream_id, recv_rx))
    }

    /// Send data on an established stream.
    pub async fn send(&self, stream_id: u32, data: Vec<u8>) -> Result<(), DaemonError> {
        let streams = self.streams.lock().await;
        let entry = streams
            .get(&stream_id)
            .ok_or(DaemonError::StreamNotFound(stream_id))?;
        entry
            .data_tx
            .send(data)
            .await
            .map_err(|_| DaemonError::SendFailed)?;
        Ok(())
    }

    /// Close and remove a stream.
    pub async fn close_stream(&self, stream_id: u32) {
        self.streams.lock().await.remove(&stream_id);
    }
}

/// Background task: bridge between a TCP connection and the channel pair.
///
/// Reads from `data_rx` and writes `RelayData` frames to TCP.
/// Reads `RelayData` frames from TCP and writes to `recv_tx`.
/// On `RelayClose` or channel close, removes the stream.
async fn run_stream_bridge(
    stream_id: u32,
    mut reader: tokio::io::ReadHalf<TcpStream>,
    mut writer: tokio::io::WriteHalf<TcpStream>,
    mut data_rx: mpsc::Receiver<Vec<u8>>,
    recv_tx: mpsc::Sender<Vec<u8>>,
    streams: Arc<Mutex<HashMap<u32, StreamEntry>>>,
) {
    loop {
        tokio::select! {
            // Outbound: IPC agent → TCP wire
            maybe_data = data_rx.recv() => {
                match maybe_data {
                    Some(data) => {
                        if write_frame(&mut writer, FrameType::RelayData, &data).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // Channel closed: send RelayClose
                        let _ = write_frame(&mut writer, FrameType::RelayClose, &[]).await;
                        break;
                    }
                }
            }
            // Inbound: TCP wire → IPC agent
            frame_result = read_frame(&mut reader) => {
                match frame_result {
                    Ok((FrameType::RelayData, data)) => {
                        if recv_tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Ok((FrameType::RelayClose, _)) | Err(_) => {
                        break;
                    }
                    Ok(_) => {} // ignore other frames
                }
            }
        }
    }
    streams.lock().await.remove(&stream_id);
    tracing::debug!("daemon: stream {} closed", stream_id);
}
