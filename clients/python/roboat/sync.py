"""Synchronous wrapper around the async Daemon client."""

from __future__ import annotations

import asyncio
from typing import Optional

from .daemon import Daemon, Stream


class SyncDaemon:
    """
    Synchronous wrapper for Daemon. Uses asyncio.run() internally.
    Not suitable for use inside an existing event loop — use Daemon directly
    in async code.
    """

    def __init__(self, socket_path: Optional[str] = None) -> None:
        kwargs = {"socket_path": socket_path} if socket_path else {}
        self._daemon = Daemon(**kwargs)
        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self._daemon.connect())

    def listen(self, agent_id: str, registry_token: Optional[str] = None) -> None:
        self._loop.run_until_complete(self._daemon.listen(agent_id, registry_token))

    def dial(self, target_agent_id: str, stream_class: str = "control") -> "SyncStream":
        stream = self._loop.run_until_complete(
            self._daemon.dial(target_agent_id, stream_class)
        )
        return SyncStream(stream, self._loop)

    def next_incoming(self) -> "SyncStream":
        stream = self._loop.run_until_complete(self._daemon._incoming_queue.get())
        return SyncStream(stream, self._loop)

    def close(self) -> None:
        self._loop.run_until_complete(self._daemon.close())
        self._loop.close()


class SyncStream:
    def __init__(self, stream: Stream, loop: asyncio.AbstractEventLoop) -> None:
        self._stream = stream
        self._loop = loop

    @property
    def stream_id(self) -> int:
        return self._stream.stream_id

    @property
    def class_(self) -> str:
        return self._stream.class_

    def send(self, data: bytes) -> None:
        self._loop.run_until_complete(self._stream.send(data))

    def recv(self) -> bytes:
        return self._loop.run_until_complete(self._stream.recv())

    def close(self) -> None:
        self._loop.run_until_complete(self._stream.close())
