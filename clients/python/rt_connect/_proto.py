"""IPC frame encoding/decoding helpers (synchronous, used by async layer)."""

import json
import struct

_HEADER = struct.Struct(">I")  # u32 big-endian
MAX_MSG_SIZE = 4 * 1024 * 1024


def encode_msg(obj: dict) -> bytes:
    payload = json.dumps(obj).encode("utf-8")
    if len(payload) > MAX_MSG_SIZE:
        raise ValueError(f"message too large: {len(payload)} bytes")
    return _HEADER.pack(len(payload)) + payload


def decode_msg(data: bytes) -> dict:
    return json.loads(data)
