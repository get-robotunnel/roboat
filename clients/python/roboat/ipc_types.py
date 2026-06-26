"""IPC protocol constants."""

from enum import Enum


class StreamClass(str, Enum):
    CONTROL = "control"
    META = "meta"
    BULK = "bulk"
