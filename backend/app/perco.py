"""Minimal client for the PERCo CT13.x web-interface protocol.

The controller exposes a JSON protocol over a WebSocket at ws://<host>/tcp.
Messages are newline-separated JSON documents (``\\n\\n`` after each one).
The bundled lighttpd of the controller rejects the handshake produced by
common WebSocket libraries, so the handshake and framing are implemented
here on top of a plain socket (stdlib only).

Only read operations are implemented: ``get`` requests and optional
authentication.  No configuration-changing commands are ever sent.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import socket
import struct
import time

log = logging.getLogger(__name__)

OP_TEXT = 0x1
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class PercoError(Exception):
    """Raised when the controller cannot be reached or answers incorrectly."""


class PercoClient:
    def __init__(self, host: str, port: int = 80, password: str = "",
                 timeout: float = 8.0, idle_timeout: float = 1.0,
                 extra_headers: dict[str, str] | None = None):
        self.host = host
        self.port = port
        self.password = password or ""
        self.timeout = timeout
        self.idle_timeout = idle_timeout
        self.extra_headers = extra_headers or {}
        self._sock: socket.socket | None = None
        self._buf = b""
        self._text_buf = ""
        self._authenticated = False

    # ------------------------------------------------------------------ socket
    def connect(self) -> None:
        self.close()
        try:
            sock = socket.create_connection((self.host, self.port), self.timeout)
        except OSError as exc:
            raise PercoError(f"connect {self.host}:{self.port}: {exc}") from exc
        sock.settimeout(self.timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        lines = [
            "GET /tcp HTTP/1.1",
            f"Host: {self.host}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            f"Origin: http://{self.host}",
            "User-Agent: Mozilla/5.0",
            "Accept: */*",
            "Accept-Language: ru",
        ]
        for name, value in self.extra_headers.items():
            lines.append(f"{name}: {value}")
        request = "\r\n".join(lines) + "\r\n\r\n"
        try:
            sock.sendall(request.encode())
        except OSError as exc:
            sock.close()
            raise PercoError(f"handshake send failed: {exc}") from exc

        try:
            head, self._buf = self._read_headers(sock)
        except Exception:
            sock.close()
            raise
        if b"101" not in head.split(b"\r\n", 1)[0]:
            sock.close()
            raise PercoError(f"unexpected handshake response: {head[:120]!r}")
        self._sock = sock
        self._authenticated = False
        self._text_buf = ""

    def _read_headers(self, sock: socket.socket) -> tuple[bytes, bytes]:
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                raise PercoError("connection closed during handshake")
            buf += chunk
        head, rest = buf.split(b"\r\n\r\n", 1)
        return head, rest

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None
        self._buf = b""
        self._text_buf = ""

    @property
    def connected(self) -> bool:
        return self._sock is not None

    # -------------------------------------------------------------------- frames
    def _send_frame(self, opcode: int, payload: bytes = b"") -> None:
        if self._sock is None:
            raise PercoError("not connected")
        length = len(payload)
        header = bytes([0x80 | opcode])
        mask_bit = 0x80
        if length < 126:
            header += bytes([mask_bit | length])
        elif length < 65536:
            header += bytes([mask_bit | 126]) + struct.pack("!H", length)
        else:
            header += bytes([mask_bit | 127]) + struct.pack("!Q", length)
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        self._sock.sendall(header + mask + masked)

    def _need(self, size: int) -> bytes:
        while len(self._buf) < size:
            if self._sock is None:
                raise PercoError("not connected")
            chunk = self._sock.recv(65536)
            if not chunk:
                raise PercoError("connection closed by controller")
            self._buf += chunk
        data, self._buf = self._buf[:size], self._buf[size:]
        return data

    def _recv_frame(self) -> tuple[int, bytes]:
        first, second = self._need(2)
        opcode = first & 0x0F
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._need(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._need(8))[0]
        mask = self._need(4) if second & 0x80 else None
        payload = self._need(length)
        if mask:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        return opcode, payload

    def send_json(self, message: dict) -> None:
        data = json.dumps(message, ensure_ascii=False).encode("utf-8") + b"\n\n"
        self._send_frame(OP_TEXT, data)

    def recv_json(self, timeout: float | None = None) -> dict:
        """Read the next JSON document.

        The controller may put several ``json\\n\\n`` documents into one
        WebSocket frame, and one document may be split across frames, so the
        decoded text is buffered and split on the ``\\n\\n`` separator.
        """
        if self._sock is None:
            raise PercoError("not connected")
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            if "\n\n" in self._text_buf:
                document, self._text_buf = self._text_buf.split("\n\n", 1)
                document = document.strip()
                if not document:
                    continue
                try:
                    return json.loads(document)
                except json.JSONDecodeError:
                    log.warning("controller sent invalid JSON: %r", document[:200])
                    continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("no message from controller")
            self._sock.settimeout(remaining)
            opcode, payload = self._recv_frame()
            if opcode == OP_PING:
                self._send_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            if opcode == OP_CLOSE:
                raise PercoError("controller closed the WebSocket")
            if opcode != OP_TEXT:
                continue
            text = payload.decode("utf-8", errors="replace")
            if text:
                self._text_buf += text

    # ---------------------------------------------------------------- protocol
    def _authenticate(self, message: dict) -> None:
        if not self.password:
            raise PercoError("controller requested a password but TURNSTILE_PASSWORD is empty")
        salt = (message.get("need_auth") or {}).get("salt", "")
        digest = hashlib.md5((str(salt) + self.password).encode("utf-8")).hexdigest()
        self.send_json({"set": "auth", "auth": {"hash": digest}})
        self._authenticated = True

    def _query(self, request: dict, predicate, timeout: float | None = None) -> list[dict]:
        """Send a read request and collect responses matching ``predicate``."""
        self.send_json(request)
        collected: list[dict] = []
        idle_deadline = time.monotonic() + (self.idle_timeout if timeout is None else timeout)
        while time.monotonic() < idle_deadline:
            remaining = idle_deadline - time.monotonic()
            try:
                message = self.recv_json(max(0.05, remaining))
            except socket.timeout:
                break
            if message.get("event") == "need_auth":
                self._authenticate(message)
                continue
            if message.get("answer", {}).get("auth") == "ok":
                continue
            if predicate(message):
                collected.append(message)
                break
        return collected

    def sysinfo(self) -> dict:
        messages = self._query({"get": "sysinfo"}, lambda m: "sysinfo" in m, timeout=3.0)
        for message in messages:
            if isinstance(message.get("sysinfo"), dict):
                return message["sysinfo"]
        raise PercoError("controller did not return sysinfo")

    def network_info(self) -> dict:
        messages = self._query({"get": "curnetwork"}, lambda m: "curnetwork" in m, timeout=3.0)
        for message in messages:
            if isinstance(message.get("curnetwork"), dict):
                return message["curnetwork"]
        return {}

    def event_count(self) -> int | None:
        messages = self._query({"get": "event_count"}, lambda m: "event_count" in m, timeout=3.0)
        for message in messages:
            value = message.get("event_count")
            if isinstance(value, dict) and "value" in value:
                return int(value["value"])
        return None

    def events(self, *, limit: int = 50, offset: int = 0, start: int = 0,
               stop: int = 9_999_999_999, categories: list[int] | None = None,
               expected: int | None = None, timeout: float | None = None) -> list[dict]:
        """Fetch events, newest first.

        The controller streams one event per WebSocket frame and does not send
        an explicit "end of page" mark, so collection stops after an idle gap
        or when ``limit`` events have arrived.
        """
        categories = [0, 1, 2, 3, 4, 5] if categories is None else categories
        request = {
            "get": "event",
            "event": {
                "limit": limit,
                "offset": offset,
                "category": categories,
                "start": start,
                "stop": stop,
            },
        }
        target = min(limit, expected) if expected is not None else limit
        self.send_json(request)
        events: list[dict] = []
        idle_deadline = time.monotonic() + (self.idle_timeout if timeout is None else timeout)
        while len(events) < target and time.monotonic() < idle_deadline:
            remaining = idle_deadline - time.monotonic()
            try:
                message = self.recv_json(max(0.05, remaining))
            except socket.timeout:
                break
            if message.get("event") == "need_auth":
                self._authenticate(message)
                continue
            answer = message.get("answer") or {}
            if answer.get("event") == "empty":
                break
            payload = message.get("event")
            if isinstance(payload, dict) and payload.get("date") is not None and payload.get("code") is not None:
                events.append(payload)
            elif payload == "empty":
                break
            idle_deadline = time.monotonic() + (self.idle_timeout if timeout is None else timeout)
        return events
