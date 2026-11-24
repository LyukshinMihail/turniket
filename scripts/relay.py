#!/usr/bin/env python3
"""Header-routing TCP relay for isolated turnstiles.

One listener serves any number of controllers. The client opens a TCP
connection to the relay, sends the usual HTTP/WebSocket handshake plus:

    X-Target-Host: 10.3.198.200:80
    X-Relay-Token: <optional shared secret>

The relay validates the target against an allowlist, connects to it, rewrites
the ``Host`` header and transparently tunnels the WebSocket from there.

Safety:
- refuses to start without ``RELAY_ALLOWED_NETWORKS`` (fail closed);
- allows only ports from ``RELAY_ALLOWED_PORTS`` (default 80);
- resolves the target and requires *every* resolved address to be inside the
  allowed networks, then connects to the validated numeric address;
- optional shared token ``TURNSTILE_RELAY_TOKEN``.

Configuration (reads .env next to the repo, environment wins):

    TURNSTILE_RELAY_ENDPOINT=host.docker.internal:9000   # port to listen on
    TURNSTILE_RELAY_TOKEN=                                # optional
    RELAY_ALLOWED_NETWORKS=10.3.198.0/24
    RELAY_ALLOWED_PORTS=80
    RELAY_LISTEN_HOST=0.0.0.0

Usage:
    python3 scripts/relay.py
    python3 scripts/relay.py --allow 10.3.198.0/24 --ports 80
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import ipaddress
import os
import socket
import sys
from pathlib import Path

HEADER_LIMIT = 16 * 1024
HEADER_TIMEOUT = 5.0
CONNECT_TIMEOUT = 5.0
DEFAULT_PORT = 9000

DENY_TEXT = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    502: "Bad Gateway",
}


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    for key in (
        "TURNSTILE_RELAY_ENDPOINT", "TURNSTILE_RELAY_TOKEN",
        "RELAY_ALLOWED_NETWORKS", "RELAY_ALLOWED_PORTS",
        "RELAY_LISTEN_HOST", "RELAY_MAX_CONNECTIONS",
    ):
        if key in os.environ:
            values[key] = os.environ[key]
    return values


def parse_host_port(value: str, default_port: int = DEFAULT_PORT) -> tuple[str, int]:
    value = value.strip()
    if not value:
        raise ValueError("empty address")
    if value.startswith("["):
        host, _, rest = value[1:].partition("]")
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return host, default_port
    if value.count(":") == 1:
        host, maybe_port = value.rsplit(":", 1)
        if maybe_port.isdigit():
            return host.strip(), int(maybe_port)
    return value, default_port


def parse_networks(raw: str) -> list[ipaddress._BaseNetwork]:
    networks = []
    for item in raw.replace(" ", "").split(","):
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            print(f"skip invalid network: {item}", file=sys.stderr)
    return networks


def parse_ports(raw: str) -> set[int]:
    ports = set()
    for item in raw.replace(" ", "").split(","):
        if item.isdigit() and 1 <= int(item) <= 65535:
            ports.add(int(item))
    return ports or {80}


async def deny(writer: asyncio.StreamWriter, status: int, message: str) -> None:
    body = message.encode("utf-8")
    writer.write(
        f"HTTP/1.1 {status} {DENY_TEXT.get(status, 'Denied')}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n".encode()
        + body
    )
    try:
        await writer.drain()
    except ConnectionError:
        pass
    writer.close()


async def read_headers(reader: asyncio.StreamReader) -> tuple[bytes, bytes] | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + HEADER_TIMEOUT
    data = b""
    while b"\r\n\r\n" not in data:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("header timeout")
        chunk = await asyncio.wait_for(reader.read(2048), remaining)
        if not chunk:
            return None
        data += chunk
        if len(data) > HEADER_LIMIT:
            raise ValueError("headers too large")
    head, rest = data.split(b"\r\n\r\n", 1)
    return head, rest


def parse_header_lines(raw: bytes) -> tuple[str, list[tuple[str, str]]]:
    lines = raw.decode("latin-1").split("\r\n")
    request_line = lines[0] if lines else ""
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers.append((name.strip(), value.strip()))
    return request_line, headers


def header_value(headers: list[tuple[str, str]], name: str) -> str | None:
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def is_allowed(ip: ipaddress._BaseAddress, networks: list[ipaddress._BaseNetwork]) -> bool:
    return any(ip in network for network in networks)


async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter,
                 networks: list[ipaddress._BaseNetwork], ports: set[int], token: str) -> None:
    peer = client_writer.get_extra_info("peername")
    try:
        try:
            parsed = await read_headers(client_reader)
        except (TimeoutError, ValueError) as exc:
            await deny(client_writer, 400, f"Bad request: {exc}")
            return
        if parsed is None:
            client_writer.close()
            return
        head, rest = parsed
        request_line, headers = parse_header_lines(head)

        if token:
            provided = header_value(headers, "x-relay-token") or ""
            if not hmac.compare_digest(provided, token):
                await deny(client_writer, 401, "Invalid relay token")
                return

        target_value = header_value(headers, "x-target-host")
        if not target_value:
            await deny(client_writer, 400, "Missing X-Target-Host header")
            return
        try:
            target_host, target_port = parse_host_port(target_value, 80)
        except ValueError:
            await deny(client_writer, 400, "Invalid X-Target-Host")
            return
        if target_port not in ports:
            await deny(client_writer, 403, f"Port {target_port} is not allowed")
            return

        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(target_host, target_port, type=socket.SOCK_STREAM)
        except socket.gaierror:
            await deny(client_writer, 403, "Cannot resolve target")
            return
        addresses: list[str] = []
        for info in infos:
            address = str(info[4][0]).split("%", 1)[0]
            if address not in addresses:
                addresses.append(address)
        if not addresses:
            await deny(client_writer, 403, "Cannot resolve target")
            return
        try:
            parsed_addresses = [ipaddress.ip_address(address) for address in addresses]
        except ValueError:
            await deny(client_writer, 403, "Invalid target address")
            return
        if any(not is_allowed(address, networks) for address in parsed_addresses):
            await deny(client_writer, 403, "Target is outside the allowed networks")
            return

        target_ip = addresses[0]
        try:
            remote_reader, remote_writer = await asyncio.wait_for(
                asyncio.open_connection(target_ip, target_port), CONNECT_TIMEOUT
            )
        except (OSError, asyncio.TimeoutError):
            await deny(client_writer, 502, "Cannot connect to target")
            return

        forwarded_headers = [
            (name, value) for name, value in headers
            if name.lower() not in {"x-target-host", "x-relay-token", "host"}
        ]
        host_value = target_host if target_port == 80 else f"{target_host}:{target_port}"
        forwarded_headers.insert(0, ("Host", host_value))
        new_head = request_line + "\r\n" + "\r\n".join(f"{k}: {v}" for k, v in forwarded_headers) + "\r\n\r\n"
        remote_writer.write(new_head.encode("latin-1") + rest)
        await remote_writer.drain()
        local = remote_writer.get_extra_info("sockname")
        local_ip = local[0] if local else "?"
        print(
            f"{peer} -> {target_host}:{target_port} ({target_ip}) via {local_ip}",
            flush=True,
        )

        await asyncio.gather(
            pipe(client_reader, remote_writer),
            pipe(remote_reader, client_writer),
        )
    except (ConnectionError, asyncio.CancelledError):
        pass
    except Exception as exc:  # pragma: no cover - defensive
        print(f"relay error: {type(exc).__name__}: {exc}", file=sys.stderr)
        try:
            client_writer.close()
        except Exception:
            pass


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def serve(listen_host: str, listen_port: int, networks: list[ipaddress._BaseNetwork],
                ports: set[int], token: str, max_connections: int) -> None:
    semaphore = asyncio.Semaphore(max_connections)

    async def limited(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with semaphore:
            await handle(reader, writer, networks, ports, token)

    server = await asyncio.start_server(limited, listen_host, listen_port)
    print(
        f"relay listening on {listen_host}:{listen_port}; "
        f"networks={','.join(str(n) for n in networks)}; "
        f"ports={','.join(str(p) for p in sorted(ports))}; "
        f"token={'on' if token else 'off'}",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def main() -> None:
    env = load_env(Path(__file__).resolve().parent.parent / ".env")
    parser = argparse.ArgumentParser(description="Header-routing relay for isolated turnstiles")
    parser.add_argument("--listen", default=None, help="bind address:port (default: from .env)")
    parser.add_argument("--allow", default=env.get("RELAY_ALLOWED_NETWORKS", ""),
                        help="comma-separated CIDRs allowed as targets")
    parser.add_argument("--ports", default=env.get("RELAY_ALLOWED_PORTS", "80"),
                        help="comma-separated allowed target ports (default: 80)")
    parser.add_argument("--token", default=env.get("TURNSTILE_RELAY_TOKEN", ""),
                        help="optional shared token")
    args = parser.parse_args()

    networks = parse_networks(args.allow)
    if not networks:
        parser.error(
            "RELAY_ALLOWED_NETWORKS is empty; refusing to start an unrestricted relay"
        )
    ports = parse_ports(args.ports)

    if args.listen:
        listen_host, listen_port = parse_host_port(args.listen, DEFAULT_PORT)
    else:
        endpoint = env.get("TURNSTILE_RELAY_ENDPOINT", "")
        try:
            _, listen_port = parse_host_port(endpoint, DEFAULT_PORT)
        except ValueError:
            listen_port = DEFAULT_PORT
        listen_host = env.get("RELAY_LISTEN_HOST", "0.0.0.0")

    max_connections = int(env.get("RELAY_MAX_CONNECTIONS", "64") or 64)
    try:
        asyncio.run(serve(listen_host, listen_port, networks, ports, args.token, max_connections))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
