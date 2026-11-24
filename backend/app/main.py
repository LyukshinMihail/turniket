"""Turnstile monitoring service: HTTP API, live stream, device management, UI."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .manager import DeviceManager
from .store import Store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("turnstile")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

TURNSTILE_HOST = os.getenv("TURNSTILE_HOST", "").strip()
TURNSTILE_NAME = os.getenv("TURNSTILE_NAME", "").strip() or "Проходная 1"
TURNSTILE_PORT = int(os.getenv("TURNSTILE_PORT", "80"))
TURNSTILE_PASSWORD = os.getenv("TURNSTILE_PASSWORD", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2"))
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "30"))
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR.parent / "data" / "turnstile.db"))


def parse_categories(raw: str) -> list[int]:
    """Event categories to collect: 0 — access by identifier, 1 — access without id, etc."""
    values = sorted({int(part) for part in raw.replace(" ", "").split(",") if part.isdigit()})
    return values or [0]


EVENT_CATEGORIES = parse_categories(os.getenv("EVENT_CATEGORIES", "0"))


def parse_proxy_map(raw: str) -> dict[tuple[str, int], tuple[str, int]]:
    """Connection routes: ``real-host[:port]=proxy-host:port`` pairs, comma-separated.

    Lets a container reach controllers that are only available through a host
    relay, while the device keeps its real address in the database.
    """
    mapping: dict[tuple[str, int], tuple[str, int]] = {}

    def split_addr(value: str, default_port: int) -> tuple[str, int]:
        value = value.strip()
        if value.count(":") == 1:
            host, maybe_port = value.rsplit(":", 1)
            if maybe_port.isdigit():
                return host.strip(), int(maybe_port)
        return value, default_port

    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or "=" not in entry:
            continue
        left, right = entry.split("=", 1)
        real_host, real_port = split_addr(left, 80)
        proxy_host, proxy_port = split_addr(right, 80)
        if real_host and proxy_host:
            mapping[(real_host, real_port)] = (proxy_host, proxy_port)
    return mapping


TURNSTILE_PROXY_MAP = parse_proxy_map(os.getenv("TURNSTILE_PROXY_MAP", ""))


def parse_endpoint(raw: str, default_port: int) -> tuple[str, int] | None:
    value = raw.strip()
    if not value:
        return None
    host, port = value, default_port
    if value.count(":") == 1:
        maybe_host, maybe_port = value.rsplit(":", 1)
        if maybe_port.isdigit():
            host, port = maybe_host.strip(), int(maybe_port)
    return (host, port) if host else None


TURNSTILE_RELAY_ENDPOINT = parse_endpoint(os.getenv("TURNSTILE_RELAY_ENDPOINT", ""), 9000)
TURNSTILE_RELAY_TOKEN = os.getenv("TURNSTILE_RELAY_TOKEN", "")


class Broadcaster:
    """Fan-out of new events from collector threads to SSE clients."""

    def __init__(self) -> None:
        self._clients: set[asyncio.Queue] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._clients.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._clients.discard(queue)

    def publish(self, device_id: int, events: list[dict]) -> None:
        if self._loop is None or not events:
            return
        device = store.get_device(device_id) or {"id": device_id, "name": ""}

        def _deliver() -> None:
            payload = {
                "type": "events",
                "device": {"id": device["id"], "name": device["name"]},
                "events": events,
                "server_time": int(time.time()),
            }
            for queue in list(self._clients):
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

        self._loop.call_soon_threadsafe(_deliver)


store = Store(DB_PATH)
broadcaster = Broadcaster()
manager = DeviceManager(
    store,
    on_events=broadcaster.publish,
    poll_interval=POLL_INTERVAL,
    backfill_days=BACKFILL_DAYS,
    categories=EVENT_CATEGORIES,
    proxy_map=TURNSTILE_PROXY_MAP,
    relay_endpoint=TURNSTILE_RELAY_ENDPOINT,
    relay_token=TURNSTILE_RELAY_TOKEN,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    broadcaster.bind_loop(asyncio.get_running_loop())
    removed = store.purge_other_categories(EVENT_CATEGORIES)
    if removed:
        log.info("removed %s events outside categories %s", removed, EVENT_CATEGORIES)
    store.seed_first_device(TURNSTILE_NAME, TURNSTILE_HOST, TURNSTILE_PORT, TURNSTILE_PASSWORD)
    manager.start_all()
    log.info("service started, categories=%s, devices=%s", EVENT_CATEGORIES, len(store.list_devices()))
    try:
        yield
    finally:
        manager.stop_all()


app = FastAPI(title="Turnstile Monitor", version="2.0.0", lifespan=lifespan)


@app.middleware("http")
async def disable_caching(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/api") or path.startswith("/static"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


class DeviceIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(80, ge=1, le=65535)
    password: str = Field("", max_length=128)


class DevicePatch(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=64)
    host: str | None = Field(None, min_length=1, max_length=255)
    port: int | None = Field(None, ge=1, le=65535)
    password: str | None = Field(None, max_length=128)


def normalize_address(raw: str, fallback_port: int) -> tuple[str, int]:
    value = raw.strip()
    for prefix in ("http://", "https://", "ws://", "wss://"):
        if value.lower().startswith(prefix):
            value = value[len(prefix):]
            break
    value = value.split("/", 1)[0].strip()
    port = fallback_port
    if value and value.count(":") == 1:
        host, maybe_port = value.rsplit(":", 1)
        if maybe_port.isdigit():
            value, port = host.strip(), int(maybe_port)
    if not value or any(char.isspace() for char in value):
        raise HTTPException(status_code=422, detail="Некорректный адрес проходной")
    if not 1 <= port <= 65535:
        raise HTTPException(status_code=422, detail="Некорректный порт")
    return value, port


def start_of_today(device_id: int | None = None) -> int:
    """Midnight of the current day in the controller's timezone."""
    tz_offset = store.timezone_offset(device_id)
    now = int(time.time())
    local_seconds = now - tz_offset
    midnight_local = local_seconds - (local_seconds % 86400)
    return midnight_local + tz_offset


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/status")
def status() -> dict:
    devices = manager.statuses()
    events = sum(device["events"] for device in devices)
    last_sync = max((device["last_sync_at"] or 0) for device in devices) if devices else 0
    return {
        "connected": any(device["connected"] for device in devices),
        "devices": devices,
        "events_stored": events,
        "last_sync_at": last_sync or None,
        "server_time": int(time.time()),
        "categories": EVENT_CATEGORIES,
    }


# ------------------------------------------------------------------- devices
@app.get("/api/devices")
def list_devices() -> dict:
    items = manager.statuses()
    return {"items": items, "total": len(items)}


@app.get("/api/devices/{device_id}")
def get_device(device_id: int) -> dict:
    state = manager.status_for(device_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Проходная не найдена")
    return state


@app.post("/api/devices", status_code=201)
def add_device(payload: DeviceIn) -> dict:
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Укажите название проходной")
    host, port = normalize_address(payload.host, payload.port)
    try:
        device = store.create_device(name, host, port, payload.password)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=409, detail="Проходная с таким адресом и портом уже добавлена"
        ) from exc
    full = store.get_device(device["id"], include_password=True)
    manager.start(full)
    return manager.status_for(device["id"]) or device


@app.patch("/api/devices/{device_id}")
def edit_device(device_id: int, payload: DevicePatch) -> dict:
    existing = store.get_device(device_id, include_password=True)
    if existing is None:
        raise HTTPException(status_code=404, detail="Проходная не найдена")

    fields: dict = {}
    if payload.name is not None:
        name = payload.name.strip()
        if not name:
            raise HTTPException(status_code=422, detail="Укажите название проходной")
        fields["name"] = name
    if payload.host is not None:
        host, port = normalize_address(payload.host, payload.port or existing["port"])
        fields["host"] = host
        fields["port"] = port
    elif payload.port is not None:
        fields["port"] = payload.port
    if payload.password is not None:
        fields["password"] = payload.password

    if not fields:
        return manager.status_for(device_id) or existing
    try:
        store.update_device(device_id, fields)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=409, detail="Проходная с таким адресом и портом уже добавлена"
        ) from exc

    manager.stop(device_id)
    updated = store.get_device(device_id, include_password=True)
    if updated is not None:
        manager.start(updated)
    return manager.status_for(device_id) or {}


@app.delete("/api/devices/{device_id}")
def remove_device(device_id: int) -> dict:
    existing = store.get_device(device_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Проходная не найдена")
    manager.stop(device_id)
    deleted = store.delete_device(device_id)
    return {"ok": True, "device": existing, "deleted_events": deleted["events"]}


# -------------------------------------------------------------------- events
@app.get("/api/events")
def events(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    device_id: int | None = None,
    q: str | None = None,
    direction: str | None = Query(None, pattern="^(in|out)$"),
    passes_only: bool = False,
    since: int | None = None,
    until: int | None = None,
) -> dict:
    items = store.list_events(
        device_id=device_id,
        limit=limit,
        offset=offset,
        q=q,
        direction=direction,
        passes_only=passes_only,
        since=since,
        until=until,
    )
    total = store.count_events(
        device_id=device_id, q=q, direction=direction, passes_only=passes_only,
        since=since, until=until,
    )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/codes")
def codes(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    device_id: int | None = None,
    q: str | None = None,
    since: int | None = None,
    until: int | None = None,
    passes_only: bool = Query(True),
) -> dict:
    items, total = store.list_codes(
        device_id=device_id, limit=limit, offset=offset, q=q,
        since=since, until=until, passes_only=passes_only,
    )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/stats")
def stats(period: str = Query("today", pattern="^(today|all)$"), device_id: int | None = None) -> dict:
    since = start_of_today(device_id) if period == "today" else None
    data = store.stats(since=since, device_id=device_id)
    data["last_pass"] = store.last_pass(device_id)
    data["period"] = period
    return data


@app.post("/api/sync")
def sync() -> dict:
    manager.trigger_all()
    return {"ok": True}


@app.get("/api/stream")
async def stream(request: Request) -> StreamingResponse:
    queue = broadcaster.subscribe()

    async def generator():
        yield "retry: 3000\n\n"
        yield "event: ready\ndata: {}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield "event: events\ndata: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
        finally:
            broadcaster.unsubscribe(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
