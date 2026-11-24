"""Background collector: polls the turnstile and stores new events."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from .perco import PercoClient, PercoError
from .store import Store

log = logging.getLogger(__name__)

PAGE_SIZE = 100
OVERLAP_SECONDS = 120
RETRY_SECONDS = 5
MAX_BACKFILL = 20_000


class Collector(threading.Thread):
    def __init__(
        self,
        store: Store,
        *,
        device_id: int,
        device_name: str,
        host: str,
        port: int = 80,
        password: str = "",
        poll_interval: float = 3.0,
        backfill_days: int = 30,
        categories: list[int] | None = None,
        proxy: tuple[str, int] | None = None,
        relay: tuple[str, int] | None = None,
        relay_token: str = "",
        on_events: Callable[[list[dict]], None] | None = None,
    ):
        super().__init__(name=f"perco-collector-{device_id}", daemon=True)
        self.store = store
        self.device_id = device_id
        self.device_name = device_name
        self.host = host
        self.port = port
        if relay is not None:
            self.connect_host, self.connect_port = relay
        else:
            self.connect_host, self.connect_port = proxy if proxy else (host, port)
        self.extra_headers: dict[str, str] = {}
        if relay is not None:
            target = f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
            self.extra_headers["X-Target-Host"] = target
            if relay_token:
                self.extra_headers["X-Relay-Token"] = relay_token
        self.password = password
        self.poll_interval = max(0.5, poll_interval)
        self.backfill_days = backfill_days
        self.categories = [0] if categories is None else list(categories)
        self.on_events = on_events

        self._client: PercoClient | None = None
        self._stop_event = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._state = {
            "connected": False,
            "last_sync_at": None,
            "last_error": None,
            "device": None,
            "received_total": 0,
        }

    # ------------------------------------------------------------------ public
    def stop(self) -> None:
        self._stop_event.set()
        self._wake.set()
        if self._client:
            self._client.close()

    def trigger_sync(self) -> None:
        self._wake.set()

    def status(self) -> dict:
        with self._lock:
            state = dict(self._state)
        state["host"] = self.host
        state["port"] = self.port
        state["device_id"] = self.device_id
        state["device_name"] = self.device_name
        state["poll_interval"] = self.poll_interval
        state["categories"] = self.categories
        return state

    def _set_state(self, **kwargs) -> None:
        with self._lock:
            self._state.update(kwargs)

    # -------------------------------------------------------------------- loop
    def run(self) -> None:
        if not self.host:
            self._set_state(last_error="TURNSTILE_HOST is not configured")
            return

        backoff = RETRY_SECONDS
        while not self._stop_event.is_set():
            try:
                self._ensure_connected()
                self._sync()
                self._set_state(connected=True, last_error=None)
                backoff = RETRY_SECONDS
                self._wake.wait(self.poll_interval)
                self._wake.clear()
            except (PercoError, OSError) as exc:
                self._set_state(connected=False, last_error=str(exc))
                log.warning("turnstile connection problem: %s", exc)
                self._disconnect()
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as exc:  # pragma: no cover - defensive
                self._set_state(connected=False, last_error=f"{type(exc).__name__}: {exc}")
                log.exception("collector error")
                self._disconnect()
                self._stop_event.wait(RETRY_SECONDS)

    def _ensure_connected(self) -> None:
        if self._client and self._client.connected:
            return
        client = PercoClient(
            self.connect_host,
            self.connect_port,
            password=self.password,
            idle_timeout=0.6,
            extra_headers=self.extra_headers,
        )
        client.connect()
        info = client.sysinfo()
        try:
            info.update(client.network_info())
        except (PercoError, OSError) as exc:
            log.debug("curnetwork unavailable: %s", exc)
        self._client = client
        self._set_state(device=info, connected=True)
        log.info(
            "connected to %s (%s) via %s:%s",
            self.host, info.get("Product", "PERCo"), self.connect_host, self.connect_port,
        )

    def _disconnect(self) -> None:
        if self._client:
            self._client.close()
        self._client = None

    def _sync(self) -> None:
        if self.store.get_device(self.device_id) is None:
            self._stop_event.set()
            return
        assert self._client is not None
        now = int(time.time())

        if self.store.last_event_date(self.device_id) is None:
            self._backfill(now)

        last_date = self.store.last_event_date(self.device_id)
        start = (last_date - OVERLAP_SECONDS) if last_date else now - OVERLAP_SECONDS
        events = self._client.events(
            limit=PAGE_SIZE,
            offset=0,
            start=max(0, start),
            stop=now + 60,
            categories=self.categories,
        )
        self._persist(events)
        self._set_state(last_sync_at=int(time.time()))

    def _backfill(self, now: int) -> None:
        assert self._client is not None
        start = 0 if self.backfill_days <= 0 else now - self.backfill_days * 86400
        offset = 0
        total = 0
        log.info("importing event history (start=%s)", start)
        while offset < MAX_BACKFILL and not self._stop_event.is_set():
            batch = self._client.events(
                limit=PAGE_SIZE,
                offset=offset,
                start=start,
                stop=now + 60,
                categories=self.categories,
            )
            if not batch:
                break
            self._persist(batch)
            total += len(batch)
            offset += PAGE_SIZE
            if len(batch) < PAGE_SIZE:
                break
        self.store.set_meta("backfilled_at", int(time.time()))
        log.info("imported %s historical events", total)

    def _persist(self, events: list[dict]) -> None:
        if self.store.get_device(self.device_id) is None:
            self._stop_event.set()
            return
        events = [
            event for event in events
            if event.get("date") is not None and event.get("code") is not None
        ]
        if not events:
            return
        # The controller streams newest-first; store oldest-first for stable notifications.
        ordered = sorted(events, key=lambda e: (int(e.get("date") or 0), str(e.get("id"))))
        inserted = self.store.insert_events(self.device_id, ordered)
        if not inserted:
            return
        for event in inserted:
            event["device_id"] = self.device_id
            event["device_name"] = self.device_name
        with self._lock:
            self._state["received_total"] = int(self._state["received_total"]) + len(inserted)
        log.info("stored %s new events", len(inserted))
        if self.on_events:
            try:
                self.on_events(inserted)
            except Exception:  # pragma: no cover - listeners must not break polling
                log.exception("event listener failed")
