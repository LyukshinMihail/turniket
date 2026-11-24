"""Supervises one collector thread per configured turnstile."""

from __future__ import annotations

import logging
import threading
from typing import Callable

from .collector import Collector
from .store import Store

log = logging.getLogger(__name__)


class DeviceManager:
    def __init__(
        self,
        store: Store,
        *,
        on_events: Callable[[int, list[dict]], None],
        poll_interval: float,
        backfill_days: int,
        categories: list[int],
        proxy_map: dict[tuple[str, int], tuple[str, int]] | None = None,
        relay_endpoint: tuple[str, int] | None = None,
        relay_token: str = "",
    ):
        self.store = store
        self.on_events = on_events
        self.poll_interval = poll_interval
        self.backfill_days = backfill_days
        self.categories = categories
        self.proxy_map = proxy_map or {}
        self.relay_endpoint = relay_endpoint
        self.relay_token = relay_token
        self._collectors: dict[int, Collector] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ lifecycle
    def start_all(self) -> None:
        for device in self.store.list_devices(include_password=True):
            if device["host"]:
                self.start(device)

    def start(self, device: dict) -> None:
        device_id = int(device["id"])
        with self._lock:
            existing = self._collectors.get(device_id)
        if existing is not None:
            self.stop(device_id)
        collector = Collector(
            self.store,
            device_id=device_id,
            device_name=device["name"],
            host=device["host"],
            port=int(device["port"]),
            password=device.get("password", ""),
            poll_interval=self.poll_interval,
            backfill_days=self.backfill_days,
            categories=self.categories,
            proxy=self.proxy_map.get((device["host"], int(device["port"]))),
            relay=self.relay_endpoint,
            relay_token=self.relay_token,
            on_events=lambda events, did=device_id: self.on_events(did, events),
        )
        with self._lock:
            self._collectors[device_id] = collector
        collector.start()
        log.info("collector for device %s (%s) started", device_id, device["host"])

    def stop(self, device_id: int) -> None:
        with self._lock:
            collector = self._collectors.pop(device_id, None)
        if collector is not None:
            collector.stop()
            collector.join(timeout=3)
            log.info("collector for device %s stopped", device_id)

    def restart(self, device: dict) -> None:
        self.stop(int(device["id"]))
        if device.get("host"):
            self.start(device)

    def stop_all(self) -> None:
        with self._lock:
            device_ids = list(self._collectors)
        for device_id in device_ids:
            self.stop(device_id)

    def trigger_all(self) -> None:
        with self._lock:
            collectors = list(self._collectors.values())
        for collector in collectors:
            collector.trigger_sync()

    # -------------------------------------------------------------------- status
    def status_for(self, device_id: int) -> dict | None:
        device = self.store.get_device(device_id)
        if device is None:
            return None
        with self._lock:
            collector = self._collectors.get(device_id)
        state = collector.status() if collector else {
            "connected": False,
            "last_sync_at": None,
            "last_error": None,
            "device": None,
            "received_total": 0,
        }
        state.update(device)
        stats = self.store.device_event_stats().get(device_id, {})
        state["events"] = stats.get("total", 0)
        state["passes"] = stats.get("passes", 0)
        state["last_event_at"] = stats.get("last_event_at")
        state["timezone_offset"] = stats.get("timezone_offset", 0)
        return state

    def statuses(self) -> list[dict]:
        stats = self.store.device_event_stats()
        result = []
        for device in self.store.list_devices():
            with self._lock:
                collector = self._collectors.get(int(device["id"]))
            state = collector.status() if collector else {
                "connected": False,
                "last_sync_at": None,
                "last_error": "collector is not running",
                "device": None,
                "received_total": 0,
            }
            state.update(device)
            device_stats = stats.get(int(device["id"]), {})
            state["events"] = device_stats.get("total", 0)
            state["passes"] = device_stats.get("passes", 0)
            state["last_event_at"] = device_stats.get("last_event_at")
            state["timezone_offset"] = device_stats.get("timezone_offset", 0)
            result.append(state)
        return result
