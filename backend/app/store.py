"""SQLite persistence for turnstiles, events and aggregated codes."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from typing import Any, Iterable

# Event codes that mean "the person actually went through".
PASS_CODES = (11, 96, 97, 98)
PASS_SQL = "(" + ",".join(str(c) for c in PASS_CODES) + ")"

DEVICES_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS devices (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    host       TEXT NOT NULL,
    port       INTEGER NOT NULL DEFAULT 80,
    password   TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_devices_host_port ON devices(host, port);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    device_id       INTEGER NOT NULL,
    id              TEXT NOT NULL,
    date            INTEGER NOT NULL,
    timezone_offset INTEGER NOT NULL DEFAULT 0,
    code            INTEGER,
    category        INTEGER,
    number          INTEGER,
    dir             INTEGER,
    card            TEXT,
    uid             TEXT,
    id_type         TEXT,
    user_type       TEXT,
    reason          TEXT,
    verification    TEXT,
    raw             TEXT NOT NULL,
    received_at     INTEGER NOT NULL,
    PRIMARY KEY (device_id, id)
);
"""

EVENTS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_events_date        ON events(date DESC);
CREATE INDEX IF NOT EXISTS idx_events_device_date ON events(device_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_events_card        ON events(card);
CREATE INDEX IF NOT EXISTS idx_events_uid         ON events(uid);
CREATE INDEX IF NOT EXISTS idx_events_code        ON events(code);
"""


class Store:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(DEVICES_SCHEMA)
            self._migrate_events()
            self._conn.executescript(EVENTS_TABLE)
            self._conn.executescript(EVENTS_INDEXES)
            self._conn.commit()

    def _migrate_events(self) -> None:
        """Upgrade the single-device schema to a per-device events table."""
        table = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone()
        if not table:
            return
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(events)")}
        if "device_id" in columns:
            return

        legacy_columns = (
            "id, date, timezone_offset, code, category, number, dir, card, uid, "
            "id_type, user_type, reason, verification, raw, received_at"
        )
        self._conn.executescript(
            "DROP INDEX IF EXISTS idx_events_date;"
            "DROP INDEX IF EXISTS idx_events_card;"
            "DROP INDEX IF EXISTS idx_events_uid;"
            "DROP INDEX IF EXISTS idx_events_code;"
            "ALTER TABLE events RENAME TO events_legacy;"
        )
        self._conn.executescript(EVENTS_TABLE)
        self._conn.execute(
            f"INSERT INTO events (device_id, {legacy_columns}) "
            f"SELECT 1, {legacy_columns} FROM events_legacy"
        )
        self._conn.execute("DROP TABLE events_legacy")
        self._conn.execute(
            "INSERT OR IGNORE INTO devices(id, name, host, port, password, created_at) "
            "VALUES (1, 'Проходная 1', '', 80, '', ?)",
            (int(time.time()),),
        )
        self._conn.commit()

    # ------------------------------------------------------------------- meta
    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    # ---------------------------------------------------------------- devices
    @staticmethod
    def _device_row(row: sqlite3.Row, include_password: bool = False) -> dict:
        data = dict(row)
        if not include_password:
            data.pop("password", None)
        return data

    def list_devices(self, include_password: bool = False) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM devices ORDER BY id").fetchall()
        return [self._device_row(row, include_password) for row in rows]

    def get_device(self, device_id: int, include_password: bool = False) -> dict | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM devices WHERE id = ?", (device_id,)).fetchone()
        return self._device_row(row, include_password) if row else None

    def create_device(self, name: str, host: str, port: int, password: str = "") -> dict:
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO devices(name, host, port, password, created_at) VALUES(?, ?, ?, ?, ?)",
                (name, host, port, password, int(time.time())),
            )
            self._conn.commit()
            row = self._conn.execute("SELECT * FROM devices WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return self._device_row(row)

    def update_device(self, device_id: int, fields: dict) -> dict | None:
        allowed = {key: value for key, value in fields.items() if key in {"name", "host", "port", "password"}}
        if allowed:
            assignments = ", ".join(f"{key} = ?" for key in allowed)
            with self._lock:
                self._conn.execute(
                    f"UPDATE devices SET {assignments} WHERE id = ?",
                    (*allowed.values(), device_id),
                )
                self._conn.commit()
        return self.get_device(device_id)

    def delete_device(self, device_id: int) -> dict:
        with self._lock:
            events = self._conn.execute(
                "DELETE FROM events WHERE device_id = ?", (device_id,)
            ).rowcount
            self._conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
            self._conn.commit()
        return {"events": events}

    def seed_first_device(self, name: str, host: str, port: int, password: str = "") -> None:
        """Create the initial device once (from .env) or bind a legacy DB to it."""
        if self.get_meta("devices_seeded") == "1":
            return
        devices = self.list_devices(include_password=True)
        placeholder = next((d for d in devices if not d["host"]), None)
        if placeholder is not None and host:
            self.update_device(
                placeholder["id"],
                {"name": name or placeholder["name"], "host": host, "port": port, "password": password},
            )
        elif not devices and host:
            self.create_device(name or "Проходная 1", host, port, password)
        self.set_meta("devices_seeded", 1)

    def device_event_stats(self) -> dict[int, dict]:
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT device_id,
                           COUNT(*) AS total,
                           SUM(CASE WHEN code IN {PASS_SQL} THEN 1 ELSE 0 END) AS passes,
                           MAX(date) AS last_event_at,
                           MAX(timezone_offset) AS timezone_offset
                    FROM events GROUP BY device_id"""
            ).fetchall()
        return {
            int(row["device_id"]): {
                "total": int(row["total"] or 0),
                "passes": int(row["passes"] or 0),
                "last_event_at": int(row["last_event_at"]) if row["last_event_at"] is not None else None,
                "timezone_offset": int(row["timezone_offset"] or 0),
            }
            for row in rows
        }

    # ----------------------------------------------------------------- events
    def insert_events(self, device_id: int, events: Iterable[dict]) -> list[dict]:
        """Insert events for one device, ignoring duplicates."""
        inserted: list[dict] = []
        now = int(time.time())
        with self._lock:
            for event in events:
                event_id = event.get("id")
                if event_id is None:
                    event_id = self._synthetic_id(event)
                event_id = str(event_id)
                raw = json.dumps(event, ensure_ascii=False, sort_keys=True)
                try:
                    cursor = self._conn.execute(
                        """INSERT OR IGNORE INTO events
                           (device_id, id, date, timezone_offset, code, category, number, dir,
                            card, uid, id_type, user_type, reason, verification, raw, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            device_id,
                            event_id,
                            int(event.get("date") or 0),
                            int(event.get("timezone_offset") or 0),
                            event.get("code"),
                            event.get("category"),
                            event.get("number"),
                            event.get("dir"),
                            self._text(event.get("card")),
                            self._text(event.get("uid")),
                            self._text(event.get("id_type")),
                            self._text(event.get("user_type")),
                            self._text(event.get("reason")),
                            self._text(event.get("verification")),
                            raw,
                            now,
                        ),
                    )
                except sqlite3.Error:
                    continue
                if cursor.rowcount:
                    row = dict(event)
                    row["id"] = event_id
                    row["device_id"] = device_id
                    row["received_at"] = now
                    inserted.append(row)
            self._conn.commit()
        return inserted

    @staticmethod
    def _text(value: Any) -> str | None:
        return None if value is None else str(value)

    @staticmethod
    def _synthetic_id(event: dict) -> str:
        payload = json.dumps(event, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return "h" + hashlib.sha1(payload).hexdigest()

    def last_event_date(self, device_id: int) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(date) AS d FROM events WHERE device_id = ?", (device_id,)
            ).fetchone()
        return int(row["d"]) if row and row["d"] is not None else None

    def timezone_offset(self, device_id: int | None = None) -> int:
        query = "SELECT timezone_offset FROM events"
        params: list[Any] = []
        if device_id is not None:
            query += " WHERE device_id = ?"
            params.append(device_id)
        query += " ORDER BY date DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(query, params).fetchone()
        return int(row["timezone_offset"]) if row else 0

    def purge_other_categories(self, keep: list[int]) -> int:
        if not keep:
            return 0
        placeholders = ",".join("?" for _ in keep)
        with self._lock:
            cursor = self._conn.execute(
                f"DELETE FROM events WHERE category IS NULL OR category NOT IN ({placeholders})",
                keep,
            )
            self._conn.commit()
        return cursor.rowcount

    def count_events(self, *, device_id: int | None = None, q: str | None = None,
                     direction: str | None = None, passes_only: bool = False,
                     since: int | None = None, until: int | None = None) -> int:
        where, params = self._event_filters(device_id, q, direction, passes_only, since, until)
        with self._lock:
            row = self._conn.execute(f"SELECT COUNT(*) AS n FROM events e {where}", params).fetchone()
        return int(row["n"])

    def list_events(self, *, device_id: int | None = None, limit: int = 50, offset: int = 0,
                    q: str | None = None, direction: str | None = None,
                    passes_only: bool = False, since: int | None = None,
                    until: int | None = None) -> list[dict]:
        where, params = self._event_filters(device_id, q, direction, passes_only, since, until)
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT e.*, d.name AS device_name
                    FROM events e
                    LEFT JOIN devices d ON d.id = e.device_id
                    {where}
                    ORDER BY e.date DESC, e.id DESC
                    LIMIT ? OFFSET ?""",
                (*params, limit, offset),
            ).fetchall()
        return [self._row(row) for row in rows]

    @staticmethod
    def _event_filters(device_id, q, direction, passes_only, since, until):
        clauses: list[str] = []
        params: list[Any] = []
        if device_id is not None:
            clauses.append("e.device_id = ?")
            params.append(int(device_id))
        if q:
            like = f"%{q.strip()}%"
            clauses.append("(e.card LIKE ? OR e.uid LIKE ?)")
            params.extend([like, like])
        if direction == "in":
            clauses.append("e.dir = 0")
        elif direction == "out":
            clauses.append("e.dir = 1")
        if passes_only:
            clauses.append(f"e.code IN {PASS_SQL}")
        if since is not None:
            clauses.append("e.date >= ?")
            params.append(int(since))
        if until is not None:
            clauses.append("e.date <= ?")
            params.append(int(until))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params

    @staticmethod
    def _row(row: sqlite3.Row) -> dict:
        data = dict(row)
        data.pop("raw", None)
        return data

    def last_pass(self, device_id: int | None = None) -> dict | None:
        where = f"WHERE code IN {PASS_SQL}"
        params: list[Any] = []
        if device_id is not None:
            where += " AND device_id = ?"
            params.append(device_id)
        with self._lock:
            row = self._conn.execute(
                f"SELECT * FROM events {where} ORDER BY date DESC, id DESC LIMIT 1", params
            ).fetchone()
        return self._row(row) if row else None

    def stats(self, since: int | None = None, device_id: int | None = None) -> dict:
        clauses: list[str] = []
        params: list[Any] = []
        if device_id is not None:
            clauses.append("device_id = ?")
            params.append(device_id)
        if since is not None:
            clauses.append("date >= ?")
            params.append(int(since))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._lock:
            overall = self._conn.execute(
                f"""SELECT
                      COUNT(*) AS total,
                      SUM(CASE WHEN code IN {PASS_SQL} THEN 1 ELSE 0 END) AS passes,
                      SUM(CASE WHEN code IN {PASS_SQL} AND dir = 0 THEN 1 ELSE 0 END) AS entries,
                      SUM(CASE WHEN code IN {PASS_SQL} AND dir = 1 THEN 1 ELSE 0 END) AS exits,
                      COUNT(DISTINCT CASE WHEN code IN {PASS_SQL} THEN COALESCE(card, uid) END) AS unique_codes,
                      MAX(timezone_offset) AS timezone_offset
                    FROM events {where}""",
                params,
            ).fetchone()
            total_where = "WHERE device_id = ?" if device_id is not None else ""
            total_params: list[Any] = [device_id] if device_id is not None else []
            total_row = self._conn.execute(
                f"SELECT COUNT(*) AS n, MAX(date) AS last FROM events {total_where}",
                total_params,
            ).fetchone()
        return {
            "total": int(overall["total"] or 0),
            "passes": int(overall["passes"] or 0),
            "entries": int(overall["entries"] or 0),
            "exits": int(overall["exits"] or 0),
            "unique_codes": int(overall["unique_codes"] or 0),
            "timezone_offset": int(overall["timezone_offset"] or 0),
            "events_all_time": int(total_row["n"] or 0),
            "last_event_at": int(total_row["last"]) if total_row["last"] is not None else None,
        }

    # ------------------------------------------------------------------ codes
    def list_codes(self, *, device_id: int | None = None, limit: int = 100, offset: int = 0,
                   q: str | None = None, since: int | None = None, until: int | None = None,
                   passes_only: bool = False) -> tuple[list[dict], int]:
        clauses: list[str] = ["(card IS NOT NULL OR uid IS NOT NULL)"]
        params: list[Any] = []
        if device_id is not None:
            clauses.append("device_id = ?")
            params.append(int(device_id))
        if q:
            like = f"%{q.strip()}%"
            clauses.append("(card LIKE ? OR uid LIKE ?)")
            params.extend([like, like])
        if since is not None:
            clauses.append("date >= ?")
            params.append(int(since))
        if until is not None:
            clauses.append("date <= ?")
            params.append(int(until))
        if passes_only:
            clauses.append(f"code IN {PASS_SQL}")
        where = "WHERE " + " AND ".join(clauses)

        with self._lock:
            total = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM (
                        SELECT device_id, COALESCE(card, uid) AS code
                        FROM events {where}
                        GROUP BY device_id, COALESCE(card, uid)
                    )""",
                params,
            ).fetchone()
            rows = self._conn.execute(
                f"""SELECT
                      e.device_id,
                      MAX(d.name) AS device_name,
                      COALESCE(e.card, e.uid) AS code,
                      MAX(e.card) AS card,
                      MAX(e.uid) AS uid,
                      MAX(e.user_type) AS user_type,
                      MAX(e.id_type) AS id_type,
                      MAX(e.timezone_offset) AS timezone_offset,
                      COUNT(*) AS events,
                      SUM(CASE WHEN e.code IN {PASS_SQL} THEN 1 ELSE 0 END) AS passes,
                      SUM(CASE WHEN e.code IN {PASS_SQL} AND e.dir = 0 THEN 1 ELSE 0 END) AS entries,
                      SUM(CASE WHEN e.code IN {PASS_SQL} AND e.dir = 1 THEN 1 ELSE 0 END) AS exits,
                      MIN(e.date) AS first_seen,
                      MAX(e.date) AS last_seen
                    FROM events e
                    LEFT JOIN devices d ON d.id = e.device_id
                    {where}
                    GROUP BY e.device_id, COALESCE(e.card, e.uid)
                    ORDER BY last_seen DESC
                    LIMIT ? OFFSET ?""",
                (*params, limit, offset),
            ).fetchall()
        return [dict(row) for row in rows], int(total["n"])
