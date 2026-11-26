"""HA to DB - periodically snapshot Home Assistant entity states into a database.

Every `interval_seconds` the current state of all selected entities is read from
the Home Assistant API and written as one point/row per entity - to InfluxDB 1.x
(same layout as LoxBerry Stats4Lox) or to an SQL database. Rows that could not be
written (database down) are kept in memory and retried in the next cycle.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import math
import os
import signal
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")
HA_URL = os.environ.get("HA_URL", "http://supervisor/core/api").rstrip("/")
HA_TOKEN = os.environ.get("HA_TOKEN") or os.environ.get("SUPERVISOR_TOKEN", "")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
WEB_PORT = int(os.environ.get("WEB_PORT", "8099"))
INGRESS_PROXY = "172.30.32.2"
# for running outside Home Assistant (development) only
ALLOW_ANY_CLIENT = os.environ.get("HA2DB_DEV") == "1"

# Rows kept in memory while the database is unreachable.
MAX_BUFFERED_ROWS = 200_000
WRITE_CHUNK = 1000
STATE_MAX_LEN = 255
NON_VALUES = {"unavailable", "unknown", ""}
BACKSLASH = "\\"

SQL_DRIVERS = {
    "mariadb": "mysql+pymysql",
    "mysql": "mysql+pymysql",
    "postgresql": "postgresql+pg8000",
    "mssql": "mssql+pymssql",
    "sqlite": "sqlite",
}

log = logging.getLogger("ha2db")
stop = threading.Event()


def load_options() -> dict:
    with open(OPTIONS_PATH, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# Reading from Home Assistant
# --------------------------------------------------------------------------

def fetch_states() -> list[dict]:
    req = urllib.request.Request(
        f"{HA_URL}/states",
        headers={
            "Authorization": f"Bearer {HA_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def to_number(state: str) -> float | None:
    try:
        number = float(state)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def matches(entity_id: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(entity_id, p) for p in patterns)


class Selection:
    """Entities ticked in the web UI, kept in /data so they survive restarts."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        self.ids: set[str] = set()
        try:
            with open(path, encoding="utf-8") as fh:
                self.ids = set(json.load(fh))
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as err:
            log.error("Could not read %s, starting with an empty selection: %s", path, err)

    def __contains__(self, entity_id: str) -> bool:
        with self.lock:
            return entity_id in self.ids

    def __len__(self) -> int:
        with self.lock:
            return len(self.ids)

    def update(self, entity_ids: list[str], selected: bool) -> None:
        with self.lock:
            if selected:
                self.ids.update(entity_ids)
            else:
                self.ids.difference_update(entity_ids)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(sorted(self.ids), fh)
            os.replace(tmp, self.path)


def by_rule(entity_id: str, opt: dict) -> bool:
    """Selected by the include lists of the add-on configuration."""
    if matches(entity_id, opt["include_entities"]):
        return True
    return entity_id.split(".", 1)[0] in opt["include_domains"]


def build_rows(states: list[dict], ts: datetime, opt: dict, selection) -> list[dict]:
    rows = []
    for item in states:
        entity_id = item.get("entity_id", "")
        if matches(entity_id, opt["exclude_entities"]):
            continue
        if entity_id not in selection and not by_rule(entity_id, opt):
            continue
        state = item.get("state")
        state = "" if state is None else str(state)
        if opt["skip_unavailable"] and state in NON_VALUES:
            continue
        value = to_number(state)
        if value is None and opt["numeric_only"]:
            continue
        attrs = item.get("attributes") or {}
        unit = attrs.get("unit_of_measurement")
        name = attrs.get("friendly_name")
        device_class = attrs.get("device_class")
        rows.append(
            {
                "ts": ts,
                "entity_id": entity_id[:255],
                "value": value,
                "state": state[:STATE_MAX_LEN],
                "unit": None if unit is None else str(unit)[:64],
                "friendly_name": None if name is None else str(name)[:255],
                "device_class": None if device_class is None else str(device_class),
            }
        )
    return rows


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------

class BadData(Exception):
    """The database rejected the data itself; retrying the same chunk is pointless."""


def esc(text: str, chars: str) -> str:
    """Escape for InfluxDB line protocol; a newline would end the line."""
    text = text.replace(BACKSLASH, BACKSLASH * 2).replace("\n", " ").replace("\r", " ")
    for ch in chars:
        text = text.replace(ch, BACKSLASH + ch)
    return text


class InfluxWriter:
    """InfluxDB 1.x, laid out like LoxBerry Stats4Lox: the measurement is the
    name of the value and the number is stored in the field "Default"."""

    def __init__(self, opt: dict):
        scheme = "https" if opt.get("influx_ssl", True) else "http"
        port = opt.get("db_port") or 8086
        query = urllib.parse.urlencode({"db": opt["db_name"], "precision": "s"})
        self.url = f"{scheme}://{opt['db_host']}:{port}/write?{query}"
        self.headers = {"Content-Type": "text/plain; charset=utf-8"}
        if opt.get("db_user"):
            cred = f"{opt['db_user']}:{opt.get('db_password') or ''}".encode()
            self.headers["Authorization"] = "Basic " + b64encode(cred).decode()
        self.context = None
        if scheme == "https" and not opt.get("influx_verify_ssl", False):
            # LoxBerry's InfluxDB uses a self-signed certificate
            self.context = ssl._create_unverified_context()  # noqa: SLF001
        self.name_from = opt.get("influx_measurement", "friendly_name")
        self.prefix = opt.get("influx_measurement_prefix") or ""

    def line(self, row: dict) -> str:
        name = row["friendly_name"] if self.name_from == "friendly_name" else None
        name = (name or "").strip() or row["entity_id"]
        tags = {
            "name": name,
            "source": "homeassistant",
            "entity_id": row["entity_id"],
            "type": row["entity_id"].split(".", 1)[0],
            "category": row["device_class"],
            "unit": row["unit"],
        }
        tag_str = "".join(
            f",{key}={esc(str(val), ',= ')}"
            for key, val in sorted(tags.items())
            if val not in (None, "")
        )
        if row["value"] is not None:
            fields = f"Default={float(row['value'])!r}"
        else:
            # text goes to its own field so it never collides with the float one
            fields = 'state="' + esc(row["state"], '"') + '"'
        ts = int(row["ts"].replace(tzinfo=timezone.utc).timestamp())
        return f"{esc(self.prefix + name, ', ')}{tag_str} {fields} {ts}"

    def write(self, chunk: list[dict]) -> None:
        body = "\n".join(self.line(row) for row in chunk).encode("utf-8")
        req = urllib.request.Request(self.url, data=body, headers=self.headers)
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.context):
                pass
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", "replace").strip()[:500]
            if err.code == 400:
                raise BadData(detail) from err
            raise RuntimeError(f"HTTP {err.code}: {detail}") from err

    def purge_old(self, days: int) -> None:  # noqa: ARG002
        log.warning("retention_days is ignored for InfluxDB, use a retention policy")


class SqlWriter:
    def __init__(self, opt: dict):
        # imported here so the InfluxDB path does not need SQLAlchemy at all
        import sqlalchemy as sa
        from sqlalchemy.engine import URL

        db_type = opt["db_type"]
        if db_type == "sqlite":
            # db_name is a file path, e.g. /share/ha2db.sqlite
            self.engine = sa.create_engine(URL.create("sqlite", database=opt["db_name"]))
        else:
            query = {"charset": "utf8mb4"} if db_type in ("mariadb", "mysql") else {}
            url = URL.create(
                SQL_DRIVERS[db_type],
                username=opt.get("db_user") or None,
                password=opt.get("db_password") or None,
                host=opt.get("db_host") or None,
                port=opt.get("db_port") or None,
                database=opt["db_name"],
                query=query,
            )
            self.engine = sa.create_engine(url, pool_pre_ping=True, pool_recycle=1800)

        name = opt["table_name"]
        self.table = sa.Table(
            name,
            sa.MetaData(),
            sa.Column(
                "id",
                sa.BigInteger().with_variant(sa.Integer, "sqlite"),
                primary_key=True,
                autoincrement=True,
            ),
            sa.Column("ts", sa.DateTime, nullable=False),  # UTC
            sa.Column("entity_id", sa.String(255), nullable=False),
            sa.Column("value", sa.Float(53)),  # NULL if the state is not a number
            sa.Column("state", sa.String(STATE_MAX_LEN)),  # raw state as shown in HA
            sa.Column("unit", sa.String(64)),
            sa.Column("friendly_name", sa.String(255)),
            sa.Index(f"ix_{name}_entity_ts", "entity_id", "ts"),
            sa.Index(f"ix_{name}_ts", "ts"),
        )
        self.columns = [c.name for c in self.table.columns if c.name != "id"]
        self.ready = False

    def write(self, chunk: list[dict]) -> None:
        if not self.ready:
            self.table.metadata.create_all(self.engine)
            self.ready = True
        data = [{key: row[key] for key in self.columns} for row in chunk]
        with self.engine.begin() as conn:
            conn.execute(self.table.insert(), data)

    def purge_old(self, days: int) -> None:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
        table = self.table
        with self.engine.begin() as conn:
            deleted = conn.execute(table.delete().where(table.c.ts < cutoff)).rowcount
        log.info("Retention: deleted %s rows older than %s days", deleted, days)


def flush(writer, pending: deque) -> int:
    """Write buffered rows; a chunk leaves the buffer only once it is stored."""
    written = 0
    while pending:
        chunk = [pending[i] for i in range(min(WRITE_CHUNK, len(pending)))]
        try:
            writer.write(chunk)
            written += len(chunk)
        except BadData as err:
            # InfluxDB stores the valid points of a batch and reports the rest
            log.error("Database rejected part of %s rows, not retried: %s", len(chunk), err)
        for _ in chunk:
            pending.popleft()
    return written


# --------------------------------------------------------------------------
# Web UI (Home Assistant ingress) - tick the entities to store
# --------------------------------------------------------------------------

def start_web(opt: dict, selection: Selection) -> None:
    with open(os.path.join(WEB_DIR, "index.html"), "rb") as fh:
        index_html = fh.read()

    class Handler(BaseHTTPRequestHandler):
        def send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, code: int, data) -> None:
            self.send(code, json.dumps(data).encode("utf-8"), "application/json")

        def allowed(self) -> bool:
            # only the Supervisor's ingress proxy may talk to this server
            if ALLOW_ANY_CLIENT or self.client_address[0] == INGRESS_PROXY:
                return True
            self.send_json(403, {"error": "forbidden"})
            return False

        def route(self) -> str:
            return "/" + urllib.parse.urlsplit(self.path).path.strip("/")

        def do_GET(self) -> None:  # noqa: N802
            if not self.allowed():
                return
            route = self.route()
            if route == "/":
                self.send(200, index_html, "text/html; charset=utf-8")
            elif route == "/api/entities":
                try:
                    states = fetch_states()
                except Exception as err:  # noqa: BLE001
                    self.send_json(502, {"error": str(err)})
                    return
                entities = []
                for item in states:
                    entity_id = item.get("entity_id", "")
                    if not entity_id or matches(entity_id, opt["exclude_entities"]):
                        continue
                    attrs = item.get("attributes") or {}
                    entities.append(
                        {
                            "entity_id": entity_id,
                            "domain": entity_id.split(".", 1)[0],
                            "name": str(attrs.get("friendly_name") or entity_id),
                            "state": str(item.get("state", ""))[:80],
                            "unit": str(attrs.get("unit_of_measurement") or ""),
                            "selected": entity_id in selection,
                            "by_rule": by_rule(entity_id, opt),
                        }
                    )
                entities.sort(key=lambda e: (e["name"].lower(), e["entity_id"]))
                self.send_json(200, {"entities": entities})
            else:
                self.send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if not self.allowed():
                return
            if self.route() != "/api/select":
                self.send_json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
                data = json.loads(self.rfile.read(length))
                entity_ids = [str(e) for e in data["entity_ids"]]
                selected = bool(data["selected"])
            except (ValueError, KeyError, TypeError):
                self.send_json(400, {"error": "bad request"})
                return
            selection.update(entity_ids, selected)
            log.info(
                "Selection changed: %s %s, %s entities ticked now",
                "added" if selected else "removed", len(entity_ids), len(selection),
            )
            self.send_json(200, {"count": len(selection)})

        def log_message(self, *args) -> None:  # noqa: ARG002
            pass

    server = ThreadingHTTPServer(("0.0.0.0", WEB_PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Web UI listening on port %s", WEB_PORT)


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def next_run(interval: int, align: bool) -> float:
    now = time.time()
    if align:
        # 300 s -> :00, :05, :10 ... regardless of when the add-on was started
        return (math.floor(now / interval) + 1) * interval
    return now + interval


def main() -> None:
    opt = load_options()
    logging.basicConfig(
        level=opt.get("log_level", "info").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    if not opt.get("db_name") or (opt["db_type"] != "sqlite" and not opt.get("db_host")):
        log.error("Fill in db_host and db_name in the add-on configuration")
        raise SystemExit(1)

    interval = int(opt["interval_seconds"])
    retention = int(opt.get("retention_days") or 0)
    writer = InfluxWriter(opt) if opt["db_type"] == "influxdb" else SqlWriter(opt)
    pending: deque = deque(maxlen=MAX_BUFFERED_ROWS)
    last_purge = 0.0
    selection = Selection(os.path.join(DATA_DIR, "selected_entities.json"))
    start_web(opt, selection)

    log.info(
        "Started: %s database '%s' on %s, every %s s",
        opt["db_type"], opt["db_name"], opt.get("db_host") or "local file", interval,
    )

    first = True
    while not stop.is_set():
        if not first:
            stop.wait(max(0.0, next_run(interval, opt["align_to_clock"]) - time.time()))
            if stop.is_set():
                break
        first = False

        ts = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
        try:
            rows = build_rows(fetch_states(), ts, opt, selection)
        except Exception as err:  # noqa: BLE001 - keep running, try next cycle
            log.error("Reading states from Home Assistant failed: %s", err)
            rows = []

        if len(pending) + len(rows) > MAX_BUFFERED_ROWS:
            log.warning("Buffer full, the oldest unwritten rows are being dropped")
        pending.extend(rows)

        try:
            written = flush(writer, pending)
            log.info("Wrote %s rows (%s entities in this cycle)", written, len(rows))
            if retention and time.time() - last_purge > 86400:
                writer.purge_old(retention)
                last_purge = time.time()
        except Exception as err:  # noqa: BLE001
            log.error(
                "Database write failed, %s rows kept for retry: %s", len(pending), err
            )

    log.info("Stopped")


if __name__ == "__main__":
    main()
