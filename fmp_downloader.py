#!/usr/bin/env python3
"""Downloader for Financial Modeling Prep historical data."""
from __future__ import annotations

import json
import logging
import math
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import MetaData, Table, create_engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError


BASE_URL = "https://financialmodelingprep.com/stable"
DAILY_ENDPOINT = BASE_URL + "/historical-price-eod/full"
INTRADAY_ENDPOINT = BASE_URL + "/historical-chart/{interval}"

START_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
PROGRESS_FILE = Path("fmp_progress.json")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "fmp_downloader.log"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_BACKOFF = 2
INTRADAY_INTERVALS = ("1hour", "30min", "15min", "5min", "1min")
INTRADAY_SECONDS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1hour": 3600,
}
DAILY_SECONDS = 86400
RATE_LIMIT_PER_MINUTE = 2900  # stay below 3000 requests/minute
BATCH_SIZE = 500

NEW_YORK_TZ = ZoneInfo("America/New_York")
DECIMAL_QUANTIZER = Decimal("0.000001")

ASSET_SYMBOLS: Tuple[Tuple[int, str], ...] = (
    (88, "^MOVE"),
    (95, "^SPX"),
    (96, "^DJI"),
    (97, "^NDX"),
    (98, "^RUT"),
    (99, "^VWB"),
    (100, "^DJUSSC"),
    (101, "^VIX3M"),
    (102, "^IRX"),
    (103, "^VIX"),
    (104, "^VXN"),
    (105, "^VIX1D"),
    (106, "^VVIX"),
    (107, "^VIN"),
    (108, "^VIX3M"),
    (113, "^N225"),
    (114, "^HSI"),
    (115, "^TWII"),
    (116, "000001.SS"),
    (117, "^KS11"),
    (118, "^GDAXI"),
    (119, "^STOXX50E"),
    (120, "^AXMJ"),
    (121, "^W2DOW"),
    (122, "^SPGNRUP"),
    (123, "^SPGSCI"),
    (124, "^TRCCRBTR"),
    (125, "MSCIWORLD"),
    (126, "SX8P.Z"),
    (140, "DX-Y.NYB"),
)
@dataclass
class Config:
    fmp_api_key: str
    db_url: str


class RateLimiter:
    """Simple rolling-window rate limiter."""

    def __init__(self, max_calls: int, period_seconds: int) -> None:
        self.max_calls = max_calls
        self.period = period_seconds
        self.calls: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            now = time.monotonic()
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            wait_time = self.period - (now - self.calls[0]) + 0.01
            if wait_time > 0:
                time.sleep(wait_time)


class ProgressTracker:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.state: Dict[str, Dict[str, str]] = {}
        if path.exists():
            try:
                self.state = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self.state = {}

    def get(self, asset_id: int, interval: str) -> Optional[datetime]:
        asset_key = str(asset_id)
        interval_state = self.state.get(asset_key, {})
        value = interval_state.get(interval)
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    def update(self, asset_id: int, interval: str, timestamp: datetime) -> None:
        asset_key = str(asset_id)
        if asset_key not in self.state:
            self.state[asset_key] = {}
        self.state[asset_key][interval] = timestamp.astimezone(timezone.utc).isoformat()
        try:
            self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            logging.getLogger(__name__).warning("Failed to persist progress tracker: %s", exc)


def load_env(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f".env file not found at {path}")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        env[key.strip()] = value
    return env


def build_config(env: Dict[str, str]) -> Config:
    api_key = env.get("FMP_API_KEY")
    if not api_key:
        raise ValueError("FMP_API_KEY is missing in .env")
    host = env.get("PG_HOST", "localhost")
    port = env.get("PG_PORT", "5432")
    user = env.get("PG_USER")
    password = env.get("PG_PASSWORD")
    database = env.get("PG_DATABASE")
    if not user or not password or not database:
        raise ValueError("Database credentials (PG_USER, PG_PASSWORD, PG_DATABASE) are required")
    url = (
        "postgresql+psycopg2://"
        f"{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(database)}"
    )
    return Config(fmp_api_key=api_key, db_url=url)


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers: List[logging.Handler] = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    handlers.append(console_handler)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    handlers.append(file_handler)

    logging.basicConfig(level=logging.INFO, handlers=handlers)


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.replace("Z", "+") if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        try:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def safe_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        result = int(float(value))
        return result
    except (TypeError, ValueError):
        return None


def normalize_volume(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if value < 0:
        return None
    return value


def to_decimal(value: Optional[float]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value.quantize(DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_fallback_vwap(open_: Optional[float], high: Optional[float], low: Optional[float], close: Optional[float], raw_vwap: Optional[float]) -> Optional[float]:
    if raw_vwap is not None:
        return raw_vwap
    prices = [price for price in (high, low, close) if price is not None]
    if len(prices) == 3:
        return sum(prices) / 3.0
    if close is not None:
        return close
    if open_ is not None:
        return open_
    if prices:
        return prices[0]
    return None


def request_json(session: requests.Session, url: str, params: Dict[str, Any], rate_limiter: RateLimiter, logger: logging.Logger) -> Any:
    for attempt in range(1, MAX_RETRIES + 1):
        rate_limiter.acquire()
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("Request error (%s/%s): %s", attempt, MAX_RETRIES, exc)
            sleep_time = RETRY_BACKOFF ** (attempt - 1)
            time.sleep(sleep_time)
            continue
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "1"))
            logger.warning("Rate limited by server, sleeping %s seconds", retry_after)
            time.sleep(retry_after)
            continue
        if response.status_code >= 500:
            logger.warning("Server error %s on %s (attempt %s)", response.status_code, url, attempt)
            time.sleep(RETRY_BACKOFF ** (attempt - 1))
            continue
        if not response.ok:
            logger.error("Failed request %s: %s - %s", url, response.status_code, response.text)
            response.raise_for_status()
        try:
            return response.json()
        except ValueError as exc:
            logger.error("Failed to parse JSON from %s: %s", url, exc)
            raise
    raise RuntimeError(f"Maximum retries exceeded for {url}")


def normalize_daily_payload(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, dict) and "historical" in raw:
        records = raw.get("historical", [])
    else:
        records = raw
    if not isinstance(records, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        dt = parse_datetime(str(item.get("date")))
        if not dt or dt < START_DATE:
            continue
        dt = ensure_utc(dt)
        open_ = safe_float(item.get("open"))
        high = safe_float(item.get("high"))
        low = safe_float(item.get("low"))
        close = safe_float(item.get("close"))
        volume = normalize_volume(safe_int(item.get("volume")))
        vwap = compute_fallback_vwap(open_, high, low, close, safe_float(item.get("vwap")))
        normalized.append(
            {
                "fecha": dt,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "change": safe_float(item.get("change")),
                "change_percent": safe_float(item.get("changePercent")),
                "vwap": vwap,
            }
        )
    normalized.sort(key=lambda row: row["fecha"])
    return normalized


def normalize_intraday_payload(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        dt = parse_datetime(str(item.get("date")))
        if not dt or dt < START_DATE:
            continue
        dt = ensure_utc(dt)
        normalized.append(
            {
                "fecha": dt,
                "open": safe_float(item.get("open")),
                "high": safe_float(item.get("high")),
                "low": safe_float(item.get("low")),
                "close": safe_float(item.get("close")),
                "volume": normalize_volume(safe_int(item.get("volume"))),
            }
        )
    normalized.sort(key=lambda row: row["fecha"])
    return normalized


def detect_temporal_gaps(
    rows: Iterable[Dict[str, Any]],
    expected_seconds: float,
    logger: logging.Logger,
    asset_id: int,
    interval: str,
) -> None:
    previous: Optional[datetime] = None
    tolerated = expected_seconds * 3
    for row in rows:
        current = row.get("fecha")
        if not isinstance(current, datetime):
            continue
        if previous is not None:
            delta = (current - previous).total_seconds()
            if delta > tolerated and not is_expected_market_closure(
                previous, current, interval, expected_seconds
            ):
                logger.warning(
                    "Gap detected for asset_id=%s interval=%s between %s and %s (%.0f seconds)",
                    asset_id,
                    interval,
                    previous.isoformat(),
                    current.isoformat(),
                    delta,
                )
        previous = current


def is_expected_market_closure(
    previous: datetime,
    current: datetime,
    interval: str,
    expected_seconds: float,
) -> bool:
    """Return True when the gap matches regular overnight/weekend pauses."""

    if interval not in INTRADAY_SECONDS:
        return False

    # Anything within the default tolerance is already considered fine.
    delta_seconds = (current - previous).total_seconds()
    if delta_seconds <= expected_seconds * 3:
        return True

    prev_date = previous.date()
    curr_date = current.date()
    if curr_date <= prev_date:
        return False

    day_diff = (curr_date - prev_date).days
    if day_diff == 1:
        # Overnight closure between trading sessions.
        return True

    # Allow sequences where the days in-between are only weekend dates.
    probe = prev_date + timedelta(days=1)
    while probe < curr_date:
        if probe.weekday() < 5:
            return False
        probe += timedelta(days=1)

    # If we reach here, the gap spans only non-trading weekend days.
    return True


def deduplicate_payload(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[datetime, Dict[str, Any]] = {}
    for item in payload:
        fecha = item.get("fecha")
        if isinstance(fecha, datetime):
            deduped[fecha] = item
    return [deduped[key] for key in sorted(deduped)]


def validate_price_rows(
    rows: List[Dict[str, Any]],
    logger: logging.Logger,
    asset_id: int,
    interval: str,
) -> None:
    if not rows:
        return
    original_len = len(rows)
    violations = {
        "high_lt_oc": 0,
        "low_gt_oc": 0,
        "high_lt_low": 0,
        "volume_negative": 0,
    }
    valid_rows: List[Dict[str, Any]] = []
    for row in rows:
        open_ = row.get("open")
        high = row.get("high")
        low = row.get("low")
        close = row.get("close")
        volume = row.get("volume")
        oc_values = [v for v in (open_, close) if isinstance(v, (int, float))]
        oc_max = max(oc_values) if oc_values else None
        oc_min = min(oc_values) if oc_values else None
        has_violation = False
        reasons: List[str] = []
        if isinstance(high, (int, float)) and oc_max is not None and high < oc_max:
            violations["high_lt_oc"] += 1
            has_violation = True
            reasons.append("high<max(open,close)")
        if isinstance(low, (int, float)) and oc_min is not None and low > oc_min:
            violations["low_gt_oc"] += 1
            has_violation = True
            reasons.append("low>min(open,close)")
        if isinstance(high, (int, float)) and isinstance(low, (int, float)) and high < low:
            violations["high_lt_low"] += 1
            has_violation = True
            reasons.append("high<low")
        if isinstance(volume, (int, float)) and volume < 0:
            violations["volume_negative"] += 1
            has_violation = True
            reasons.append("volume<0")
        if not has_violation:
            valid_rows.append(row)
        else:
            timestamp = row.get("fecha")
            if isinstance(timestamp, datetime):
                timestamp_repr = timestamp.isoformat()
            else:
                timestamp_repr = str(timestamp)
            logger.warning(
                "Discarding row for asset_id=%s interval=%s fecha=%s due to: %s",
                asset_id,
                interval,
                timestamp_repr,
                ", ".join(reasons) or "validation failure",
            )
    if len(valid_rows) != original_len:
        logger.warning(
            "Discarded %s/%s rows for asset_id=%s interval=%s due to quality violations "
            "(high>=max(open,close) failures=%s, low<=min(open,close) failures=%s, "
            "high>=low failures=%s, volume>=0 failures=%s)",
            original_len - len(valid_rows),
            original_len,
            asset_id,
            interval,
            violations["high_lt_oc"],
            violations["low_gt_oc"],
            violations["high_lt_low"],
            violations["volume_negative"],
        )
        rows[:] = valid_rows
    zero_volume = sum(1 for row in rows if not row.get("volume"))
    if zero_volume == len(rows):
        logger.warning(
            "All rows for asset_id=%s interval=%s carry empty volume; treating as missing data",
            asset_id,
            interval,
        )
    flat_candles = sum(
        1
        for row in rows
        if row.get("open") is not None
        and row.get("open") == row.get("high") == row.get("low") == row.get("close")
    )
    if flat_candles:
        logger.warning(
            "%s rows for asset_id=%s interval=%s have flat OHLC candles",
            flat_candles,
            asset_id,
            interval,
        )


def build_daily_rows(
    symbol: str,
    asset_id: int,
    payload: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in payload:
        dt_utc = ensure_utc(item["fecha"])
        dt_ny = dt_utc.astimezone(NEW_YORK_TZ)
        rows.append(
            {
                "symbol": symbol,
                "fecha": dt_ny,
                "open": to_decimal(item["open"]),
                "high": to_decimal(item["high"]),
                "low": to_decimal(item["low"]),
                "close": to_decimal(item["close"]),
                "volume": item["volume"],
                "change": to_decimal(item["change"]),
                "change_percent": to_decimal(item["change_percent"]),
                "vwap": to_decimal(item["vwap"]),
                "asset_id": asset_id,
                "fuente": "FMP",
                "intervalo": "Daily",
                "divadj_open": None,
                "divadj_high": None,
                "divadj_low": None,
                "divadj_close": None,
            }
        )
    return rows


def build_intraday_rows(
    symbol: str,
    asset_id: int,
    interval: str,
    payload: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in payload:
        dt_utc = ensure_utc(item["fecha"])
        epoch = math.floor(dt_utc.timestamp())
        dt_ny = dt_utc.astimezone(NEW_YORK_TZ)
        rows.append(
            {
                "symbol": symbol,
                "fecha": dt_ny,
                "open": to_decimal(item["open"]),
                "high": to_decimal(item["high"]),
                "low": to_decimal(item["low"]),
                "close": to_decimal(item["close"]),
                "volume": item["volume"],
                "asset_id": asset_id,
                "fuente": "FMP",
                "intervalo": interval,
                "epoch": epoch,
                "vwap": None,
            }
        )
    return rows


def chunked(iterable: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(iterable), size):
        yield iterable[idx : idx + size]


def upsert_rows(
    engine,
    table: Table,
    rows: List[Dict[str, Any]],
    unique_columns: Tuple[str, ...],
    logger: logging.Logger,
) -> int:
    inserted = 0
    conflict_elements = list(unique_columns)
    conflict_hint = ", ".join(conflict_elements)
    for batch in chunked(rows, BATCH_SIZE):
        base_stmt = pg_insert(table).values(batch)
        try:
            stmt = base_stmt.on_conflict_do_nothing(index_elements=conflict_elements)
            with engine.begin() as connection:
                result = connection.execute(stmt)
        except DBAPIError as exc:
            original_message = str(getattr(exc, "orig", exc)).lower()
            if "no unique or exclusion constraint matching the on conflict specification" in original_message:
                logger.warning(
                    "Table %s lacks a UNIQUE/PK for (%s); falling back to plain insert",
                    table.name,
                    conflict_hint,
                )
                with engine.begin() as connection:
                    result = connection.execute(base_stmt)
            else:
                raise
        inserted += result.rowcount or 0
    if inserted:
        logger.info("Inserted %s rows into %s", inserted, table.name)
    else:
        logger.info("No new rows to insert into %s", table.name)
    return inserted


def fetch_daily_data(
    symbol: str,
    start: datetime,
    end: datetime,
    session: requests.Session,
    rate_limiter: RateLimiter,
    logger: logging.Logger,
    cache: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    cache_key = f"{symbol}:{start.date().isoformat()}:{end.date().isoformat()}"
    if cache_key in cache:
        return cache[cache_key]
    url = DAILY_ENDPOINT
    params = {
        "symbol": symbol,
        "apikey": session.params.get("apikey"),
        "from": start.date().isoformat(),
        "to": end.date().isoformat(),
    }
    payload = request_json(session, url, params=params, rate_limiter=rate_limiter, logger=logger)
    normalized = normalize_daily_payload(payload)
    cache[cache_key] = normalized
    logger.info(
        "Fetched %s daily rows for %s between %s and %s",
        len(normalized),
        symbol,
        start.date().isoformat(),
        end.date().isoformat(),
    )
    return normalized


def fetch_intraday_data(
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    session: requests.Session,
    rate_limiter: RateLimiter,
    logger: logging.Logger,
    cache: Dict[str, Dict[str, List[Dict[str, Any]]]],
) -> List[Dict[str, Any]]:
    interval_cache = cache.setdefault(interval, {})
    cache_key = f"{symbol}:{start.isoformat()}:{end.isoformat()}"
    if cache_key in interval_cache:
        return interval_cache[cache_key]
    url = INTRADAY_ENDPOINT.format(interval=interval)
    params = {
        "apikey": session.params.get("apikey"),
        "symbol": symbol,
        "from": start.strftime("%Y-%m-%d %H:%M:%S"),
        "to": end.strftime("%Y-%m-%d %H:%M:%S"),
    }
    payload = request_json(session, url, params=params, rate_limiter=rate_limiter, logger=logger)
    normalized = normalize_intraday_payload(payload)
    interval_cache[cache_key] = normalized
    logger.info(
        "Fetched %s %s rows for %s between %s and %s",
        len(normalized),
        interval,
        symbol,
        start.isoformat(),
        end.isoformat(),
    )
    return normalized


def ensure_daily_coverage(engine, table: Table, asset_id: int, logger: logging.Logger) -> None:
    query = select(func.count()).select_from(table).where(table.c.asset_id == asset_id).where(table.c.intervalo == "Daily")
    with engine.connect() as connection:
        total = connection.execute(query).scalar_one()
    if total == 0:
        logger.info("No daily data present yet for asset_id=%s", asset_id)


def ensure_intraday_tables(engine, table: Table, asset_id: int, interval: str, logger: logging.Logger) -> None:
    query = (
        select(func.count())
        .select_from(table)
        .where(table.c.asset_id == asset_id)
        .where(table.c.intervalo == interval)
    )
    with engine.connect() as connection:
        total = connection.execute(query).scalar_one()
    if total == 0:
        logger.info("No intraday data yet for asset_id=%s interval=%s", asset_id, interval)


def get_last_timestamp(engine, table: Table, asset_id: int, interval: str) -> Optional[datetime]:
    query = (
        select(func.max(table.c.fecha))
        .where(table.c.asset_id == asset_id)
        .where(table.c.intervalo == interval)
    )
    with engine.connect() as connection:
        value = connection.execute(query).scalar_one_or_none()
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def filter_new_daily_rows(
    engine,
    table: Table,
    asset_id: int,
    payload: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    query = (
        select(table.c.fecha)
        .where(table.c.asset_id == asset_id)
        .where(table.c.intervalo == "Daily")
        .where(table.c.fecha >= START_DATE)
    )
    with engine.connect() as connection:
        existing_dates = {row[0].astimezone(timezone.utc) for row in connection.execute(query)}
    api_dates = {row["fecha"] for row in payload}
    missing_dates = sorted(api_dates - existing_dates)
    if missing_dates:
        logger.info(
            "Detected %s missing daily rows for asset_id=%s; filling gaps",
            len(missing_dates),
            asset_id,
        )
    new_payload = [row for row in payload if row["fecha"] not in existing_dates]
    return new_payload


def filter_new_intraday_rows(
    engine,
    table: Table,
    asset_id: int,
    interval: str,
    payload: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not payload:
        return []
    start = payload[0]["fecha"]
    end = payload[-1]["fecha"]
    query = (
        select(table.c.fecha)
        .where(table.c.asset_id == asset_id)
        .where(table.c.intervalo == interval)
        .where(table.c.fecha >= start)
        .where(table.c.fecha <= end)
    )
    with engine.connect() as connection:
        existing = {row[0].astimezone(timezone.utc) for row in connection.execute(query)}
    return [row for row in payload if row["fecha"] not in existing]


def _compute_fetch_window(
    last_progress: Optional[datetime],
    last_db: Optional[datetime],
    interval_seconds: int,
    buffer_intervals: int,
) -> Tuple[datetime, datetime, Optional[datetime]]:
    latest_known = max(filter(None, [last_progress, last_db]), default=None)
    if latest_known:
        start_candidate = latest_known - timedelta(seconds=interval_seconds * buffer_intervals)
        start = max(START_DATE, start_candidate)
    else:
        start = START_DATE
    end = datetime.now(timezone.utc)
    if end <= start:
        end = start + timedelta(seconds=interval_seconds)
    return start, end, latest_known


def process_daily(
    symbol: str,
    asset_id: int,
    session: requests.Session,
    rate_limiter: RateLimiter,
    cache: Dict[str, List[Dict[str, Any]]],
    engine,
    table: Table,
    progress: ProgressTracker,
    logger: logging.Logger,
) -> None:
    last_progress = progress.get(asset_id, "Daily")
    last_db = get_last_timestamp(engine, table, asset_id, "Daily")
    start, end, latest_known = _compute_fetch_window(last_progress, last_db, DAILY_SECONDS, buffer_intervals=3)
    if latest_known and (datetime.now(timezone.utc) - latest_known) < timedelta(hours=12):
        logger.info("Daily data for asset_id=%s is up-to-date (latest %s)", asset_id, latest_known)
        return
    payload = fetch_daily_data(symbol, start, end, session, rate_limiter, logger, cache)
    payload = deduplicate_payload(payload)
    detect_temporal_gaps(payload, 86400, logger, asset_id, "Daily")
    new_payload = filter_new_daily_rows(engine, table, asset_id, payload, logger)
    validate_price_rows(new_payload, logger, asset_id, "Daily")
    if not new_payload:
        logger.info("No new daily data needed for asset_id=%s", asset_id)
        if payload:
            progress.update(asset_id, "Daily", payload[-1]["fecha"])
        return
    rows = build_daily_rows(symbol, asset_id, new_payload)
    inserted = upsert_rows(engine, table, rows, ("asset_id", "intervalo", "fecha", "symbol"), logger)
    if inserted:
        logger.info(
            "Confirmed insertion of %s daily rows for asset_id=%s symbol=%s",
            inserted,
            asset_id,
            symbol,
        )
    progress.update(asset_id, "Daily", new_payload[-1]["fecha"])


def process_intraday(
    symbol: str,
    asset_id: int,
    interval: str,
    session: requests.Session,
    rate_limiter: RateLimiter,
    cache: Dict[str, Dict[str, List[Dict[str, Any]]]],
    engine,
    table: Table,
    progress: ProgressTracker,
    logger: logging.Logger,
) -> None:
    last_progress = progress.get(asset_id, interval)
    last_db = get_last_timestamp(engine, table, asset_id, interval)
    start, end, latest_known = _compute_fetch_window(
        last_progress,
        last_db,
        INTRADAY_SECONDS[interval],
        buffer_intervals=5,
    )
    if latest_known and (datetime.now(timezone.utc) - latest_known) < timedelta(minutes=5):
        logger.info("%s data for asset_id=%s is up-to-date (latest %s)", interval, asset_id, latest_known)
        return
    payload = fetch_intraday_data(symbol, interval, start, end, session, rate_limiter, logger, cache)
    payload = deduplicate_payload(payload)
    expected_seconds = INTRADAY_SECONDS[interval]
    detect_temporal_gaps(payload, expected_seconds, logger, asset_id, interval)
    new_payload = filter_new_intraday_rows(engine, table, asset_id, interval, payload)
    validate_price_rows(new_payload, logger, asset_id, interval)
    if not new_payload:
        logger.info("No new %s data needed for asset_id=%s", interval, asset_id)
        if payload:
            progress.update(asset_id, interval, payload[-1]["fecha"])
        return
    rows = build_intraday_rows(symbol, asset_id, interval, new_payload)
    inserted = upsert_rows(engine, table, rows, ("asset_id", "intervalo", "fecha", "symbol"), logger)
    if inserted:
        logger.info(
            "Confirmed insertion of %s %s rows for asset_id=%s symbol=%s",
            inserted,
            interval,
            asset_id,
            symbol,
        )
    progress.update(asset_id, interval, new_payload[-1]["fecha"])


def main() -> None:
    setup_logging()
    logger = logging.getLogger("fmp_downloader")
    logger.info(
        "All payload timestamps are normalised to UTC internally; stored fecha uses America/New_York "
        "timezone while epoch keeps the UTC reference"
    )
    try:
        env = load_env(Path(".env"))
        config = build_config(env)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed to load configuration: %s", exc)
        sys.exit(1)

    engine = create_engine(config.db_url, pool_pre_ping=True, future=True)
    metadata = MetaData()
    try:
        daily_table = Table("cotizaciones_diarias", metadata, autoload_with=engine)
        intraday_table = Table("cotizaciones_intradia", metadata, autoload_with=engine)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed to reflect database tables: %s", exc)
        sys.exit(1)

    progress = ProgressTracker(PROGRESS_FILE)
    rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE, 60)
    session = requests.Session()
    session.params = {"apikey": config.fmp_api_key}

    daily_cache: Dict[str, List[Dict[str, Any]]] = {}
    intraday_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for asset_id, symbol in ASSET_SYMBOLS:
        logger.info("Processing symbol=%s asset_id=%s", symbol, asset_id)
        try:
            ensure_daily_coverage(engine, daily_table, asset_id, logger)
            process_daily(
                symbol,
                asset_id,
                session,
                rate_limiter,
                daily_cache,
                engine,
                daily_table,
                progress,
                logger,
            )
            for interval in INTRADAY_INTERVALS:
                ensure_intraday_tables(engine, intraday_table, asset_id, interval, logger)
                process_intraday(
                    symbol,
                    asset_id,
                    interval,
                    session,
                    rate_limiter,
                    intraday_cache,
                    engine,
                    intraday_table,
                    progress,
                    logger,
                )
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Error processing symbol %s (asset_id=%s): %s", symbol, asset_id, exc)
            continue

    logger.info("Data synchronization completed")


if __name__ == "__main__":
    main()
