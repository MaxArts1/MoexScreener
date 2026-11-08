#!/usr/bin/env python3
"""
MOEX Real-Time Screener v3.1 — adds 3 missing ineff.world features
===================================================================
Additions over v3:
  M10 Arba (Arbitrage)   — linked-instrument spread with fair-price z-score
  M11 Trace              — digital trace: session log for each detected robot
  M12 Scalping (Plan)    — "near the plan": last-trade >= 2x average trade size

Data source: MOEX ISS REST (boardprice + candles), same as v3 --rest mode.
WebSocket is not required for the new modules.

HTML dashboard: the screener writes data/screener.json on every cycle.
Open dashboard.html in a browser to watch the screener live (the page
polls the JSON file via fetch() every 5s — run it from the same folder
or serve it with `python -m http.server`).
"""

import asyncio
import json
import os
import signal
import statistics
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

# Force UTF-8 stdout
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _csv(name: str, default: str) -> list[str]:
    return [t.strip().upper() for t in os.getenv(name, default).split(",") if t.strip()]

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (ValueError, TypeError):
        return float(default)

def _env_int(name: str, default: int) -> int:
    return int(_env_float(name, float(default)))

# MOEX ISS timestamps are Moscow time (UTC+3).
MSK_TZ = timezone(timedelta(hours=3))


def _msk_now() -> datetime:
    return datetime.now(tz=MSK_TZ)

MOEX_WS_URL = os.getenv("MOEX_WS_URL", "wss://iss.moex.com/infocx/v3/websocket")
MOEX_DOMAIN = os.getenv("MOEX_DOMAIN", "DEMO")
MOEX_LOGIN = os.getenv("MOEX_LOGIN", "guest")
MOEX_PASSCODE = os.getenv("MOEX_PASSCODE", "guest")
BASE = os.getenv("MOEX_REST_BASE", "https://iss.moex.com/iss")

EQUITIES = _csv("EQUITIES", "")  # empty = use CORE_EQUITIES fallback (liquid blue chips)
# Core liquid equities — same as reference dashboard + extra liquid names (DOMRF, etc.)
CORE_EQUITIES = [
    "AFKS", "AFLT", "AKRN", "ALRS", "AQUA", "ASTR", "BANE", "BANEP", "BAZA", "BELU",
    "BSPB", "BTBR", "CBOM", "CHMF", "CNRU", "DATA", "DIAS", "DOMRF", "ENPG", "ETLN",
    "EUTR", "FEES", "FESH", "FIXR", "FLOT", "GAZP", "GLRX", "GMKN", "HEAD", "HYDR",
    "IRAO", "IRKT", "IVAT", "KCHEP", "KMAZ", "LEAS", "LENT", "LKOH", "LSNGP", "LSRG",
    "MAGN", "MBNK", "MDMG", "MGKL", "MGNT", "MGTSP", "MOEX", "MRKC", "MRKP", "MRKU",
    "MRKV", "MRKZ", "MSNG", "MSRS", "MTLR", "MTLRP", "MTSS", "MVID", "NKHP", "NKNCP",
    "NKSH", "NLMK", "NMTP", "NVTK", "OGKB", "OKEY", "OZON", "OZPH", "PHOR", "PIKK",
    "PLZL", "POSI", "PRMD", "RAGR", "RASP", "RENI", "RNFT", "ROLO", "ROSN", "RTKM",
    "RTKMP", "RUAL", "SBER", "SBERP", "SELG", "SFIN", "SGZH", "SIBN", "SMLT", "SNGS",
    "SNGSP", "SOFL", "SPBE", "STSB", "SVAV", "SVCB", "T", "TATN", "TATNP", "TGKA",
    "TGKN", "TRMK", "TRNFP", "UGLD", "UKUZ", "UNAC", "UPRO", "UWGN", "VKCO", "VSEH",
    "VSMO", "VTBR", "WUSH", "X5", "YDEX", "ZAYM",
]
FUTURES = _csv("FUTURES", "")    # empty = full FUTURE board
FUTURES_TOP_N = int(os.getenv("FUTURES_TOP_N", "30"))  # top-N by VALTODAY for futures
EQUITIES_TOP_N = int(os.getenv("EQUITIES_TOP_N", "20")) # 0 = all, else top-N (by VALTODAY from boardprice)
FOREX_TOP_N = int(os.getenv("FOREX_TOP_N", "20")) # top-N by VALTODAY for currencies

def _fetch_board_securities(board: str, top_n: int = 0) -> list:
    """Return the most-active tickers on a board.

    1. securities.json -> all active SECIDs on the board (one call).
    2. If top_n > 0: probe a candidate pool with parallel individual
       boardprice calls to read VALTODAY, sort desc, keep top_n.
    3. If top_n == 0: return the full active list.
    Falls back to [] on network error (caller uses hardcoded core list).
    """
    import urllib.request, json as _json
    import concurrent.futures

    board_map = {
        "TQBR":   "stock/markets/shares",
        "CURREN": "stock/markets/forex",
        "RFUD":   "futures/markets/forts",   # MOEX derivatives board
        "FUTURE": "futures/markets/forts",   # legacy alias
    }
    eng = board_map.get(board, "stock/markets/shares")
    if board == "FUTURE":
        board = "RFUD"

    # Step 1: all active SECIDs
    url_sec = f"{BASE}/engines/{eng}/boards/{board}/securities.json?iss.meta=off"
    req = urllib.request.Request(url_sec, headers={"User-Agent": "moex-screener/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        sec = data.get("securities", {})
        columns = sec.get("columns", [])
        rows = sec.get("data", [])
        # securities.json also carries marketdata with today's turnover — use it
        # to rank instruments without one extra request per ticker.
        md_block = data.get("marketdata") or {}
        md_cols = md_block.get("columns") or []
        md_rows = md_block.get("data") or []
        valtoday: dict[str, float] = {}
        if md_rows and "SECID" in md_cols and "VALTODAY" in md_cols:
            i_sid = md_cols.index("SECID")
            i_val = md_cols.index("VALTODAY")
            for row in md_rows:
                try:
                    valtoday[row[i_sid]] = float(row[i_val] or 0)
                except (TypeError, ValueError):
                    continue
        if not rows:
            return []
        idx_secid = columns.index("SECID") if "SECID" in columns else 0
        idx_status = columns.index("STATUS") if "STATUS" in columns else None
        idx_type = columns.index("SECTYPE") if "SECTYPE" in columns else None
        all_ids = []
        for row in rows:
            status = row[idx_status] if idx_status is not None else "A"
            sec_type = row[idx_type] if idx_type is not None else ""
            if status != "A":
                continue
            if board == "TQBR" and sec_type in ("IFA1", "IFA2", "IFA3", "IFAY"):
                continue
            all_ids.append(row[idx_secid])
    except Exception as e:
        print(f"[REF] securities.json {board} fetch failed: {e}")
        return []

    if top_n <= 0:
        return all_ids

    # Step 2a: rank by the turnover we already have (no extra requests)
    if any(v > 0 for v in valtoday.values()):
        return sorted(all_ids, key=lambda s: valtoday.get(s, 0.0), reverse=True)[:top_n]

    # Step 2b: marketdata empty — probe a candidate pool by VALTODAY
    pool = all_ids[:min(120, len(all_ids))]

    def _valtoday(secid: str) -> float:
        urls = [
            f"{BASE}/engines/{eng}/boards/{board}/securities/{secid}/boardprice.json?iss.meta=off&iss.depth=1",
            f"{BASE}/engines/{eng}/securities/{secid}.json?iss.meta=off",
        ]
        for url_bp in urls:
            try:
                r2 = urllib.request.Request(url_bp, headers={"User-Agent": "moex-screener/3.1"})
                with urllib.request.urlopen(r2, timeout=12) as resp:
                    bp = _json.loads(resp.read().decode("utf-8"))
                md = bp.get("marketdata") or {}
                cols = md.get("columns") or []
                brows = md.get("data") or []
                if not brows or "VALTODAY" not in cols:
                    continue
                return float(brows[-1][cols.index("VALTODAY")] or 0)
            except Exception:
                continue
        return 0.0

    ranked = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for secid, val in zip(pool, ex.map(_valtoday, pool)):
            ranked[secid] = val
    top = sorted(ranked, key=lambda s: ranked[s], reverse=True)[:top_n]
    # Guard: if VALTODAY ranking returned all zeros (API hiccup), fall back to first top_n
    if all(v == 0 for v in ranked.values()):
        top = all_ids[:top_n]
    return top


def _get_equities() -> list:
    """Return EQUITIES: explicit list from env, or CORE_EQUITIES fallback."""
    if EQUITIES:
        return EQUITIES
    return list(CORE_EQUITIES)


def _get_futures() -> list:
    """Return FUTURES: explicit list from env, or top FUTURES_TOP_N RFUD by VALTODAY."""
    if FUTURES:
        return FUTURES
    return _fetch_board_securities("RFUD", top_n=FUTURES_TOP_N)

# Arba (M10) — linked instrument pairs: (regular, preferred)
ARBA_PAIRS = [
    ("SBER", "SBERP"),
    ("GAZP", "GAZPP"),
    ("VTBR", "VTBRP"),
    ("TATN", "TATNP"),
    ("SNGS", "SNGSP"),
    ("MTLR", "MTLRP"),
]

# MOEX lot sizes (shares per lot) — used to convert QTY → lots
LOT_SIZES = {
    "SBER": 10, "GAZP": 10, "LKOH": 1, "VTBR": 1, "MGNT": 1,
    "ROSN": 1, "GMKN": 10, "TATN": 1, "SNGS": 10, "PLZT": 1,
    "SBERP": 10, "GAZPP": 10, "VTBRP": 1, "TATNP": 1,
    "YDEX": 1, "T": 1, "NVTK": 1, "SNGSP": 10,
    "MTLR": 1, "MTLRP": 10,
}

# Trace (M11) session thresholds
TRACE_VOLUME_RATIO = 2.0   # volume spike ratio to open a session
TRACE_PRICE_TOLERANCE = 0.005
TRACE_MIN_TICKS = 3

# Scalping (M12) — "near the plan" multiplier
SCALPING_PLAN_RATIO = 2.0

RENDER_SEC = _env_float("RENDER_SEC", 5.0)
ICEBERG_VOLUME_SPIKE_PCT = _env_float("ICEBERG_VOLUME_SPIKE_PCT", 2.0)
ICEBERG_PRICE_TOLERANCE = _env_float("ICEBERG_PRICE_TOLERANCE", 0.005)
ICEBERG_HISTORY_WINDOW = _env_int("ICEBERG_HISTORY_WINDOW", 20)
SPREAD_ALERT_THRESHOLD = _env_float("SPREAD_ALERT_THRESHOLD", 0.3)

# Robot DB (M08 v2)
# Robot DB lives next to the script unless ROBOT_DB_DIR overrides it
# (inside Docker the env var is set to /app/robot_db).
ROBOT_DB_DIR = os.getenv("ROBOT_DB_DIR") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "robot_db"
)
SOUND_ALERT_ENABLED = os.getenv("SOUND_ALERT_ENABLED", "true").lower() == "true"

# Spread zone multiplier: how far outside bid/ask to consider "near the spread"
# For wide-spread tickers (ROSN, TATN, SNGS), use a larger zone
SPREAD_ZONE_FACTOR = float(os.getenv("SPREAD_ZONE_FACTOR", "3.0"))


# ---------------------------------------------------------------------------
# STOMP utilities (same as v3)
# ---------------------------------------------------------------------------
class _STOMPFrame:
    def __init__(self, cmd, headers=None, body=""):
        self.cmd = cmd
        self.headers = headers or {}
        self.body = body


def _stomp_to_bytes(frame) -> bytes:
    lines = [frame.cmd.encode("utf-8")]
    for k, v in frame.headers.items():
        lines.append(f"{k}:{v}".encode("utf-8"))
    lines.append(b"")
    if isinstance(frame.body, str):
        lines.append(frame.body.encode("utf-8"))
    elif isinstance(frame.body, bytes):
        lines.append(frame.body)
    lines.append(b"\x00")
    return b"\n".join(lines)


def _stomp_parse(data) -> _STOMPFrame:
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    data = data.rstrip("\x00")
    lines = data.split("\n")
    if not lines:
        return _STOMPFrame("ERROR", {}, "")
    cmd = lines[0]
    headers = {}
    body_start = 1
    for i, line in enumerate(lines[1:], 1):
        if line == "":
            body_start = i + 1
            break
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k] = v
    body = "\n".join(lines[body_start:]) if body_start < len(lines) else ""
    return _STOMPFrame(cmd, headers, body)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class TickerState:
    ticker: str
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    day_volume: Optional[float] = None
    day_value: Optional[float] = None
    updates: int = 0
    last_update: Optional[datetime] = None
    bid_sizes: list = field(default_factory=list)
    ask_sizes: list = field(default_factory=list)
    trade_times: list = field(default_factory=list)
    trade_volumes: list = field(default_factory=list)
    trade_prices: list = field(default_factory=list)
    trade_buysell: list = field(default_factory=list)  # B/S per real trade (M08 direction)
    trade_gaps: list = field(default_factory=list)
    strike_gaps: list = field(default_factory=list)    # inter-strike gaps (10-60s robots)
    last_strikes: dict = field(default_factory=dict)  # {price_level: [ts1, ts2, ...]}
    history: list = field(default_factory=list)  # (ts, last, vol)
    _trade_keys: set = field(default_factory=set)
    _trade_key_order: list = field(default_factory=list)

    def push_candle(self, ts, last, vol):
        self.last = last
        self.volume = vol
        self.history.append((ts, last, vol))
        if len(self.history) > 240:
            self.history.pop(0)
        self.updates += 1
        self.last_update = datetime.now()

    def push_trade(self, price, vol, ts, buy_sell=""):
        # Drop duplicates: every poll re-reads a small tail of trades.json,
        # so the same trade would otherwise be counted on every cycle.
        key = (round(float(ts), 3), float(price or 0), float(vol or 0), buy_sell or "")
        if key in self._trade_keys:
            return False
        self._trade_keys.add(key)
        self._trade_key_order.append(key)
        if len(self._trade_key_order) > 2000:
            self._trade_keys.discard(self._trade_key_order.pop(0))

        self.last = price
        self.trade_times.append(ts)
        if vol:
            self.trade_volumes.append(vol)
            self.trade_prices.append(price)
            self.trade_buysell.append(buy_sell or "S")
        else:
            # Keep arrays aligned even when vol is 0/None
            self.trade_volumes.append(0.0)
            self.trade_prices.append(price)
            self.trade_buysell.append(buy_sell or "S")
        # Track inter-trade gaps for M08 robot CV
        if len(self.trade_times) >= 2:
            self.trade_gaps.append(ts - self.trade_times[-2])
        # Trade history limit: must cover ROBOT_MIN_LIFETIME_MIN * 60 / ROBOT_STRIKE_MIN_SEC
        # so the oldest strike of a valid robot is still in the window.
        # With 2 min lifetime and 6s min interval: 2*60/6 = 20 strikes minimum,
        # but we keep 600 to be safe for slower robots (up to 90s interval → 2*60/90 ≈ 2).
        MAX_TRADES = 600
        if len(self.trade_times) > MAX_TRADES:
            self.trade_times.pop(0)
            self.trade_volumes.pop(0)
            self.trade_prices.pop(0)
            self.trade_gaps.pop(0) if self.trade_gaps else None
            self.trade_buysell.pop(0)
        # Keep the dedup set bounded (it only needs to cover the recent tail)
        if len(self._trade_keys) > MAX_TRADES * 4:
            self._trade_keys.clear()
            self._trade_keys.update(
                (round(t, 3), round(p, 6) if p is not None else None, v)
                for t, p, v in zip(self.trade_times[-MAX_TRADES:],
                                   self.trade_prices[-MAX_TRADES:],
                                   self.trade_volumes[-MAX_TRADES:])
            )
        self.updates += 1
        return True

    # Backwards-compat alias
    push_trade_point = push_trade

    def set_book(self, bid, ask, bid_sizes, ask_sizes):
        self.bid = bid
        self.ask = ask
        self.bid_sizes = bid_sizes
        self.ask_sizes = ask_sizes

    @property
    def avg_trade_size(self) -> Optional[float]:
        if len(self.trade_volumes) < 3:
            return None
        return statistics.fmean(self.trade_volumes[-20:])

    @property
    def price_change_60s(self) -> Optional[float]:
        h = self.history
        if len(h) < 2:
            return None
        now = h[-1][0]
        then = now - 60
        before = [v for t, v, _ in h if t >= then and v]
        if not before or h[-1][1] is None:
            return None
        return (h[-1][1] - before[0]) / before[0] if before[0] else None


@dataclass
class ArbaPair:
    """Arbitrage pair state."""
    a: str
    b: str
    spread_history: list = field(default_factory=list)  # (ts, spread)
    fair: Optional[float] = None
    z_score: Optional[float] = None

    @property
    def label(self) -> str:
        return f"{self.a}/{self.b}"


@dataclass
class TraceSession:
    """Digital trace: one algorithm session."""
    ticker: str
    started: float
    end_time: Optional[float] = None
    start_price: Optional[float] = None
    end_price: Optional[float] = None
    volume_absorbed: float = 0.0
    active: bool = True

    def close(self, now, last_price):
        if not self.active:
            return
        self.active = False
        self.end_time = now
        self.end_price = last_price

    @property
    def duration_sec(self) -> Optional[float]:
        if not self.end_time:
            return None
        return self.end_time - self.started

    @property
    def price_move_pct(self) -> Optional[float]:
        if not self.start_price or not self.end_price or self.start_price == 0:
            return None
        return (self.end_price - self.start_price) / self.start_price


@dataclass
class Signal:
    module: str
    ticker: str
    value: float
    text: str
    severity: int = 1


class Module:
    name = "base"

    def evaluate(self, states: dict, ctx: dict) -> list[Signal]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# M01 — Activity
# ---------------------------------------------------------------------------
class M01_Activity(Module):
    name = "М1 Кто торгуется"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            if s.day_value:
                out.append(Signal(
                    self.name, t, float(s.day_value),
                    f"day value {s.day_value:,.0f} RUB",
                    severity=min(5, int(s.day_value / 1e8) + 1),
                ))
        return out


# ---------------------------------------------------------------------------
# M02 — Price jolt
# ---------------------------------------------------------------------------
class M02_PriceJolt(Module):
    name = "М2 Скачок цены"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            chg = s.price_change_60s
            if chg is not None and abs(chg) >= 0.003:
                out.append(Signal(
                    self.name, t, round(chg * 100, 2),
                    f"{chg * 100:+.2f}% за 60s",
                    severity=min(5, int(abs(chg * 1000))),
                ))
        return out


# ---------------------------------------------------------------------------
# M03 — Volume spike
# ---------------------------------------------------------------------------
class M03_VolumeSpike(Module):
    name = "М3 Всплеск объёма"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            vols = [v for _, _, v in s.history if v]
            if len(vols) < 5:
                continue
            recent = vols[-5:]
            prev = vols[:-1]
            if not prev:
                continue
            mu = statistics.fmean(prev)
            sd = statistics.pstdev(prev) if len(prev) > 1 else 0
            if mu <= 0 or sd == 0:
                continue
            z = (recent[-1] - mu) / sd
            if z >= 3:
                out.append(Signal(
                    self.name, t, round(z, 1),
                    f"объём x{z:.1f} от нормы",
                    severity=min(5, int(z)),
                ))
        return out


# ---------------------------------------------------------------------------
# M04 — Big trades
# ---------------------------------------------------------------------------
class M04_BigTrades(Module):
    name = "М4 Мегасделки"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            if not (s.last and s.volume):
                continue
            est_value = s.last * s.volume
            if est_value >= 1_000_000:
                out.append(Signal(
                    self.name, t, round(est_value / 1e6, 2),
                    f"~{est_value / 1e6:.1f}M R per tick",
                    severity=3,
                ))
        return out


# ---------------------------------------------------------------------------
# M05 — Plites (real book depth 5)
# ---------------------------------------------------------------------------
class M05_Plites(Module):
    name = "М5 Толстые уровни"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            # Use real book_sizes if available, else spread heuristic
            for side, sizes in (("bid", s.bid_sizes), ("ask", s.ask_sizes)):
                for i in range(len(sizes) - 1):
                    if sizes[i] and sizes[i + 1]:
                        ratio = sizes[i] / sizes[i + 1]
                        if ratio >= 5:
                            out.append(Signal(
                                self.name, t, round(ratio, 1),
                                f"{side} level {i+1}: x{ratio:.1f} (wall {sizes[i]})",
                                severity=3,
                            ))
                            break
            # Fallback: spread heuristic when no book data
            if not s.bid_sizes and not s.ask_sizes and s.bid and s.ask:
                width = s.ask - s.bid
                if width > 0 and width < 0.1:
                    out.append(Signal(
                        self.name, t, round(width, 4),
                        f"spread {width:.4f} (thin)",
                        severity=2,
                    ))
        return out


# ---------------------------------------------------------------------------
# M06 — Iceberg (all tickers)
# ---------------------------------------------------------------------------
class M06_IcebergAll(Module):
    name = "М6 Айсберг-радар"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            h = s.history
            if len(h) < 5:
                continue
            recent = h[-5:]
            vols = [v for _, _, v in recent if v]
            prices = [p for _, p, _ in recent if p]
            if not vols or not prices:
                continue
            v_avg = statistics.fmean(vols)
            if v_avg == 0:
                continue
            ratio = vols[-1] / v_avg
            price_move = abs(prices[-1] - prices[-2]) / prices[-2] if prices[-2] else 0
            if ratio >= ICEBERG_VOLUME_SPIKE_PCT and price_move <= ICEBERG_PRICE_TOLERANCE:
                out.append(Signal(
                    self.name, t, round(ratio, 1),
                    f"x{ratio:.1f} объём, цена не двигается",
                    severity=3,
                ))
        return out


# ---------------------------------------------------------------------------
# M07 — Iceberg single
# ---------------------------------------------------------------------------
class M07_IcebergSingle(Module):
    name = "М7 Айсберг (по одному)"

    def evaluate(self, states, ctx):
        t = next(iter(states), None)
        if t is None:
            return []
        s = states[t]
        out = []
        h = s.history
        if len(h) >= 5:
            recent = h[-5:]
            vols = [v for _, _, v in recent if v]
            if vols and statistics.fmean(vols[:-1]) > 0:
                ratio = vols[-1] / statistics.fmean(vols[:-1])
                if ratio >= 2.5:
                    out.append(Signal(
                        self.name, t, round(ratio, 1),
                        f"x{ratio:.1f} на последнем уровне",
                        severity=4,
                    ))
        return out


# ---------------------------------------------------------------------------
# M08 — Robots (with session tracking, DB, and countdown)
# Robots repeat minimal-size orders every 6–90 sec near the spread,
# and must survive at least ROBOT_MIN_LIFETIME_MIN minutes.
# ---------------------------------------------------------------------------
ROBOT_STRIKE_MIN_SEC = float(os.getenv("ROBOT_STRIKE_MIN_SEC", "3"))   # min interval between strikes
ROBOT_STRIKE_MAX_SEC = float(os.getenv("ROBOT_STRIKE_MAX_SEC", "120")) # max interval
ROBOT_MIN_REPEATS    = int(os.getenv("ROBOT_MIN_REPEATS", "5"))       # min strikes at one price
# Robot strikes use the MINIMAL lot. Baseline = min trade qty on the ticker.
# Robot ALWAYS strikes with the SAME lot size, so anything > factor x min is noise.
ROBOT_QTY_MIN_FACTOR = float(os.getenv("ROBOT_QTY_MIN_FACTOR", "2.0"))  # strike qty <= factor x ticker MIN qty
# Direction must be consistent: majority side share >= this
ROBOT_DIR_MIN_SHARE  = float(os.getenv("ROBOT_DIR_MIN_SHARE", "0.70"))
# Only analyse trades within this window (minutes) — long windows smear the pattern
ROBOT_TRADE_WINDOW_MIN = float(os.getenv("ROBOT_TRADE_WINDOW_MIN", "90"))
ROBOT_MIN_LIFETIME_MIN = float(os.getenv("ROBOT_MIN_LIFETIME_MIN", "1"))  # min survival time (minutes)

@dataclass
class RobotSession:
    """Tracks one robot activity session (periodic minimal strikes) — aniscan-compatible."""
    ticker: str
    detected_at: float          # detection timestamp (unix)
    strike_price: float         # price level the robot works on
    strike_interval: float = 0.0     # median interval between strikes (sec) — period
    strikes: int = 0            # number of strikes observed — operationCount
    start_price: float = 0.0
    start_volume: float = 0.0   # median lot size (shares) — lots
    last_update: float = 0.0    # last strike timestamp — lastOperationDttm
    last_price: float = 0.0
    last_volume: float = 0.0
    active: bool = True
    _strike_ts: list = field(default_factory=list)
    lot_size: int = 1           # MOEX lot size for ticker
    direction: str = "flat"     # "long", "short", or "flat"
    dir_share: float = 0.0      # share of strikes on the dominant side (0..1)

    @property
    def price_move(self) -> float:
        if self.start_price == 0:
            return 0.0
        return (self.last_price - self.start_price) / self.start_price

    @property
    def duration_sec(self) -> float:
        # Duration based on observed strikes: number of strikes × median interval.
        # Robust to trade-window sliding and wall-clock vs trade-ts mismatches.
        if self.strikes > 1 and self.strike_interval > 0:
            return (self.strikes - 1) * self.strike_interval
        # Fallback: wall-clock confirmation delta
        last_confirmed = getattr(self, "_last_confirmed", None)
        if last_confirmed:
            return max(0.0, last_confirmed - self.detected_at)
        return max(0.0, self.last_update - self.detected_at)

    @property
    def first_operation_dttm(self) -> str:
        """HH:MM:SS of first strike (MSK)."""
        from datetime import datetime, timezone, timedelta
        msk_tz = timezone(timedelta(hours=3))
        return datetime.fromtimestamp(self.detected_at, tz=msk_tz).strftime("%H:%M:%S")

    @property
    def last_operation_dttm(self) -> str:
        """HH:MM:SS of last strike (MSK)."""
        from datetime import datetime, timezone, timedelta
        msk_tz = timezone(timedelta(hours=3))
        return datetime.fromtimestamp(self.last_update, tz=msk_tz).strftime("%H:%M:%S")

    @property
    def next_operation_dttm(self) -> str:
        """HH:MM:SS of next expected strike (MSK)."""
        if self.strike_interval <= 0:
            return "—"
        from datetime import datetime, timezone, timedelta
        msk_tz = timezone(timedelta(hours=3))
        next_ts = self.last_update + self.strike_interval
        return datetime.fromtimestamp(next_ts, tz=msk_tz).strftime("%H:%M:%S")

    @property
    def lot_in_minute(self) -> int:
        """Volume per minute (shares/min) = 60/period * lot_size."""
        if self.strike_interval <= 0 or self.start_volume <= 0:
            return 0
        return int((60.0 / self.strike_interval) * self.start_volume)

    def compute_next_strike(self, now: float | None = None) -> float:
        """Estimate seconds until next robot strike based on observed cadence."""
        if len(self._strike_ts) < 2:
            return 0.0
        gaps = [self._strike_ts[i+1] - self._strike_ts[i] for i in range(len(self._strike_ts)-1)]
        median_gap = statistics.median(gaps)
        elapsed = (time.time() if now is None else now) - self.last_update
        return max(0.0, median_gap - elapsed)

    def to_dict(self, now: float | None = None) -> dict:
        # Convert start_volume (shares) to lots
        lots = int(self.start_volume / self.lot_size) if self.lot_size > 0 else 0
        return {
            "ticker": self.ticker,
            "detected_at": self.detected_at,
            "last_update": self.last_update,
            "duration_sec": self.duration_sec,
            "strike_price": self.strike_price,
            "strike_interval": round(self.strike_interval, 1),
            "strikes": self.strikes,
            "start_price": self.start_price,
            "start_volume": self.start_volume,
            "last_price": self.last_price,
            "price_move_pct": round(self.price_move * 100, 4),
            "last_volume": self.last_volume,
            "active": self.active,
            "next_strike_estimate": round(self.compute_next_strike(now), 1),
            # aniscan-compatible fields
            "period": round(self.strike_interval, 1),
            "lots": lots,
            "operation_count": self.strikes,
            "first_operation_dttm": self.first_operation_dttm,
            "last_operation_dttm": self.last_operation_dttm,
            "next_operation_dttm": self.next_operation_dttm,
            "lot_in_minute": self.lot_in_minute,
            "seconds_to_next": round(self.compute_next_strike(now), 1) if self.active else 0,
            "direction": self.direction,
            "dir_share": self.dir_share,
        }


class M08_Robots(Module):
    name = "М8 Роботы (радар)"

    def __init__(self):
        self.sessions: dict[str, RobotSession] = {}
        self._db_dir = ROBOT_DB_DIR
        self._db_file = os.path.join(self._db_dir, "robots.json")
        self._last_db_write = 0.0
        self._db_lock = False
        self._market_now = 0.0  # exchange clock, kept in sync from evaluate()
        os.makedirs(self._db_dir, exist_ok=True)
        self._load_db()

    def _load_db(self):
        """Load robot sessions from JSON DB (discard records older than 30 min)."""
        if os.path.exists(self._db_file):
            try:
                with open(self._db_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                now = time.time()
                records = data.get("robots", [])
                # Use the freshest stored record as the reference clock: after
                # the session close wall-clock time keeps running, and comparing
                # against it would throw away perfectly good live sessions.
                newest = max((r.get("last_update", 0) or 0 for r in records), default=0)
                ref = newest if newest > 0 else now
                for r in records:
                    last_update = r.get("last_update", 0)
                    # Keep only sessions updated within the last 30 minutes
                    if ref - last_update > 1800:
                        continue
                    key = f"{r['ticker']}@{r.get('strike_price', 0)}"
                    if key not in self.sessions:
                        s = RobotSession(
                            ticker=r["ticker"],
                            detected_at=r.get("detected_at", 0),
                            strike_price=r.get("strike_price", 0),
                            start_price=r.get("start_price", 0),
                            start_volume=r.get("start_volume", 0),
                        )
                        s.strike_interval = r.get("strike_interval", 0)
                        s.strikes = r.get("strikes", 0)
                        s.active = r.get("active", False)
                        s.last_price = r.get("last_price", 0)
                        s.last_volume = r.get("last_volume", 0)
                        s.last_update = r.get("last_update", 0)
                        s.direction = r.get("direction", "flat")
                        self.sessions[key] = s
            except Exception as e:
                print(f"[M08] DB load error: {e}")

    def _save_db(self, force: bool = False):
        """Persist robot sessions to JSON DB (throttled to 30s).
        Only sessions updated in the last 30 min are written."""
        if not force and time.time() - self._last_db_write < 30:
            return
        now = self._market_now or time.time()
        fresh = [s.to_dict(now) for s in self.sessions.values()
                 if now - s.last_update < 1800]
        try:
            payload = {
                "updated_at": now,
                "robots": fresh,
            }
            os.makedirs(self._db_dir, exist_ok=True)
            tmp = os.path.join(self._db_dir, "robots.json.tmp")
            final = os.path.join(self._db_dir, "robots.json")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            os.replace(tmp, final)
            self._last_db_write = now
        except Exception as e:
            print(f"[M08] DB save error: {e}")

    @staticmethod
    def _play_sound():
        """Play a short Windows system beep (double 'ding')."""
        try:
            if os.name == "nt":
                import ctypes
                # 1000 Hz for 200ms, 1500 Hz for 200ms
                ctypes.windll.winmm.PlaySoundW(b"\x00\x00\x00", None, 0x40)
            else:
                import subprocess
                subprocess.run(["zenity", "--notification",
                                "--text", "MOEX robot detected"],
                               timeout=1)
        except Exception:
            pass

    def _maybe_alert_sound(self, ticker: str, is_new: bool):
        """Trigger sound on new detection or on strike countdown hitting 0."""
        if not SOUND_ALERT_ENABLED:
            return
        if is_new:
            print(f"    [SOUND] robot alert: {ticker} (new detection)")
            self._play_sound()

    def evaluate(self, states, ctx):
        out = []
        ts_now = ctx.get("ts", time.time())
        # Exchange clock: trade timestamps, not wall clock. They differ after
        # the close and during auctions (the tape freezes, the clock does not).
        market_now = float(ctx.get("market_ts") or ts_now)
        self._market_now = market_now
        new_alerts = []

# Robot detection uses REAL trade data from trades.json (MOEX ISS REST).
        # Pattern: same price struck ROBOT_MIN_REPEATS+ times
        # with inter-strike intervals in [ROBOT_STRIKE_MIN_SEC, ROBOT_STRIKE_MAX_SEC],
        # minimal QTY (<= ROBOT_QTY_MAX_FACTOR x median), within spread zone.
        for t, s in states.items():
            # Use real trade timestamps (from trades.json) if available.
            # Align trade_times with trade_prices/trade_volumes/trade_buysell:
            # all four arrays must have the same length and order.
            n = min(len(s.trade_times), len(s.trade_prices),
                    len(s.trade_volumes), len(s.trade_buysell))
            if n < ROBOT_MIN_REPEATS + 1:
                continue
            trade_times = s.trade_times[-n:]
            trade_prices = s.trade_prices[-n:]
            trade_volumes = s.trade_volumes[-n:]
            trade_buysell = s.trade_buysell[-n:]

            # Build (ts, price, qty, bs) tuples from real trades
            trade_tuples = list(zip(trade_times, trade_prices, trade_volumes, trade_buysell))
            if len(trade_tuples) < ROBOT_MIN_REPEATS:
                continue

            # Restrict to the recent window — long windows smear the periodic pattern
            if trade_tuples:
                cutoff = trade_tuples[-1][0] - ROBOT_TRADE_WINDOW_MIN * 60
                trade_tuples = [x for x in trade_tuples if x[0] >= cutoff]
            if len(trade_tuples) < ROBOT_MIN_REPEATS:
                continue

            # Group by price level (use aligned tuples: ts, price, qty, bs)
            price_hits: dict[float, list] = {}
            for ts_, price, qty_, bs_ in trade_tuples:
                if price is None:
                    continue
                price_hits.setdefault(price, []).append((ts_, price, qty_, bs_))

            # Baseline for "minimal lot": the smallest trade size on this ticker.
            # A robot always strikes with the SAME minimal lot, so anything bigger
            # or varying is noise.
            ticker_vols = [x[2] for x in trade_tuples if x[2] and x[2] > 0]
            ticker_min_qty = min(ticker_vols) if ticker_vols else 1.0
            max_allowed = ticker_min_qty * ROBOT_QTY_MIN_FACTOR

            for price, hits in price_hits.items():
                # Keep only minimal-lot strikes — this removes the ordinary trading noise
                # that previously made the direction guess random.
                hits = [h for h in hits if h[2] and 0 < h[2] <= max_allowed]
                if len(hits) < ROBOT_MIN_REPEATS:
                    continue
                # Robots work near the book. Skip prices far from the current spread
                # (zone = SPREAD_ZONE_FACTOR x spread around bid/ask).
                if s.bid is not None and s.ask is not None and s.ask > s.bid:
                    zone = SPREAD_ZONE_FACTOR * (s.ask - s.bid)
                    if not (s.bid - zone <= price <= s.ask + zone):
                        continue
                ts_list = sorted(set(h[0] for h in hits))
                if len(ts_list) < ROBOT_MIN_REPEATS:
                    continue
                gaps = [ts_list[i+1] - ts_list[i] for i in range(len(ts_list)-1)]
                if not gaps:
                    continue
                med_gap = statistics.median(gaps)
                if not (ROBOT_STRIKE_MIN_SEC <= med_gap <= ROBOT_STRIKE_MAX_SEC):
                    continue
                # Strike intervals must be stable: CV < 40% of median gap.
                gap_std = statistics.pstdev(gaps) if len(gaps) > 1 else 0
                gap_cv = gap_std / med_gap if med_gap > 0 else 1.0
                if gap_cv > 0.40:
                    continue

                strike_qtys = [h[2] for h in hits if h[2] > 0]
                if not strike_qtys:
                    continue
                med_qty = statistics.median(strike_qtys)
                # Lot size must be consistent: CV < 30% of median qty.
                qty_std = statistics.pstdev(strike_qtys) if len(strike_qtys) > 1 else 0
                qty_cv = qty_std / med_qty if med_qty > 0 else 1.0
                if qty_cv > 0.30:
                    continue

                # Direction: majority BUYSELL among the MINIMAL strikes only,
                # and it must be consistent (>= ROBOT_DIR_MIN_SHARE).
                bs_flags = [h[3] for h in hits if h[3] in ("B", "S")]
                if not bs_flags:
                    continue
                buys = sum(1 for b in bs_flags if b == "B")
                sells = len(bs_flags) - buys
                total = len(bs_flags)
                if buys >= sells:
                    sess_dir, share = "long", buys / total
                else:
                    sess_dir, share = "short", sells / total
                if share < ROBOT_DIR_MIN_SHARE:
                    # Mixed direction — not a robot, just ordinary two-way trading
                    continue

                # Robot confirmed — create/update session
                key = f"{t}@{price}"
                sess = self.sessions.get(key)
                is_new = False
                if sess is None:
                    # New session: detected_at = oldest observed strike.
                    sess = RobotSession(
                        ticker=t,
                        detected_at=ts_list[0],
                        strike_price=price,
                        start_price=price,
                        start_volume=int(med_qty),
                        lot_size=LOT_SIZES.get(t, 1),
                    )
                    sess.direction = sess_dir
                    sess.active = False  # provisional until min lifetime reached
                    self.sessions[key] = sess
                    is_new = True
                else:
                    # Existing session (active or provisional) — keep original detected_at
                    sess.direction = sess_dir

                sess._strike_ts = ts_list[-20:]
                sess.strikes = len(ts_list)
                sess.strike_interval = round(med_gap, 1)
                sess.last_update = ts_list[-1]
                sess.last_price = price
                sess.last_volume = int(med_qty)
                sess.dir_share = round(share, 2)
                # Use current time for staleness check — the robot is confirmed NOW
                sess._last_confirmed = time.time()

                # Promote provisional → active once min lifetime is reached.
                # Always show session in dashboard: as "provisional" if below min lifetime.
                if sess.duration_sec >= ROBOT_MIN_LIFETIME_MIN * 60:
                    sess.active = True
                else:
                    # Still below min lifetime — keep provisional, don't emit signal
                    sess.next_strike_estimate = sess.compute_next_strike()
                    continue

                sess.next_strike_estimate = sess.compute_next_strike()

                text = (
                    f"{'🟢 LON' if sess.direction == 'long' else '🔴 SHR'} "
                    f"{len(ts_list)} ударов @ {price:.2f}, "
                    f"каждые ~{sess.strike_interval:.0f}s, "
                    f"QTY={int(med_qty)}, в работе {int(sess.duration_sec // 60)} мин"
                )
                out.append(Signal(
                    self.name, t, round(med_gap, 1),
                    text,
                    severity=min(5, int(med_gap / 15) + 2),
                ))
                if is_new:
                    new_alerts.append(key)
                    self._maybe_alert_sound(t, is_new=True)

        # Purge stale sessions — keeps DB fresh:
        #   * ticker no longer watched (equity list changed) after 10 market min
        #   * any session not updated for 30 market minutes
        for key in list(self.sessions.keys()):
            sess = self.sessions[key]
            idle = market_now - sess.last_update
            if (sess.ticker not in states and idle > 600) or idle > 1800:
                del self.sessions[key]
                print(f"[M08] purged stale session {key}")

        # Close sessions for prices no longer observed in recent trades
        for key, sess in list(self.sessions.items()):
            t = key.split("@")[0]
            if t not in states:
                continue
            s = states[t]
            # Check if this price was in the last 50 real trades
            recent_prices = set(s.trade_prices[-50:]) if s.trade_prices else set()
            try:
                p_val = float(key.split("@", 1)[1])
            except (ValueError, IndexError):
                continue
            if p_val not in recent_prices and sess.active:
                sess.active = False

        # Persist DB every cycle (throttled)
        if new_alerts:
            self._save_db(force=True)
        else:
            self._save_db(force=False)

        return out


# ---------------------------------------------------------------------------
# M09 — Open Interest
# ---------------------------------------------------------------------------
class M09_OpenInterest(Module):
    name = "М9 Открытый интерес"

    def __init__(self, futures: list | None = None):
        self.futures = futures or FUTURES
        self._last_fetch = 0.0
        self._oi_data: dict[str, float] = {}

    def set_oi(self, ticker, value):
        self._oi_data[ticker] = value

    def evaluate(self, states, ctx):
        if time.time() - self._last_fetch < 300 and not self._oi_data:
            return []
        self._last_fetch = time.time()
        out = []
        for fut in self.futures:
            val = self._oi_data.get(fut)
            if val:
                out.append(Signal(self.name, fut, float(val),
                                  f"OI {val:,.0f} лотов", severity=2))
        return out


# ---------------------------------------------------------------------------
# M10 — Arba (Arbitrage pairs with fair price)
# ---------------------------------------------------------------------------
class M10_Arba(Module):
    name = "М10 Арбитраж"

    def __init__(self, pairs: list[tuple[str, str]] | None = None):
        self.pairs = pairs or ARBA_PAIRS
        self.pair_states: dict[str, ArbaPair] = {
            f"{a}/{b}": ArbaPair(a, b) for a, b in self.pairs
        }

    def update_pair(self, a: str, b: str, last_a: float, last_b: float):
        key = f"{a}/{b}"
        p = self.pair_states.setdefault(key, ArbaPair(a, b))
        spread = last_a - last_b
        p.spread_history.append((time.time(), spread))
        if len(p.spread_history) > 120:
            p.spread_history.pop(0)
        # fair = mean of last 10 spreads
        if len(p.spread_history) >= 5:
            recent_spreads = [s for _, s in p.spread_history[-10:]]
            p.fair = statistics.fmean(recent_spreads)
            sd = statistics.pstdev(recent_spreads) if len(recent_spreads) > 1 else 0
            if sd > 0:
                p.z_score = (spread - p.fair) / sd

    def evaluate(self, states, ctx):
        out = []
        for key, p in self.pair_states.items():
            if not p.spread_history:
                continue
            current_spread = p.spread_history[-1][1]
            if p.fair is not None and p.z_score is not None:
                if abs(p.z_score) >= 2:
                    direction = "выше" if p.z_score > 0 else "ниже"
                    out.append(Signal(
                        self.name, key, round(current_spread, 4),
                        f"спред {current_spread:.4f}, fair {p.fair:.4f}, "
                        f"z={p.z_score:+.2f} ({direction})",
                        severity=min(5, int(abs(p.z_score))),
                    ))
            # Also flag negative spread (arbitrage window)
            if current_spread < -0.01:
                out.append(Signal(
                    self.name, key, round(current_spread, 4),
                    f"ОТРИЦАТЕЛЬНЫЙ СПРЕД {current_spread:.4f}",
                    severity=5,
                ))
        return out


# ---------------------------------------------------------------------------
# M11 — Trace (digital trace sessions)
# ---------------------------------------------------------------------------
class M11_Trace(Module):
    name = "М11 След алгоритма"

    def __init__(self):
        self.sessions: dict[str, TraceSession] = {}

    def open_session(self, ticker: str, ts: float, price: float, vol: float):
        if ticker in self.sessions and self.sessions[ticker].active:
            return
        s = TraceSession(ticker=ticker, started=ts, start_price=price, volume_absorbed=vol)
        self.sessions[ticker] = s
        print(f"    [TRACE-OPEN] {ticker} @ {price} vol={vol}")

    def feed_session(self, ticker: str, ts: float, price: float, vol: float):
        sess = self.sessions.get(ticker)
        if not sess or not sess.active:
            return
        sess.volume_absorbed += vol

    def close_session(self, ticker: str, ts: float, price: float):
        sess = self.sessions.get(ticker)
        if sess and sess.active:
            sess.close(ts, price)

    def evaluate(self, states, ctx):
        out = []
        now = ctx.get("ts", time.time())
        for t, s in states.items():
            sess = self.sessions.get(t)
            if not sess:
                continue
            # Check for ongoing session with significant duration
            if sess.active and sess.start_price and s.last:
                moved = abs(s.last - sess.start_price) / sess.start_price
                if moved >= 0.002:  # 0.2% move during session
                    out.append(Signal(
                        self.name, t, round(moved * 100, 3),
                        f"в работе {int(now - sess.started)}s, "
                        f"{sess.start_price} → {s.last}, "
                        f"объём {sess.volume_absorbed:,.0f}",
                        severity=3,
                    ))
            # Also show recently closed sessions (last 5)
            if not sess.active and sess.end_time:
                dur = sess.duration_sec or 0
                if dur >= 10 and sess.price_move_pct is not None:
                    out.append(Signal(
                        self.name, t, round(sess.price_move_pct * 100, 3),
                        f"сессија {int(dur)}s, "
                        f"{sess.start_price} → {sess.end_price}, "
                        f"объём {sess.volume_absorbed:,.0f}",
                        severity=2,
                    ))
        return out


# ---------------------------------------------------------------------------
# M12 — Scalping / Plan (large order near the plan)
# ---------------------------------------------------------------------------
class M12_Scalping(Module):
    name = "М12 Скальпинг"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            avg_size = s.avg_trade_size
            if avg_size is None or avg_size <= 0:
                continue
            last_vol = s.volume
            if last_vol is None:
                continue
            ratio = last_vol / avg_size
            if ratio >= SCALPING_PLAN_RATIO:
                out.append(Signal(
                    self.name, t, round(ratio, 1),
                    f"x{ratio:.1f} от плана ({last_vol:,.0f} шт)",
                    severity=min(4, int(ratio)),
                ))
        return out


# ---------------------------------------------------------------------------
# M13 — Volatility (ATR-style on last 5 candles)
# ---------------------------------------------------------------------------
class M13_Volatility(Module):
    name = "М13 Раскачка"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            h = s.history
            if len(h) < 6:
                continue
            # 5-period range = max(high)-min(low) approximated by last value
            recent = h[-6:]
            prices = [p for _, p, _ in recent if p]
            if len(prices) < 2:
                continue
            hi = max(prices)
            lo = min(prices)
            if lo == 0:
                continue
            range_pct = (hi - lo) / lo
            if range_pct >= 0.01:  # >= 1% range in last ~5 polls
                out.append(Signal(
                    self.name, t, round(range_pct * 100, 3),
                    f"диапазон {range_pct*100:.2f}% за {len(h)-1} обновлений",
                    severity=min(4, int(range_pct * 1000)),
                ))
        return out


# ---------------------------------------------------------------------------
# M14 — VWAP deviation
# ---------------------------------------------------------------------------
class M14_VWAP(Module):
    name = "М14 Отрыв от средней"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            h = s.history
            if len(h) < 5:
                continue
            prices = [p for _, p, _ in h[-5:] if p]
            vols = [v for _, _, v in h[-5:] if v]
            if not prices or not vols:
                continue
            # VWAP = sum(price*vol) / sum(vol)
            total_vol = sum(vols)
            if total_vol == 0:
                continue
            vwap = sum(p * v for p, v in zip(prices, vols)) / total_vol
            last = prices[-1]
            dev_pct = (last - vwap) / vwap if vwap else 0
            if abs(dev_pct) >= 0.005:  # >= 0.5% from VWAP
                side = "над" if dev_pct > 0 else "под"
                out.append(Signal(
                    self.name, t, round(dev_pct * 100, 3),
                    f"{side} VWAP на {abs(dev_pct)*100:.2f}% "
                    f"(vwap {vwap:.4f}, last {last:.4f})",
                    severity=min(4, int(abs(dev_pct * 1000))),
                ))
        return out


# ---------------------------------------------------------------------------
# M15 — Крупные деньги (whale / institutional volume)
# ---------------------------------------------------------------------------
class M15_Fan(Module):
    name = "М15 Крупные деньги"

    def __init__(self):
        self.threshold = _env_int("FAN_VOLUME_THRESHOLD", 1_000_000)

    def evaluate(self, states, ctx):
        out = []
        # Tickers with day volume >= threshold: large investors active
        for t, s in states.items():
            if not s.day_volume:
                continue
            if s.day_volume >= self.threshold:
                out.append(Signal(
                    self.name, t, float(s.day_volume),
                    f"крупные деньги: {s.day_volume:,.0f} шт за день",
                    severity=min(5, int(s.day_volume / 1e6)),
                ))
        return out


# ---------------------------------------------------------------------------
# M16 — Валюты (forex / CURREN board)
# ---------------------------------------------------------------------------
FOREX_TICKERS = [
    "USD-05M26", "USD-09M26", "EUR-05M26", "EUR-09M26",
    "CNY-05M26", "BRL-05M26", "GBP-05M26",
]

def _fetch_forex_boardprice(security: str) -> dict:
    url = (
        f"{BASE}/engines/stock/markets/forex/boards/CURREN"
        f"/securities/{security}/boardprice.json?iss.meta=off&iss.depth=5"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}
    md = payload.get("marketdata") or {}
    rows = md.get("data") or []
    if not rows:
        return {}
    cols = md.get("columns") or []
    d = dict(zip(cols, rows[-1]))

    def _f(k):
        v = d.get(k)
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    return {"last": _f("LAST"), "bid": _f("BID"), "ask": _f("OFFER"),
            "spread": _f("SPREAD"), "value": _f("VALTODAY")}


class M16_Forex(Module):
    name = "М16 Валюты"

    def __init__(self):
        self.forex_states: dict[str, TickerState] = {}

    def poll(self):
        for sec in FOREX_TICKERS:
            data = _fetch_forex_boardprice(sec)
            if not data:
                continue
            ts_ = self.forex_states.setdefault(sec, TickerState(sec))
            ts_.last = data.get("last")
            ts_.bid = data.get("bid")
            ts_.ask = data.get("ask")
            ts_.day_value = data.get("value")
            if ts_.last:
                ts_.push_candle(time.time(), ts_.last, 0.0)

    def evaluate(self, states, ctx):
        self.poll()
        out = []
        for sec, s in self.forex_states.items():
            if not s.last:
                continue
            width = (s.ask - s.bid) if s.ask and s.bid else 0
            if s.day_value and s.day_value >= 1_000_000:
                out.append(Signal(
                    self.name, sec, float(s.day_value),
                    f"day value {s.day_value:,.0f} RUB, spread {width:.4f}",
                    severity=2,
                ))
        return out


# ---------------------------------------------------------------------------
# REST fetchers
# ---------------------------------------------------------------------------
def _fetch_boardprice_rest(ticker: str, depth: int = 5) -> dict:
    url = (
        f"{BASE}/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}/boardprice.json?iss.meta=off&iss.depth={depth}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}

    md = payload.get("marketdata") or {}
    rows = md.get("data") or []
    if not rows:
        return {}
    cols = md.get("columns") or []
    d = dict(zip(cols, rows[-1]))

    def _f(k):
        v = d.get(k)
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    return {
        "last": _f("LAST"),
        "bid": _f("BID"),
        "ask": _f("OFFER"),
        "qty": _f("QTY"),
        "volume": _f("VOLTODAY"),
        "day_volume": _f("VOLTODAY"),
        "day_value": _f("VALTODAY"),
        "numtrades": _f("NUMTRADES"),
        "spread": _f("SPREAD"),
    }


def _fetch_trades_rest(ticker: str, limit: int = 50, total: int | None = None) -> list[dict]:
    """Fetch the last N trades from MOEX ISS REST (guest access, trades.json).

    ISS ignores iss.limit/iss.sort on trades.json and returns at most 5000
    rows starting from the beginning of the session, so the "last" trades of
    a liquid ticker would really be its morning trades. boardprice gives us
    NUMTRADES (trades executed today) — use it as the offset via `start=` to
    read the actual tail of the tape.

    Returns list of {ts, price, qty, buy_sell} sorted oldest-first.
    """
    start = 0
    if total and total > limit:
        start = max(0, int(total) - limit - 5)
    url = (
        f"{BASE}/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}/trades.json"
        f"?iss.meta=off&start={start}"
    )

    def _get(url):
        req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())

    payload = None
    try:
        # Probe total row count so we can jump to the tail of the session.
        probe = _get(f"{base_url}&iss.limit=1")
        cursor = (probe.get("trades") or {}).get("cursor") or {}
        total = int(cursor.get("TOTAL") or 0)
        if total > limit:
            start = total - limit
            payload = _get(f"{base_url}&iss.limit={limit}&iss.start={start}")
        else:
            payload = _get(f"{base_url}&iss.limit={limit}")
    except Exception:
        try:
            payload = _get(f"{base_url}&iss.limit={limit}")
        except Exception:
            return []

    if payload is None:
        return []

    rows = (payload.get("trades") or {}).get("data") or []
    cols = (payload.get("trades") or {}).get("columns") or []
    out = []
    for row in rows:
        d = dict(zip(cols, row))
        try:
            # SYSTIME = "YYYY-MM-DD HH:MM:SS" in UTC (verified against real tape:
            # the last trade of a 15:48 MSK session shows 12:48 here).
            systime_str = d.get("SYSTIME", "")
            if systime_str:
                from datetime import datetime as _dt
                dt = _dt.strptime(systime_str, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc)
                ts = dt.timestamp()
            else:
                ts = time.time()
            price = float(d.get("PRICE", 0))
            qty = float(d.get("QUANTITY", 0))
            bs = d.get("BUYSELL", "")
            out.append({"ts": ts, "price": price, "qty": qty, "buy_sell": bs})
        except (ValueError, TypeError):
            continue
    # Sort by real timestamp (ISS paging order is not guaranteed) and keep the
    # requested amount of the freshest trades.
    out.sort(key=lambda r: r["ts"])
    return out[-limit:] if limit else out


def _fetch_oi_rest(ticker: str) -> Optional[float]:
    # Derivatives live on the FORTS engine/board (RFUD), not on the stock engine
    url = (
        f"{BASE}/engines/futures/markets/forts"
        f"/securities/{ticker}/openInterest.json?iss.meta=off"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    md = payload.get("marketdata") or {}
    rows = md.get("data") or []
    if not rows:
        return None
    cols = md.get("columns") or []
    d = dict(zip(cols, rows[-1]))
    # futures.json/marketdata expose OPENPOSITION; older endpoints used OPENINTEREST
    oi = d.get("OPENPOSITION")
    if oi in (None, ""):
        oi = d.get("OPENINTEREST")
    try:
        return float(oi) if oi not in (None, "") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class ScreenerEngine:
    def __init__(self,
                 equities: list | None = None,
                 futures: list | None = None,
                 arba_pairs: list | None = None):
        self.equities = equities or _get_equities()
        self.futures = futures or _get_futures()
        if not self.equities:
            print("[BOOT] No equities — falling back to CORE_EQUITIES")
            self.equities = list(CORE_EQUITIES)
        if not self.futures:
            print("[BOOT] No futures from ISS — M09 (open interest) disabled")
            self.futures = []
        self.arba_pairs = arba_pairs or ARBA_PAIRS

        self.states: dict[str, TickerState] = {t: TickerState(t) for t in self.equities}
        # Add arba tickers that aren't in equities
        for a, b in self.arba_pairs:
            if a not in self.states:
                self.states[a] = TickerState(a)
            if b not in self.states:
                self.states[b] = TickerState(b)

        self.oi_module = M09_OpenInterest(self.futures)
        self.arba_module = M10_Arba(self.arba_pairs)
        self.trace_module = M11_Trace()

        self.modules = [
            M01_Activity(),
            M02_PriceJolt(),
            M03_VolumeSpike(),
            M04_BigTrades(),
            M05_Plites(),
            M06_IcebergAll(),
            M07_IcebergSingle(),
            M08_Robots(),
            self.oi_module,
            self.arba_module,
            self.trace_module,
            M12_Scalping(),
            M13_Volatility(),
            M14_VWAP(),
            M15_Fan(),
            M16_Forex(),
        ]
        self.running = True
        self._last_oi_ts = 0.0
        self._last_trade_feed_ts = 0.0
        self._market_ts = 0.0  # newest exchange trade timestamp seen

    def _handle_sigint(self, *_):
        print("\n[!] Stop")
        self.running = False

    def _poll_rest_once(self):
        ts = time.time()
        
        # Step 1: Fetch boardprice for all equities (parallel)
        import concurrent.futures
        
        def fetch_boardprice(t):
            return t, _fetch_boardprice_rest(t, depth=5)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            boardprice_results = list(executor.map(fetch_boardprice, self.equities))
        
        numtrades_by_ticker: dict[str, int] = {}
        for t, data in boardprice_results:
            if not data:
                continue
            st = self.states.setdefault(t, TickerState(t))
            st.last = data.get("last")
            st.bid = data.get("bid")
            st.ask = data.get("ask")
            st.day_volume = data.get("day_volume")
            st.day_value = data.get("day_value")

            qty = data.get("qty")
            st.volume = qty
            nt = data.get("numtrades")
            if nt:
                numtrades_by_ticker[t] = int(nt)

            # Price history for the price-based modules (M02/M13/M14).
            # Real trades are pushed in step 2 — boardprice QTY is the size of
            # the last trade, not a new trade, so it must not be counted again.
            if st.last is not None:
                st.push_candle(ts, st.last, qty or 0.0)

        # Step 2: Fetch trades for all equities (parallel).
        # Primary source: T-Bank Invest API (clean 90-min window, exact timestamps).
        # Fallback: MOEX trades.json (rate-limited, may return the whole session).
        def fetch_trades(t):
            trades = None
            try:
                import tbank_feed
                trades = tbank_feed.fetch_trades_tbank(
                    t, minutes=int(ROBOT_TRADE_WINDOW_MIN), limit=1000)
            except Exception:
                trades = None
            if trades is None:
                trades = _fetch_trades_rest(t, limit=300)
            return t, trades

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            trades_results = list(executor.map(fetch_trades, self.equities))

        for t, real_trades in trades_results:
            st = self.states.get(t)
            if not st:
                continue
            for rt in real_trades:
                if st.push_trade(rt["price"], rt["qty"], rt["ts"], rt.get("buy_sell", "")):
                    # Track the newest exchange timestamp we have seen: the
                    # market clock may lag wall-clock time (auctions, after close).
                    if rt["ts"] > self._market_ts:
                        self._market_ts = rt["ts"]

        # M10 Arba: fetch paired tickers and update spread (parallel)
        def fetch_arba_pair(pair):
            a, b = pair
            da = _fetch_boardprice_rest(a)
            db = _fetch_boardprice_rest(b)
            return a, b, da.get("last") if da else None, db.get("last") if db else None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            arba_results = list(executor.map(fetch_arba_pair, self.arba_pairs))
        
        for a, b, la, lb in arba_results:
            if la is not None and lb is not None:
                self.arba_module.update_pair(a, b, la, lb)
                # Also update M11 trace sessions: if volume spike, open session
                sa = self.states[a]
                if sa.history:
                    vols = [v for _, _, v in sa.history[-10:] if v]
                    if vols and sa.volume:
                        avg_v = statistics.fmean(vols[:-1]) if len(vols) > 1 else 0
                        if avg_v > 0 and sa.volume >= TRACE_VOLUME_RATIO * avg_v:
                            self.trace_module.open_session(a, ts, la, sa.volume)
                        else:
                            self.trace_module.feed_session(a, ts, la, sa.volume or 0)

        # OI refresh (every 5 min)
        if time.time() - self._last_oi_ts >= 300:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                oi_results = list(executor.map(_fetch_oi_rest, self.futures))
            for f, oi in zip(self.futures, oi_results):
                if oi is not None:
                    self.oi_module.set_oi(f, oi)
            self._last_oi_ts = time.time()

    def collect_signals(self) -> list[Signal]:
        ctx = {"ts": time.time(), "market_ts": self._market_ts or time.time()}
        out: list[Signal] = []
        for m in self.modules:
            out.extend(m.evaluate(self.states, ctx))
        out.sort(key=lambda s: (-s.severity, -s.value))
        return out

    def render(self, sigs: list[Signal]):
        now = _msk_now().strftime("%H:%M:%S")
        total_updates = sum(s.updates for s in self.states.values())
        print(f"\n{'=' * 64}")
        print(f"  MOEX SCREENER v3.1  ({now})  REST  updates={total_updates}")
        print(f"{'=' * 64}")

        by_module: dict[str, list[Signal]] = {}
        for s in sigs:
            by_module.setdefault(s.module, []).append(s)

        for mod in [m.name for m in self.modules]:
            if mod not in by_module:
                continue
            print(f"\n  >> {mod}")
            for s in by_module[mod][:6]:
                mark = "!" * s.severity
                print(f"    [{s.ticker:10s}] {s.text}  {mark}")

        if not sigs:
            print("\n  (no signals - quiet market)")
        print()

    def _write_snapshot(self, sigs: list[Signal]):
        """Write data/screener.json for the HTML dashboard."""
        try:
            import os as _os
            snap_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data")
            _os.makedirs(snap_dir, exist_ok=True)
            snap = {
                "ts": time.time(),
                "time": _msk_now().strftime("%Y-%m-%d %H:%M:%S"),
                "exchange_time": (
                    datetime.fromtimestamp(self._market_ts, tz=MSK_TZ).strftime("%H:%M:%S")
                    if self._market_ts else None
                ),
                "signals": [
                    {"module": s.module, "ticker": s.ticker,
                     "value": s.value, "text": s.text, "severity": s.severity}
                    for s in sigs[:80]
                ],
                "states": {},
                "robots": [],
                "arba": [],
            }
            for t, st in self.states.items():
                snap["states"][t] = {
                    "last": st.last, "bid": st.bid, "ask": st.ask,
                    "day_volume": st.day_volume, "day_value": st.day_value,
                    "updates": st.updates,
                }
            # Arba spreads (from M10 pair_states)
            for key, ps in getattr(self.arba_module, "pair_states", {}).items():
                snap["arba"].append({
                    "pair": key,
                    "pair_a": ps.a,
                    "pair_b": ps.b,
                    "spread": ps.spread_history[-1][1] if ps.spread_history else None,
                    "fair": ps.fair,
                    "z_score": ps.z_score,
                })
            # Robots (from M08) — include BOTH active and recently-closed sessions
            robots_mod = next((m for m in self.modules if m.name.startswith("М8")), None)
            if robots_mod and hasattr(robots_mod, "sessions"):
                snap["robots"] = [s.to_dict(self._market_ts or None)
                                  for s in robots_mod.sessions.values()]
            # Atomic write: tmp -> rename (no truncated JSON on crash)
            tmp_path = _os.path.join(snap_dir, "screener.json.tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(snap, f, ensure_ascii=False, indent=1)
            os.replace(tmp_path, _os.path.join(snap_dir, "screener.json"))
        except Exception as e:
            print(f"[snap] {e}")

    async def run(self, cycles: int | None = None, use_ws: bool = False):
        mode = "WebSocket" if use_ws else "REST fallback"
        print(f"MOEX Screener v3.1 — {len(self.equities)} equities + "
              f"{len(self.futures)} futures OI + {len(self.arba_pairs)} arba pairs, "
              f"{mode}, render {RENDER_SEC}s")
        print("Ctrl+C to stop\n")
        signal.signal(signal.SIGINT, self._handle_sigint)

        i = 0
        while self.running:
            self._poll_rest_once()
            sigs = self.collect_signals()
            self.render(sigs)
            self._write_snapshot(sigs)
            i += 1
            if cycles and i >= cycles:
                break
            for _ in range(int(RENDER_SEC * 2)):
                if not self.running:
                    break
                time.sleep(0.5)
        print("\nStopped.")

# ---------------------------------------------------------------------------
# HTTP server for dashboard (optional: --http flag)
# ---------------------------------------------------------------------------
def start_http_server(directory: str, port: int = 8080):
    """Serve the project directory over HTTP so dashboard.html can fetch data/screener.json.
    Runs in a daemon thread; the server lives for the lifetime of the process."""
    import http.server
    import socketserver
    import functools
    import threading

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=directory, **kw)

        def log_message(self, *args):
            pass  # dashboard polls every 5s — keep the console readable

    handler = QuietHandler

    # Allow reuse of the port and run requests in threads so a slow
    # REST poll cycle never blocks the HTTP listener.
    class ReuseTCPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    httpd = ReuseTCPServer(("0.0.0.0", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    print(f"[http] Dashboard: http://localhost:{port}/dashboard.html")
    return httpd


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
async def main():
    cycles: int | None = None
    args = sys.argv[1:]
    use_http = False
    port = 8080
    if "--rest" in args:
        args.remove("--rest")
    if "--http" in args:
        args.remove("--http")
        use_http = True
    if "--port" in args:
        idx = args.index("--port")
        if idx + 1 < len(args) and args[idx + 1].isdigit():
            port = int(args[idx + 1])
            del args[idx:idx + 2]
        else:
            del args[idx]
    if "--help" in args or "-h" in args:
        print("MOEX Screener v3.1")
        print("  python moex_screener_v3_1.py [cycles] [--rest] [--http] [--port N]")
        print("    cycles   number of poll cycles before exit (default: unlimited)")
        print("    --rest   REST mode (default and only data source in v3.1)")
        print("    --http   serve dashboard.html + data/ over HTTP")
        print("    --port   HTTP port for --http (default 8080)")
        return
    if args and args[0].isdigit():
        cycles = int(args[0])
    engine = ScreenerEngine()

    if use_http:
        import os as _os
        proj_dir = _os.path.dirname(_os.path.abspath(__file__))
        _os.makedirs(_os.path.join(proj_dir, "data"), exist_ok=True)
        start_http_server(proj_dir, port)

    await engine.run(cycles=cycles, use_ws=False)


if __name__ == "__main__":
    asyncio.run(main())
