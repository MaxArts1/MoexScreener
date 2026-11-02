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
from datetime import datetime
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

MOEX_WS_URL = os.getenv("MOEX_WS_URL", "wss://iss.moex.com/infocx/v3/websocket")
MOEX_DOMAIN = os.getenv("MOEX_DOMAIN", "DEMO")
MOEX_LOGIN = os.getenv("MOEX_LOGIN", "guest")
MOEX_PASSCODE = os.getenv("MOEX_PASSCODE", "guest")
BASE = os.getenv("MOEX_REST_BASE", "https://iss.moex.com/iss")

EQUITIES = _csv("EQUITIES", "SBER,GAZP,LKOH,VTBR,MGNT,ROSN,GMKN,TATN,SNGS,PLZT")
FUTURES = _csv("FUTURES", "SIU6,BRU6,GDH6,MEU6")

# Arba (M10) — linked instrument pairs: (regular, preferred)
ARBA_PAIRS = [
    ("SBER", "SBERP"),
    ("GAZP", "GAZPP"),
    ("VTBR", "VTBRP"),
    ("TATN", "TATNP"),
]

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
ROBOT_DB_DIR = os.getenv("ROBOT_DB_DIR", "E:/Deepseek/ReTools/moex-screener/robot_db")
SOUND_ALERT_ENABLED = os.getenv("SOUND_ALERT_ENABLED", "true").lower() == "true"


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
    history: list = field(default_factory=list)  # (ts, last, vol)

    def push_candle(self, ts, last, vol):
        self.last = last
        self.volume = vol
        self.history.append((ts, last, vol))
        if len(self.history) > 240:
            self.history.pop(0)
        self.updates += 1
        self.last_update = datetime.now()

    def push_trade(self, price, vol, ts):
        self.last = price
        self.trade_times.append(ts)
        if vol:
            self.trade_volumes.append(vol)
            self.trade_prices.append(price)
        if len(self.trade_times) > 200:
            self.trade_times.pop(0)
            self.trade_volumes.pop(0)
            self.trade_prices.pop(0)
        self.updates += 1

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
# ---------------------------------------------------------------------------
@dataclass
class RobotSession:
    """Tracks one robot activity session."""
    ticker: str
    detected_at: float          # time when robot first detected
    cv: float                   # coefficient of variation
    start_price: float
    start_volume: float
    last_update: float = 0.0
    last_price: float = 0.0
    last_volume: float = 0.0
    active: bool = True
    next_strike_estimate: float = 0.0  # seconds until next expected strike

    def record(self, ts: float, price: float, vol: float, cv: float):
        self.last_update = ts
        self.last_price = price
        self.last_volume = vol
        self.cv = cv

    def compute_next_strike(self) -> float:
        """Estimate seconds until next robot strike based on observed cadence."""
        if len(self.gaps) < 2:
            return 0.0
        median_gap = statistics.median(self.gaps)
        return max(0.0, median_gap)

    @property
    def price_move(self) -> float:
        if self.start_price == 0:
            return 0.0
        return (self.last_price - self.start_price) / self.start_price

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.last_update - self.detected_at)

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "detected_at": self.detected_at,
            "last_update": self.last_update,
            "duration_sec": self.duration_sec,
            "start_price": self.start_price,
            "last_price": self.last_price,
            "price_move_pct": round(self.price_move * 100, 4),
            "start_volume": self.start_volume,
            "last_volume": self.last_volume,
            "cv": round(self.cv, 4),
            "active": self.active,
        }


class M08_Robots(Module):
    name = "М8 Роботы (радар)"

    def __init__(self):
        self.sessions: dict[str, RobotSession] = {}
        self._db_dir = ROBOT_DB_DIR
        self._db_file = os.path.join(self._db_dir, "robots.json")
        self._last_db_write = 0.0
        self._db_lock = False
        os.makedirs(self._db_dir, exist_ok=True)
        self._load_db()

    def _load_db(self):
        """Load robot sessions from JSON DB."""
        if os.path.exists(self._db_file):
            try:
                with open(self._db_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for r in data.get("robots", []):
                    key = f"{r['ticker']}"
                    if key not in self.sessions:
                        s = RobotSession(
                            ticker=r["ticker"],
                            detected_at=r.get("detected_at", 0),
                            cv=r.get("cv", 0),
                            start_price=r.get("start_price", 0),
                            start_volume=r.get("start_volume", 0),
                        )
                        s.active = r.get("active", False)
                        s.last_price = r.get("last_price", 0)
                        s.last_volume = r.get("last_volume", 0)
                        s.last_update = r.get("last_update", 0)
                        self.sessions[key] = s
            except Exception as e:
                print(f"[M08] DB load error: {e}")

    def _save_db(self, force: bool = False):
        """Persist robot sessions to JSON DB (throttled to 30s)."""
        if not force and time.time() - self._last_db_write < 30:
            return
        try:
            payload = {
                "updated_at": time.time(),
                "robots": [s.to_dict() for s in self.sessions.values()],
            }
            tmp = self._db_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, self._db_file)
            self._last_db_write = time.time()
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
        new_alerts = []

        for t, s in states.items():
            ts = s.trade_times
            if len(ts) < 8:
                continue
            gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
            if not gaps or statistics.fmean(gaps) <= 0:
                continue
            mu = statistics.fmean(gaps)
            sd = statistics.pstdev(gaps) if len(gaps) > 1 else 0
            if mu <= 0 or sd == 0:
                continue
            cv = sd / mu
            if cv >= 0.25:
                # Not a robot — remove stale session if exists
                if t in self.sessions and self.sessions[t].active:
                    self.sessions[t].active = False
                continue

            # Robot detected / continuing
            key = t
            sess = self.sessions.get(key)
            is_new = False
            if sess is None or not sess.active:
                sess = RobotSession(
                    ticker=t,
                    detected_at=ts_now,
                    cv=cv,
                    start_price=s.last or 0.0,
                    start_volume=s.volume or 0.0,
                )
                self.sessions[key] = sess
                is_new = True

            # Store gaps on session (for countdown)
            sess.gaps = gaps
            sess.record(ts_now, s.last or 0.0, s.volume or 0.0, cv)
            sess.next_strike_estimate = sess.compute_next_strike()

            next_in = sess.next_strike_estimate
            text = (
                f"CV={cv:.3f}, ~{next_in:.1f}s до удара, "
                f"{sess.duration_sec:.0f}s в работе, "
                f"цена {sess.start_price:.4f} → {sess.last_price:.4f} "
                f"({sess.price_move * 100:+.3f}%)"
            )

            out.append(Signal(
                self.name, t, round(cv, 3),
                text,
                severity=min(5, int(cv * 100) + 2),
            ))

            if is_new:
                new_alerts.append(t)
                self._maybe_alert_sound(t, is_new=True)

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
    name = "M14 Срыв от средней"

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
    name = "M15 Крупные деньги"

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
    name = "M16 Валюты"

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


def _fetch_oi_rest(ticker: str) -> Optional[float]:
    url = (
        f"{BASE}/engines/stock/markets/futures/boards/FUTURE"
        f"/securities/{ticker}/openInterest.json?iss.meta=off"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    rows = (payload.get("data") or [])
    if not rows:
        return None
    cols = payload.get("columns") or []
    d = dict(zip(cols, rows[-1]))
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
        self.equities = equities or EQUITIES
        self.futures = futures or FUTURES
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

    def _handle_sigint(self, *_):
        print("\n[!] Stop")
        self.running = False

    def _poll_rest_once(self):
        ts = time.time()
        for t in self.equities:
            data = _fetch_boardprice_rest(t, depth=5)
            if not data:
                continue
            st = self.states.setdefault(t, TickerState(t))
            st.last = data.get("last")
            st.bid = data.get("bid")
            st.ask = data.get("ask")
            st.day_volume = data.get("day_volume")
            st.day_value = data.get("day_value")

            # QTY = last trade volume (use it for M12)
            qty = data.get("qty")
            st.volume = qty

            if st.last is not None:
                st.push_candle(ts, st.last, qty or 0.0)
                # Simulate a trade for M08/M12 (REST only)
                st.push_trade(st.last, qty or 0.0, ts)

        # M10 Arba: fetch paired tickers and update spread
        for a, b in self.arba_pairs:
            da = _fetch_boardprice_rest(a)
            db = _fetch_boardprice_rest(b)
            la = da.get("last") if da else None
            lb = db.get("last") if db else None
            if la is not None and lb is not None:
                self.arba_module.update_pair(a, b, la, lb)
                # Also update M11 trace sessions: if volume spike, open session
                # (simplified: open session on volume > 2x history avg)
                sa = self.states[a]
                if sa.history:
                    vols = [v for _, _, v in sa.history[-10:] if v]
                    if vols and sa.volume:
                        avg_v = statistics.fmean(vols[:-1]) if len(vols) > 1 else 0
                        if avg_v > 0 and sa.volume >= TRACE_VOLUME_RATIO * avg_v:
                            self.trace_module.open_session(a, ts, la, sa.volume)
                        else:
                            self.trace_module.feed_session(a, ts, la, sa.volume or 0)

        # OI refresh
        if time.time() - self._last_oi_ts >= 300:
            for f in self.futures:
                oi = _fetch_oi_rest(f)
                if oi is not None:
                    self.oi_module.set_oi(f, oi)
            self._last_oi_ts = time.time()

    def collect_signals(self) -> list[Signal]:
        ctx = {"ts": time.time()}
        out: list[Signal] = []
        for m in self.modules:
            out.extend(m.evaluate(self.states, ctx))
        out.sort(key=lambda s: (-s.severity, -s.value))
        return out

    def render(self, sigs: list[Signal]):
        now = datetime.now().strftime("%H:%M:%S")
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
            i += 1
            if cycles and i >= cycles:
                break
            for _ in range(int(RENDER_SEC * 2)):
                if not self.running:
                    break
                time.sleep(0.5)
        print("\nStopped.")

# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
async def main():
    cycles: int | None = None
    args = sys.argv[1:]
    if "--rest" in args:
        args.remove("--rest")
    if args and args[0].isdigit():
        cycles = int(args[0])
    engine = ScreenerEngine()
    await engine.run(cycles=cycles, use_ws=False)


if __name__ == "__main__":
    asyncio.run(main())
