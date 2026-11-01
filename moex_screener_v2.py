#!/usr/bin/env python3
"""
MOEX Multi-Module Screener v2
============================
M7 cycle: see m7-cycles/moex-screener/

9 modules:
  M01 Activity        — top tickers by volume
  M02 Price Jolt      — sharp price moves
  M03 Volume Spike    — Z-score volume outliers
  M04 Big Trades      — trades >= 1M RUB
  M05 Plites          — price approaching thick parts of the book
  M06 Iceberg (all)   — iceberg detector across tickers
  M07 Iceberg (single)— iceberg on one ticker
  M08 Robots          — robotic-pattern heuristics
  M09 Open Interest   — futures OI (5 min update)

Data source: MOEX ISS REST API (public, no auth).
Usage:
    python moex_screener_v2.py
"""

import json
import signal
import statistics
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# Force UTF-8 stdout so Cyrillic + symbols survive Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE = "https://iss.moex.com/iss"
TICKERS = [
    "SBER", "GAZP", "LKOH", "VTBR", "MGNT",
    "ROSN", "GMKN", "TATN", "SNGS", "PLZT",
]
POLL_SEC = 5
OI_FUTURE_TICKER = "SIU6"  # sample futures for OI module
DATA_START = "2026-10-31T15:00:00"  # trading day in Moscow time


# ---------------------------------------------------------------------------
# Shared data structures
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
    history: list = field(default_factory=list)     # list of (ts, last, vol)
    updates: int = 0

    def push(self, ts: float, last: Optional[float], vol: Optional[float]):
        self.last = last
        self.volume = vol
        self.history.append((ts, last, vol))
        if len(self.history) > 240:
            self.history.pop(0)
        self.updates += 1

    @property
    def last_n(self, n: int = 10):
        return self.history[-n:]

    @property
    def price_change_60s(self) -> Optional[float]:
        h = self.history
        if len(h) < 2:
            return None
        now = h[-1][0]
        then = max(t for t, _, _ in h) - 60
        before = [v for t, v, _ in h if t >= then]
        if not before or h[-1][1] is None:
            return None
        return (h[-1][1] - before[0]) / before[0] if before[0] else None


@dataclass
class Signal:
    module: str
    ticker: str
    value: float
    text: str
    severity: int = 1


class Module:
    name = "base"

    def evaluate(self, states: dict[str, TickerState], ctx: dict) -> list[Signal]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# M01 — Activity
# ---------------------------------------------------------------------------
class M01_Activity(Module):
    name = "M01 Активность"

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
    name = "M02 Резкие движения"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            chg = s.price_change_60s
            if chg is not None and abs(chg) >= 0.003:
                out.append(Signal(
                    self.name, t, round(chg * 100, 2),
                    f"{chg * 100:+.2f}% за 60s",
                    severity=min(5, int(abs(chg * 1000))) ,
                ))
        return out


# ---------------------------------------------------------------------------
# M03 — Volume spike
# ---------------------------------------------------------------------------
class M03_VolumeSpike(Module):
    name = "M03 Всплески объёма"

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
    name = "M04 Крупные сделки"

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
# M05 — Plites (thick levels)
# ---------------------------------------------------------------------------
class M05_Plites(Module):
    name = "M05 Плиты"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            if not (s.last and s.bid and s.ask):
                continue
            # rough heuristic: wide book = likely plite
            width = s.ask - s.bid
            if width > 0:
                out.append(Signal(
                    self.name, t, round(width, 3),
                    f"width {width:.3f} R - plite?",
                    severity=2,
                ))
        return out


# ---------------------------------------------------------------------------
# M06 — Iceberg all
# ---------------------------------------------------------------------------
class M06_IcebergAll(Module):
    name = "M06 Айсберги (все)"

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
            if ratio >= 2.0 and price_move <= 0.001:
                out.append(Signal(
                    self.name, t, round(ratio, 1),
                    f"x{ratio:.1f} объём, цена не двигается",
                    severity=3,
                ))
        return out


# ---------------------------------------------------------------------------
# M07 — Iceberg single (first ticker only)
# ---------------------------------------------------------------------------
class M07_IcebergSingle(Module):
    name = "M07 Айсберг (1 тикер)"

    def evaluate(self, states, ctx):
        # apply only to first ticker
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
# M08 — Robots
# ---------------------------------------------------------------------------
class M08_Robots(Module):
    name = "M08 Роботы"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            h = s.history
            if len(h) < 8:
                continue
            ts = [x[0] for x in h]
            gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
            if not gaps:
                continue
            mu = statistics.fmean(gaps)
            sd = statistics.pstdev(gaps) if len(gaps) > 1 else 0
            if mu > 0 and sd > 0 and (sd / mu) < 0.25:
                cv = sd / mu
                out.append(Signal(
                    self.name, t, round(cv, 2),
                    f"регулярность тиков (CV={cv:.2f})",
                    severity=3,
                ))
        return out


# ---------------------------------------------------------------------------
# M09 — Open interest
# ---------------------------------------------------------------------------
class M09_OpenInterest(Module):
    name = "M09 Открытый интерес"

    def __init__(self):
        self._last_fetch = 0.0

    def evaluate(self, states, ctx):
        # throttle: only fetch every 5 min
        if time.time() - self._last_fetch < 300:
            return []
        self._last_fetch = time.time()

        url = (
            f"{BASE}/engines/stock/markets/futures/boards/FUTURE"
            f"/securities/{OI_FUTURE_TICKER}/openInterest.json?iss.meta=off"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/2.0"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return [Signal(self.name, OI_FUTURE_TICKER, 0.0, f"OI: {e}", severity=1)]

        rows = (payload.get("data") or [])[-1:]
        if not rows:
            return []
        cols = payload.get("columns") or []
        row = rows[0]
        d = dict(zip(cols, row))
        oi = d.get("OPENINTEREST")
        if oi:
            return [Signal(self.name, OI_FUTURE_TICKER, float(oi),
                          f"OI {oi:,} лотов", severity=2)]
        return []


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------
def fetch_state(ticker: str, ts: float) -> TickerState:
    """Load historical candle data (test mode) or live boardprice."""
    s = TickerState(ticker=ticker)
    s.history.append((ts, 0.0, 0.0))
    url = (
        f"{BASE}/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}/candles.json?iss.meta=off&iss.start={DATA_START}&interval=1&limit=60"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        s.updates += 1
        return s

    candles = payload.get("candles") or {}
    cols = candles.get("columns") or []
    rows = candles.get("data") or []
    if rows:
        last_row = rows[-1]
        d = dict(zip(cols, last_row))
        # candles data format: [open, close, high, low, value, volume, begin, end]
        s.last = d.get("close")
        s.volume = d.get("volume")
        s.day_value = d.get("value")
        s.day_volume = d.get("volume")
        # For testing: simulate a bid/ask from close
        if s.last:
            s.bid = s.last - 0.01
            s.ask = s.last + 0.01
        # Populate history with simulated tick data from the last few candles
        for i, row in enumerate(rows[-20:]):
            dd = dict(zip(cols, row))
            s.history.append((ts + i, dd.get("close"), dd.get("volume")))
        s.history.pop(0)  # remove the dummy
        s.updates = len(rows)
    return s


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class ScreenerEngine:
    def __init__(self, tickers: list[str] | None = None):
        self.tickers = tickers or TICKERS
        self.states: dict[str, TickerState] = {}
        self.modules = [
            M01_Activity(),
            M02_PriceJolt(),
            M03_VolumeSpike(),
            M04_BigTrades(),
            M05_Plites(),
            M06_IcebergAll(),
            M07_IcebergSingle(),
            M08_Robots(),
            M09_OpenInterest(),
        ]
        self.running = True

    def _handle_sigint(self, *_):
        print("\n[!] Stop")
        self.running = False

    def poll_once(self):
        ts = time.time()
        for t in self.tickers:
            self.states[t] = fetch_state(t, ts)

    def collect_signals(self) -> list[Signal]:
        ctx = {"ts": time.time()}
        out: list[Signal] = []
        for m in self.modules:
            out.extend(m.evaluate(self.states, ctx))
        out.sort(key=lambda s: (-s.severity, -s.value))
        return out

    def render(self, sigs: list[Signal]):
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'=' * 64}")
        print(f"  MOEX SCREENER v2  ({now})")
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
                print(f"    [{s.ticker:6s}] {s.text}  {mark}")

        if not sigs:
            print("\n  (no signals - quiet market)")
        print()

    def run(self, cycles: int | None = None):
        print(f"MOEX Screener v2 - {len(self.tickers)} tickers, poll {POLL_SEC}s")
        print("Ctrl+C to stop\n")
        signal.signal(signal.SIGINT, self._handle_sigint)

        i = 0
        while self.running:
            self.poll_once()
            sigs = self.collect_signals()
            self.render(sigs)
            i += 1
            if cycles and i >= cycles:
                break
            for _ in range(POLL_SEC * 2):
                if not self.running:
                    break
                time.sleep(0.5)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
def main():
    cycles: int | None = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        cycles = int(sys.argv[1])
    engine = ScreenerEngine()
    engine.run(cycles=cycles)


if __name__ == "__main__":
    main()
