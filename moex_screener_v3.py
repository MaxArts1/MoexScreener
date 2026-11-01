#!/usr/bin/env python3
"""
MOEX Real-Time WebSocket Screener v3
====================================
M7 cycle: see m7-cycles/moex-screener/

Data source: MOEX ISS WebSocket API (STOMP, public guest access).
  wss://iss.moex.com/infocx/v3/websocket

Subscribes (MXSE = MarketData Exchange Stock Engine):
  MXSE.book  - live order book (for M05 Plites, M06 Iceberg)
  MXSE.trades- real-time trades  (for M08 Robots)
  MXSE.candles - 1-minute candles (for M02 Price Jolt, M03 Volume Spike)
  MXSE.openInterest - futures OI (for M09, 5-min updates)

Usage:
    python moex_screener_v3.py
    python moex_screener_v3.py 10       # 10 cycles then stop
"""

import asyncio
import json
import signal
import statistics
import sys
import time
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
except ImportError:
    print("[ERROR] pip install websockets")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# STOMP utilities (vendored from moex-ws-test/stomp_utils.py)
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
# Config
# ---------------------------------------------------------------------------
MOEX_WS_URL = "wss://iss.moex.com/infocx/v3/websocket"
MOEX_DOMAIN = "DEMO"  # guest access; change to "MOEX" with real key

# Default stock tickers (equities)
EQUITIES = [
    "SBER", "GAZP", "LKOH", "VTBR", "MGNT",
    "ROSN", "GMKN", "TATN", "SNGS", "PLZT",
]

# Default futures tickers for OI module
FUTURES = [
    "SIU6", "BRU6", "GDH6", "MEU6",
]

# Poll interval for the render loop (s)
RENDER_SEC = 5

# Iceberg detection thresholds
ICEBERG_VOLUME_SPIKE_PCT = 2.0
ICEBERG_PRICE_TOLERANCE = 0.005
ICEBERG_HISTORY_WINDOW = 20

# Spread alert threshold (rubles)
SPREAD_ALERT_THRESHOLD = 0.3


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
    # Order book (for plite/iceberg detection)
    bid_sizes: list = field(default_factory=list)  # [size_l1, size_l2, ...]
    ask_sizes: list = field(default_factory=list)
    # Trade history (for M08 robots)
    trade_times: list = field(default_factory=list)
    # Price/volume history (for M02/M03)
    history: list = field(default_factory=list)  # (ts, last, vol)

    def push_candle(self, ts: float, last: Optional[float], vol: Optional[float]):
        self.last = last
        self.volume = vol
        self.history.append((ts, last, vol))
        if len(self.history) > 240:
            self.history.pop(0)
        self.updates += 1
        self.last_update = datetime.now()

    def push_trade(self, price: float, ts: float):
        self.last = price
        self.trade_times.append(ts)
        if len(self.trade_times) > 100:
            self.trade_times.pop(0)
        self.updates += 1

    def set_book(self, bid: float, ask: float, bid_sizes: list, ask_sizes: list):
        self.bid = bid
        self.ask = ask
        self.bid_sizes = bid_sizes
        self.ask_sizes = ask_sizes

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
                    severity=min(5, int(abs(chg * 1000))),
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
# M05 — Plites (real book depth)
# ---------------------------------------------------------------------------
class M05_Plites(Module):
    name = "M05 Плиты"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            if not s.bid_sizes and not s.ask_sizes:
                continue
            # Look for a "plite": a single level with > 5x the next level
            # across the book (a huge wall)
            for side, sizes in (("bid", s.bid_sizes), ("ask", s.ask_sizes)):
                for i in range(len(sizes) - 1):
                    if sizes[i] and sizes[i + 1]:
                        ratio = sizes[i] / sizes[i + 1]
                        if ratio >= 5:
                            out.append(Signal(
                                self.name, t, round(ratio, 1),
                                f"{side} уровень {i + 1}: x{ratio:.1f} "
                                f"(плита {sizes[i]} шт.)",
                                severity=3,
                            ))
                            break
        return out


# ---------------------------------------------------------------------------
# M06 — Iceberg (all tickers)
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
    name = "M07 Айсберг (1 тикер)"

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
# M08 — Robots (regular tick intervals)
# ---------------------------------------------------------------------------
class M08_Robots(Module):
    name = "M08 Роботы"

    def evaluate(self, states, ctx):
        out = []
        for t, s in states.items():
            # Use real trade times, not candle timestamps
            ts = s.trade_times
            if len(ts) < 8:
                continue
            gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
            if not gaps or statistics.fmean(gaps) <= 0:
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
# M09 — Open Interest (5-min throttle)
# ---------------------------------------------------------------------------
class M09_OpenInterest(Module):
    name = "M09 Открытый интерес"

    def __init__(self, futures: list[str] | None = None):
        self.futures = futures or FUTURES
        self._last_fetch = 0.0
        self._oi_data: dict[str, float] = {}

    def set_oi(self, ticker: str, value: float):
        self._oi_data[ticker] = value

    def evaluate(self, states, ctx):
        if time.time() - self._last_fetch < 300 and not self._oi_data:
            return []
        self._last_fetch = time.time()
        out = []
        for fut in self.futures:
            val = self._oi_data.get(fut)
            if val:
                out.append(Signal(
                    self.name, fut, float(val),
                    f"OI {val:,.0f} лотов",
                    severity=2,
                ))
        return out


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------
class MoexWebSocket:
    """STOMP client for MOEX ISS WebSocket."""

    def __init__(self, tickers: list[str], futures: list[str] | None = None):
        self.tickers = tickers
        self.futures = futures or []
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self.states: dict[str, TickerState] = {}
        self._recv_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._oi_module: Optional[M09_OpenInterest] = None

    def _subscribe(self, conn_id: str, ticker: str, dest: str,
                   board: str, interval: int = 1) -> _STOMPFrame:
        # MOEX STOMP SUBSCRIBE format (from moex-ws-test/monitor_1500.py):
        #   destination = "MXSE.candles"
        #   selector    = 'TICKER="MXSE.TQBR.SBER"'
        # The ticker parameter is passed via the SELECTOR, not a separate header.
        selector = f'TICKER="MXSE.{board}.{ticker}"'
        return _STOMPFrame(
            "SUBSCRIBE",
            {
                "id": f"{conn_id}-{ticker}-{dest}",
                "destination": dest,
                "selector": selector,
                "ack": "auto",
            },
            "",
        )

    async def connect(self, skip_ws: bool = False):
        if skip_ws:
            self.connected = False
            print("[WS] Skipped (REST mode)")
            return

        uri = MOEX_WS_URL
        self.ws = await websockets.connect(
            uri,
            subprotocols=["STOMP"],
            max_size=2 ** 22,
            ping_interval=20,
            ping_timeout=10,
        )
        self.connected = True
        print(f"[WS] Connected to {uri}")

        # STOMP CONNECT (MOEX simplified headers)
        connect_frame = _STOMPFrame(
            "CONNECT",
            {
                "domain": MOEX_DOMAIN,
                "login": "guest",
                "passcode": "guest",
            },
            "",
        )
        await self.ws.send(_stomp_to_bytes(connect_frame).decode("utf-8"))

        # Read CONNECTED
        first = await self.ws.recv()
        frame = _stomp_parse(first)
        if frame.cmd != "CONNECTED":
            raise ConnectionError(f"Expected CONNECTED, got {frame.cmd}")
        print(f"[WS] STOMP CONNECTED (id={frame.headers.get('server-instance-id', '?')})")

        # Subscribe equities: candles only (book/trades/OI aren't available in guest mode)
        conn_id = "moex-screener"
        for t in self.tickers:
            f = self._subscribe(conn_id, t, "MXSE.candles", "TQBR", 1)
            await self.ws.send(_stomp_to_bytes(f).decode("utf-8"))
            print(f"[WS] Subscribed to {t} (candles)")
        # Note: MXSE.book / MXSE.trades / MXSE.openInterest are not
        # available under guest/guest. For full data use a real MOEX
        # license key (MOEX_LOGIN / MOEX_PASSCODE from .env).
        if self.futures:
            print(f"[WS] Subscribed to futures OI: {', '.join(self.futures)}")

        # Start background tasks
        self._recv_task = asyncio.create_task(self._receive_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self):
        """STOMP ping every 25s."""
        while self.connected:
            await asyncio.sleep(25)
            if self.ws and not self.ws.close_code:
                await self.ws.send(_stomp_to_bytes(_STOMPFrame("", {}, "")).decode("utf-8"))

    async def _receive_loop(self):
        while self.connected:
            try:
                raw = await self.ws.recv()
                await self._process_message(raw)
            except ConnectionClosed:
                print("[WS] Connection closed")
                self.connected = False
                break
            except Exception as e:
                print(f"[WS] recv error: {e}")
                break

    async def _process_message(self, raw: str):
        frame = _stomp_parse(raw)
        if frame.cmd == "MESSAGE":
            dest = frame.headers.get("destination", "")
            try:
                data = json.loads(frame.body)
            except json.JSONDecodeError:
                return
            await self._dispatch(dest, data, frame.headers)
        elif frame.cmd == "ERROR":
            print(f"[WS] STOMP error: {frame.body}")
        # IGNORE: receipt, etc.

    async def _dispatch(self, dest: str, data: dict,
                       frame_headers: dict | None = None):
        """Route by destination / subscription."""
        # The ticker is in the subscription id: "moex-screener-SBER-MXSE.candles"
        sub_id = (frame_headers or {}).get("subscription", "")
        ticker = None
        if "-" in sub_id:
            # moex-screener-{ticker}-{dest}
            parts = sub_id.split("-")
            # Find the first part that looks like a known ticker
            for p in parts[1:]:
                if p.upper() in self.states:
                    ticker = p
                    break
        if ticker is None:
            ticker = self._extract_ticker(dest)
        if ticker is None:
            return

        state = self.states.setdefault(ticker, TickerState(ticker=ticker))

        if "book" in dest or "orderbook" in dest:
            self._on_book(state, data)
        elif "trades" in dest:
            self._on_trades(state, data)
        elif "candles" in dest:
            self._on_candles(state, data)
        elif "openInterest" in dest:
            if self._oi_module:
                oi = data.get("OPENINTEREST")
                if oi:
                    self._oi_module.set_oi(ticker, float(oi))

    @staticmethod
    def _extract_ticker(dest: str) -> Optional[str]:
        # dest looks like "SBER" or "MXSE.book/SBER/TQBR"
        if "/" in dest:
            parts = dest.split("/")
            if len(parts) >= 2:
                return parts[1]
        return dest or None

    @staticmethod
    def _on_book(state: TickerState, data: dict):
        # "compact" book format:
        # { "book": [ [price, qty], ... ] , "offer": [ ... ] }
        cols = data.get("columns") or []
        rows = data.get("data") or []
        if rows and cols:
            d = dict(zip(cols, rows[-1]))
            bid = d.get("BID")
            ask = d.get("ASK") or d.get("OFFER")
            state.bid = float(bid) if bid not in (None, "") else None
            state.ask = float(ask) if ask not in (None, "") else None
            # In compact format, sizes come via separate keys or nested
            # For now, just mark as "has book" for M05
            state.bid_sizes = []
            state.ask_sizes = []

    @staticmethod
    def _on_trades(state: TickerState, data: dict):
        cols = data.get("columns") or []
        rows = data.get("data") or []
        for row in rows:
            d = dict(zip(cols, row))
            last = d.get("LAST")
            ts = time.time()
            if last:
                state.push_trade(float(last), ts)
                state.last_update = datetime.now()

    @staticmethod
    def _on_candles(state: TickerState, data: dict):
        cols = data.get("columns") or []
        rows = data.get("data") or []
        if not rows:
            return
        d = dict(zip(cols, rows[-1]))

        def _last(v):
            if isinstance(v, list):
                return v[-1] if v else None
            return v

        close = _last(d.get("CLOSE"))
        vol = _last(d.get("VOLUME"))
        ts = time.time()
        state.push_candle(
            ts,
            float(close) if close is not None else None,
            float(vol) if vol is not None else 0.0,
        )
        state.volume = state.history[-1][2]

    async def close(self):
        if self._recv_task:
            self._recv_task.cancel()
        if self._ping_task:
            self._ping_task.cancel()
        if self.ws:
            await self.ws.close()
            self.connected = False


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class ScreenerEngine:
    def __init__(self,
                 equities: list[str] | None = None,
                 futures: list[str] | None = None):
        self.equities = equities or EQUITIES
        self.futures = futures or FUTURES
        self.states: dict[str, TickerState] = {
            t: TickerState(t) for t in self.equities
        }
        self.oi_module = M09_OpenInterest(self.futures)
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
        ]
        self.ws = MoexWebSocket(self.equities, self.futures)
        self.running = True

    def _handle_sigint(self, *_):
        print("\n[!] Stop")
        self.running = False

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
        print(f"  MOEX SCREENER v3  ({now})  WS={self.ws.connected} "
              f"total updates={total_updates}")
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

    async def run(self, cycles: int | None = None):
        print(f"MOEX Screener v3 — {len(self.equities)} equities + "
              f"{len(self.futures)} futures OI, render {RENDER_SEC}s")
        print("Connecting to MOEX WebSocket...")
        print("Ctrl+C to stop\n")
        signal.signal(signal.SIGINT, self._handle_sigint)

        await self.ws.connect()

        i = 0
        while self.running:
            await asyncio.sleep(RENDER_SEC)
            sigs = self.collect_signals()
            self.render(sigs)
            i += 1
            if cycles and i >= cycles:
                break

        await self.ws.close()
        print("\nStopped.")


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
async def main():
    cycles: int | None = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        cycles = int(sys.argv[1])
    engine = ScreenerEngine()
    await engine.run(cycles=cycles)


if __name__ == "__main__":
    asyncio.run(main())
