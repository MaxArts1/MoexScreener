#!/usr/bin/env python3
"""
MOEX Real-Time Arbitrage Screener
==================================
Phase 4 (SOLUTION) + Phase 7 (IMPLEMENTATION) of M7 cycle.

Tracks:
  1. SBER/SBERP spread (dividend-linked arbitrage)
  2. Iceberg detection (volume spike without price movement)

Data source:
  - MOEX ISS REST API (public, no auth): https://iss.moex.com/iss/

Usage:
    python moex_screener.py

M7 cycle: see m7-cycles/moex-screener/
"""

import json
import signal
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MOEX_REST_BOARDPRICE = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR"
    "/securities/{ticker}/boardprice.json?iss.meta=off&iss.depth=5"
)

# Instruments to monitor (SBER/SBERP pair)
DEFAULT_TICKERS = ["SBER", "SBERP"]

# Iceberg detection thresholds
ICEBERG_VOLUME_SPIKE_PCT = 2.0   # volume spike = 200% above recent average
ICEBERG_PRICE_TOLERANCE = 0.005  # max price move for "no-move" detection
ICEBERG_HISTORY_WINDOW = 20       # last N trades for spike detection

# Spread detection
SPREAD_ALERT_THRESHOLD = 0.3      # rubles, alert when spread >= this


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class TickerData:
    """Real-time data for one MOEX ticker."""
    ticker: str
    price: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    updates: int = 0
    last_update: Optional[datetime] = None

    # Iceberg tracking
    volume_history: list = field(default_factory=list)
    price_history: list = field(default_factory=list)

    def record_trade(self, price: float, volume: float,
                      bid: float, ask: float):
        """Record a new quote for iceberg detection."""
        self.price = price
        self.bid = bid
        self.ask = ask
        self.volume = volume
        self.last_update = datetime.now()
        self.updates += 1

        # Keep last N entries
        self.volume_history.append(volume)
        self.price_history.append(price)
        if len(self.volume_history) > ICEBERG_HISTORY_WINDOW:
            self.volume_history.pop(0)
            self.price_history.pop(0)

    @property
    def iceberg_suspected(self) -> bool:
        """
        Iceberg heuristic:
          - current volume is a significant spike vs the rolling average
          - price has barely moved (within ICEBERG_PRICE_TOLERANCE)
          This is a placeholder — real iceberg detection would use
          order-book level data (MOEX WS order-book-5 channel).
        """
        if len(self.volume_history) < 5:
            return False

        recent_avg = sum(self.volume_history[:-1]) / len(self.volume_history[:-1])
        current_vol = self.volume_history[-1]
        if recent_avg == 0:
            return False

        vol_spike = current_vol / recent_avg
        price_move = abs(
            self.price_history[-1] - self.price_history[-2]
        )
        return (vol_spike >= ICEBERG_VOLUME_SPIKE_PCT
                and price_move <= ICEBERG_PRICE_TOLERANCE)

    @property
    def volume_spike_pct(self) -> float:
        if len(self.volume_history) < 2:
            return 0.0
        avg = sum(self.volume_history[:-1]) / len(self.volume_history[:-1])
        if avg == 0:
            return 0.0
        return ((self.volume_history[-1] - avg) / avg) * 100


# ---------------------------------------------------------------------------
# Spread calculator
# ---------------------------------------------------------------------------
class SpreadCalculator:
    """SBER/SBERP spread calculator.

    The spread here is simply ``SBERP.last - SBER.last``. For a real
    dividend-arbitrage model you would subtract the expected dividend
    value and the cost of carry. This is enough to spot large,
    abnormal gaps that often appear right before ex-dividend dates.
    """

    def __init__(self, sber: TickerData, sberp: TickerData):
        self.sber = sber
        self.sberp = sberp

    @property
    def current_spread(self) -> Optional[float]:
        if self.sber.price is None or self.sberp.price is None:
            return None
        return self.sberp.price - self.sber.price

    @property
    def is_alert(self) -> bool:
        spread = self.current_spread
        return spread is not None and abs(spread) >= SPREAD_ALERT_THRESHOLD


# ---------------------------------------------------------------------------
# MOEX REST API
# ---------------------------------------------------------------------------
def fetch_boardprice(ticker: str) -> Optional[dict]:
    """Fetch the current board-price for one ticker from MOEX ISS REST.

    Returns a dict with keys LAST, BID, ASK, VOLUME or None on error.
    """
    url = MOEX_REST_BOARDPRICE.format(ticker=ticker)
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

    data = payload.get("data") or []
    if not data:
        return None

    columns = payload.get("columns") or []
    if not columns:
        return None
    row = data[-1]
    return dict(zip(columns, row))


def fetch_quote(ticker: str) -> Optional[dict]:
    """Return (last, bid, ask, volume) for a ticker from ISS REST."""
    payload = fetch_boardprice(ticker)
    if payload is None:
        return None
    out = {}
    for key in ("LAST", "BID", "ASK", "VOLUME"):
        raw = payload.get(key)
        out[key] = float(raw) if raw not in (None, "") else None
    return out


# ---------------------------------------------------------------------------
# Main screener
# ---------------------------------------------------------------------------
class MoexScreener:
    def __init__(self, tickers: list[str] | None = None):
        self.tickers = tickers or DEFAULT_TICKERS
        self.data: dict[str, TickerData] = {
            t: TickerData(t) for t in self.tickers
        }

        self.spread_calc: Optional[SpreadCalculator] = None
        if "SBER" in self.data and "SBERP" in self.data:
            self.spread_calc = SpreadCalculator(
                self.data["SBER"], self.data["SBERP"]
            )

        self.running = True

    def _handle_sigint(self, *_):
        print("\n[!] Stopping...")
        self.running = False

    def poll_once(self):
        """Fetch the latest quote for every ticker (one REST round-trip)."""
        for ticker in self.tickers:
            quote = fetch_quote(ticker)
            if quote and quote["LAST"] is not None:
                td = self.data[ticker]
                td.record_trade(
                    price=quote["LAST"],
                    volume=quote["VOLUME"] or 0.0,
                    bid=quote["BID"] or 0.0,
                    ask=quote["ASK"] or 0.0,
                )
            else:
                print(f"[{ticker}] no data returned")

    def print_status(self):
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'=' * 60}")
        print(f"  MOEX SCREENER  ({now})")
        print(f"{'=' * 60}")

        for ticker in self.tickers:
            td = self.data[ticker]
            if td.price is None:
                print(f"  {ticker:6s}  — waiting for data")
                continue

            status = (
                f"{td.price:10.2f}  "
                f"bid={td.bid:.2f} ask={td.ask:.2f} "
                f"vol={td.volume:,.0f}"
            )
            if td.iceberg_suspected:
                status += f"  ⚡ ICEBERG (vol {td.volume_spike_pct:+.1f}%)"
            print(f"  {ticker:6s}  {status}")

        if self.spread_calc:
            spread = self.spread_calc.current_spread
            if spread is not None:
                direction = "short SBERP, long SBER" if spread > 0 else \
                            "long SBERP, short SBER"
                flag = "  ✅ SPREAD ALERT" if self.spread_calc.is_alert else ""
                print(f"\n  SBER→SBERP spread: {spread:+.4f}  "
                      f"({direction}){flag}")

        print()

    def run(self, interval: int = 5):
        print(f"MOEX screener — polling every {interval}s "
              f"(tickers: {', '.join(self.tickers)})")
        print("Ctrl+C to stop\n")

        signal.signal(signal.SIGINT, self._handle_sigint)

        while self.running:
            self.poll_once()
            self.print_status()
            # Sleep in small increments so Ctrl+C is responsive
            for _ in range(interval * 2):
                if not self.running:
                    break
                time.sleep(0.5)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    tickers = DEFAULT_TICKERS
    # Add more tickers as needed:
    # tickers += ["GASPR", "VTBR", "LKOH"]

    screener = MoexScreener(tickers)
    try:
        screener.run()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
