"""Probe MOEX REST trades.json — what does guest actually get?"""
import sys, json, urllib.request, time
sys.stdout.reconfigure(encoding="utf-8")

BASE = "https://iss.moex.com/iss"

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())

for t in ["SBER", "GAZP", "VTBR"]:
    print(f"\n=== {t} ===")
    # depth 5 last trades
    d = get(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities/{t}/trades.json?iss.meta=off&iss.depth=5")
    md = d.get("marketdata") or {}
    cols = md.get("columns") or []
    rows = md.get("data") or []
    print("cols:", cols)
    for i, row in enumerate(rows[-3:]):
        r = dict(zip(cols, row))
        print(f"  trade{i}: {r.get('TRADETIME')} QTY={r.get('QTY')} PRICE={r.get('PRICE')} BID={r.get('BID')} OFFER={r.get('OFFER')}")
    # trades between times (10 sec window)
    now_ms = time.time() * 1000
    start = f"{int((now_ms-15000)/1000)}"
    d2 = get(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities/{t}/trades.json?iss.meta=off&iss.timeframe=6&iss.datefrom=2026-11-02&iss.timeto=2026-11-02")
    rows2 = (d2.get("marketdata") or {}).get("data") or []
    print(f"trades in full day: {len(rows2)} rows (capped by server)")
    # candles 1 sec (guest has candles WS; check REST candles)
    d3 = get(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities/{t}/candles.json?iss.meta=off&iss.interval=1&iss.start=2026-11-02T15:00:00&iss.end=2026-11-02T15:05:00&iss.limit=10")
    rows3 = (d3.get("marketdata") or {}).get("data") or []
    cols3 = (d3.get("marketdata") or {}).get("columns") or []
    print("candles cols:", cols3)
    for r in rows3[:3]:
        print("  ", dict(zip(cols3, r)))
