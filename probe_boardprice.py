"""Probe: what fields does boardprice.json actually give us?"""
import sys, json, urllib.request
sys.stdout.reconfigure(encoding="utf-8")

BASE = "https://iss.moex.com/iss"

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "moex-screener/3.1"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

d = get(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities/SBER/boardprice.json?iss.meta=off&iss.depth=5")
md = d.get("marketdata") or {}
cols = md.get("columns") or []
print("boardprice cols:", cols)
rows = md.get("data") or []
for i, row in enumerate(rows[-3:]):
    print(f" row{i}:", dict(zip(cols, row)))
print(f"total rows: {len(rows)}")

# Also check: book.json with depth 10 (is it available?)
for depth in [3, 10, 20]:
    try:
        d2 = get(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities/SBER/boardprice.json?iss.meta=off&iss.depth={depth}")
        rows2 = (d2.get("marketdata") or {}).get("data") or []
        print(f"depth={depth}: {len(rows2)} rows")
    except Exception as e:
        print(f"depth={depth}: ERROR {e}")
