"""T-Bank trade source for the screener (replaces rate-limited MOEX trades.json).

Provides the same interface as _fetch_trades_rest(ticker, limit) -> list of
{ts, price, qty, buy_sell} sorted oldest-first, but pulls from the T-Bank
Invest API which returns a clean time window and exact ISO timestamps.
"""
import io
import json
import ssl
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from collections import OrderedDict

TBASE = "https://invest-public-api.tinkoff.ru/rest"
TSVC = "tinkoff.public.invest.api.contract.v1"

_CTX = ssl.create_default_context()
_CTX.check_hostname = False
_CTX.verify_mode = ssl.CERT_NONE

_TOKEN = None
_UID_CACHE = {}
_TBANK_DISABLED_UNTIL = 0.0


def _load_token(path=".env.tbank"):
    global _TOKEN
    if _TOKEN is not None:
        return _TOKEN
    try:
        raw = io.open(path, encoding="utf-8").read().strip()
    except Exception:
        _TOKEN = ""
        return _TOKEN
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            if "TOKEN" in k.upper():
                _TOKEN = v.strip().strip('"').strip("'")
                return _TOKEN
    _TOKEN = raw.splitlines()[0].strip().strip('"').strip("'") if raw else ""
    return _TOKEN


def tbank_available() -> bool:
    if time.time() < _TBANK_DISABLED_UNTIL:
        return False
    return bool(_load_token())


def _call(method, payload, timeout=20):
    token = _load_token()
    if not token:
        raise RuntimeError("no token")
    req = urllib.request.Request(
        f"{TBASE}/{TSVC}.{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _q2f(q):
    if not q:
        return None
    try:
        return int(q.get("units", 0)) + int(q.get("nano", 0)) / 1e9
    except Exception:
        return None


def resolve_uid(ticker: str):
    """Resolve ticker -> TQBR instrument uid (cached)."""
    if ticker in _UID_CACHE:
        return _UID_CACHE[ticker]
    uid = None
    try:
        d = _call("InstrumentsService/FindInstrument",
                  {"query": ticker, "instrument_kind": "INSTRUMENT_TYPE_SHARE"})
        cands = [i for i in d.get("instruments", []) if i.get("ticker") == ticker]
        cands.sort(key=lambda i: (i.get("classCode") != "TQBR",
                                  not i.get("apiTradeAvailableFlag")))
        if cands:
            uid = cands[0].get("uid")
    except Exception:
        uid = None
    _UID_CACHE[ticker] = uid
    return uid


def fetch_trades_tbank(ticker: str, minutes: int = 90, limit: int = 1000):
    """Return list of {ts, price, qty, buy_sell} sorted oldest-first.

    Returns None on failure so the caller can fall back to MOEX.
    """
    if not tbank_available():
        return None
    uid = resolve_uid(ticker)
    if not uid:
        return None
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
    to = now.isoformat().replace("+00:00", "Z")
    try:
        d = _call("MarketDataService/GetLastTrades",
                  {"instrument_id": uid, "from": frm, "to": to})
    except urllib.error.HTTPError as e:
        # 401/403 => token problem, back off for a while
        if e.code in (401, 403, 429):
            globals()["_TBANK_DISABLED_UNTIL"] = time.time() + 300
        return None
    except Exception:
        return None

    out = []
    for t in d.get("trades", []):
        px = _q2f(t.get("price"))
        if px is None:
            continue
        try:
            ts = datetime.fromisoformat(t["time"].replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        qty = int(t.get("quantity", 0))
        bs = "B" if t.get("direction", "").endswith("BUY") else (
            "S" if t.get("direction", "").endswith("SELL") else "")
        out.append({"ts": ts, "price": round(px, 6), "qty": float(qty), "buy_sell": bs})
    out.sort(key=lambda r: r["ts"])
    if len(out) > limit:
        out = out[-limit:]
    return out
