"""
Indices strip - a row of boxes (NIFTY 50, BANK NIFTY, ...) with a green up-arrow / red down-arrow and the % change from the
previous close. Read-only: it only asks Dhan for index prices (marketfeed/ohlc, segment IDX_I) when a page is opened.
Cached 15 seconds. If Dhan fails or an index is not found, that box is simply left out (nothing else on the page is affected).
It does not touch any spurt / volume / signal logic.
"""
import os
import io
import json
import time
import logging

log = logging.getLogger("indices_strip")

# shown name -> (names to look for in Dhan's instrument list, fallback security id or None)
INDICES = [
    ("NIFTY 50", ["NIFTY", "NIFTY50"], 13),
    ("BANK NIFTY", ["BANKNIFTY", "NIFTYBANK"], 25),
    ("SENSEX", ["SENSEX"], 51),
    ("NIFTY MIDCAP 100", ["NIFTYMIDCAP100", "NIFTYMIDCAP"], None),
    ("NIFTY IT", ["NIFTYIT"], None),
    ("INDIA VIX", ["INDIAVIX"], 21),
]
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
_HERE = os.path.dirname(os.path.abspath(__file__))
IDS_PATH = os.path.join(_HERE, "results", "idx_ids.json")
_cache = {"t": 0.0, "html": ""}
_ids_mem = {}
_ids_try = {"t": 0.0}


def _norm(s):
    return "".join(ch for ch in str(s).upper() if ch.isalnum())


def _load_ids():
    """{shown name: security id}. Fallback ids first, then whatever the instrument list gives (saved to disk, tried at most once an hour)."""
    global _ids_mem
    if _ids_mem:
        return _ids_mem
    ids = {n: i for n, _, i in INDICES if i}
    try:
        with open(IDS_PATH) as f:
            ids.update({k: int(v) for k, v in json.load(f).items()})
    except Exception:
        pass
    missing = [n for n, _, _ in INDICES if n not in ids]
    if missing and time.time() - _ids_try["t"] > 3600:
        _ids_try["t"] = time.time()
        try:
            import requests
            import pandas as pd
            r = requests.get(SCRIP_URL, timeout=60)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text), low_memory=False, dtype=str)
            seg = df[df["SEM_SEGMENT"].astype(str).str.strip() == "I"]
            found = {}
            for n, names, _ in INDICES:
                if n in ids:
                    continue
                for col in ("SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME", "SEM_CUSTOM_SYMBOL"):
                    if col not in seg.columns:
                        continue
                    m = seg[seg[col].map(_norm).isin(names)]
                    if len(m):
                        found[n] = int(m.iloc[0]["SEM_SMST_SECURITY_ID"])
                        break
            if found:
                ids.update(found)
                try:
                    os.makedirs(os.path.dirname(IDS_PATH), exist_ok=True)
                    with open(IDS_PATH, "w") as f:
                        json.dump(found, f)
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"index id lookup failed: {e}")
    _ids_mem = ids if not missing or all(n in ids for n in missing) else {}
    return ids


def _fetch():
    """[(name, last, pct)] for the indices Dhan answers for. Never raises."""
    out = []
    try:
        import live_scanner as ls
        ids = _load_ids()
        if not ids:
            return out
        resp = ls._dhan_post("marketfeed/ohlc", {"IDX_I": [int(v) for v in ids.values()]}, "indices")
        data = (resp.json().get("data") or {}).get("IDX_I") or {}
        for name, _, _ in INDICES:
            q = data.get(str(ids.get(name)))
            if not isinstance(q, dict):
                continue
            try:
                last = float(q.get("last_price"))
                prev = float((q.get("ohlc") or {}).get("close"))
                if prev > 0:
                    out.append((name, last, (last - prev) / prev * 100.0))
            except (TypeError, ValueError):
                pass
    except Exception as e:
        log.warning(f"indices fetch failed: {e}")
    return out


_CSS = ('<style>.ixs{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 12px}'
        '.ixs .c{background:#fff;border:1px solid #ddd;border-left:6px solid #888;border-radius:6px;padding:6px 12px;min-width:130px;font-family:Arial,sans-serif}'
        '.ixs .c.up{border-left-color:#0a7d33}.ixs .c.dn{border-left-color:#c62828}'
        '.ixs .n{font-size:12px;font-weight:bold;color:#555}.ixs .v{font-size:12px;color:#333}'
        '.ixs .p{font-size:15px;font-weight:bold}.ixs .up .p{color:#0a7d33}.ixs .dn .p{color:#c62828}</style>')


def strip_html():
    """The HTML of the strip ('' when nothing could be fetched). Never raises."""
    try:
        if time.time() - _cache["t"] < 15:
            return _cache["html"]
        rows = _fetch()
        if not rows:
            html = ""
        else:
            boxes = ""
            for name, last, pct in rows:
                up = pct >= 0
                boxes += (f'<div class="c {"up" if up else "dn"}"><div class="n">{name}</div><div class="v">{last:,.2f}</div>'
                          f'<div class="p">{"&#9650;" if up else "&#9660;"} {abs(pct):.2f}%</div></div>')
            html = _CSS + f'<div class="ixs">{boxes}</div>'
        _cache.update({"t": time.time(), "html": html})
        return html
    except Exception:
        return ""
