# -*- coding: utf-8 -*-
"""Login gate for a Flask scanner app. Add to app.py right after `app = Flask(__name__)`:

    import auth_gate
    auth_gate.install(app)

- Every page needs a login except /health (Render's health check), /login, /register, /setup, /logout.
- First run: open /setup once and create the admin account (only works while there are no accounts).
- Colleagues open /register; the account stays "pending" until the admin approves it at /admin/users.
- Accounts and the session key live in /var/data (the Render disk) when it is writable, so uploads/restarts keep everybody logged in.
- Passwords are stored only as salted hashes.
"""
import os
import json
import time
import secrets
import threading
from html import escape

from flask import request, redirect, session, Response
from werkzeug.security import generate_password_hash, check_password_hash

_LOCK = threading.Lock()
_OPEN = ("/health", "/login", "/register", "/setup", "/logout", "/bg/home.jpg", "/bg/page.jpg")
_FAILS = {}  # ip -> [timestamps] of recent failed logins


def _data_dir():
    d = "/var/data"
    try:
        if os.path.isdir(d) and os.access(d, os.W_OK):
            return d
    except Exception:
        pass
    return os.path.dirname(os.path.abspath(__file__))


USERS_FILE = os.path.join(_data_dir(), "users.json")
KEY_FILE = os.path.join(_data_dir(), "session.key")
TOPIC_FILE = os.path.join(_data_dir(), "alert_topic.txt")


# ---------------------------------------------------------------- alert feed (alerts also shown on the web pages)
FEED_FILE = os.path.join(_data_dir(), "alerts_feed.jsonl")
_FEED_LOCK = threading.Lock()
_FEED_KEEP = 300


def _feed_add(title, msg, prio):
    try:
        rec = {"t": time.time(), "title": str(title)[:160], "msg": str(msg)[:1500], "p": str(prio or "default")}
        with _FEED_LOCK:
            rows = []
            try:
                with open(FEED_FILE, "r", encoding="utf-8") as f:
                    rows = [x for x in f.read().splitlines() if x.strip()]
            except Exception:
                pass
            rows.append(json.dumps(rec, ensure_ascii=False))
            rows = rows[-_FEED_KEEP:]
            with open(FEED_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(rows) + "\n")
    except Exception:
        pass


def _feed_read(n=100):
    out = []
    try:
        with open(FEED_FILE, "r", encoding="utf-8") as f:
            for ln in f.read().splitlines()[-n:]:
                try:
                    out.append(json.loads(ln))
                except Exception:
                    pass
    except Exception:
        pass
    out.reverse()
    return out


def _capture_ntfy():
    """Every phone alert the scanners send to ntfy is also written to the feed, so people without the ntfy app can read it on the web page."""
    try:
        import requests
        if getattr(requests.post, "_ag_wrapped", False):
            return
        orig = requests.post

        def post(url, *a, **kw):
            resp = orig(url, *a, **kw)
            try:
                if isinstance(url, str) and url.startswith("https://ntfy.sh/") and getattr(resp, "status_code", 999) < 300:
                    h = kw.get("headers") or {}
                    d = kw.get("data")
                    if isinstance(d, bytes):
                        d = d.decode("utf-8", "replace")
                    _feed_add(h.get("Title", "Alert"), d or "", h.get("Priority", "default"))
            except Exception:
                pass
            return resp
        post._ag_wrapped = True
        requests.post = post
    except Exception:
        pass


def _ist(ts):
    return time.strftime("%d %b %H:%M", time.gmtime(ts + 19800)) + " IST"


def _feed_html(rows):
    if not rows:
        return "<div style='color:#8d9bb0;padding:6px 0'>No alert yet. New alerts appear here as soon as the scanner sends them.</div>"
    out = ""
    for r in rows:
        col = {"urgent": "#ef4444", "high": "#f59e0b"}.get(r.get("p"), "#3b82f6")
        out += (f"<div style='border-left:4px solid {col};background:rgba(24,29,39,.85);border-radius:6px;padding:6px 10px;margin:6px 0'>"
                f"<div style='font-size:11px;color:#8d9bb0'>{escape(_ist(r.get('t', 0)))}</div>"
                f"<div style='font-weight:700'>{escape(r.get('title', ''))}</div>"
                f"<div style='white-space:pre-wrap;font-size:13px'>{escape(r.get('msg', ''))}</div></div>")
    return out


def alert_topic():
    """The ntfy topic used for the scanner's phone alerts: the one the admin saved (kept on the disk), else the NTFY_TOPIC setting."""
    try:
        with open(TOPIC_FILE, "r", encoding="utf-8") as f:
            t = f.read().strip()
            if t:
                return t
    except Exception:
        pass
    return os.environ.get("NTFY_TOPIC", "").strip()


def _set_topic(t):
    with open(TOPIC_FILE, "w", encoding="utf-8") as f:
        f.write(t)
    os.environ["NTFY_TOPIC"] = t      # the scanners read NTFY_TOPIC each time they send, so the new topic is used at once


def _load():
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save(users):
    tmp = USERS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(users, f)
    os.replace(tmp, USERS_FILE)


def _key():
    try:
        with open(KEY_FILE, "r", encoding="utf-8") as f:
            k = f.read().strip()
            if k:
                return k
    except Exception:
        pass
    k = secrets.token_hex(32)
    try:
        with open(KEY_FILE, "w", encoding="utf-8") as f:
            f.write(k)
    except Exception:
        pass
    return k


def _art():
    """Background artwork (all drawn here, nothing downloaded): a faint candlestick chart, and a bull and a bear."""
    import random
    from urllib.parse import quote
    rnd = random.Random(11)
    w, h, n = 1400, 300, 70
    price, items, last = 150.0, [], None
    pts = []
    for i in range(n):
        o = price
        c = o + rnd.uniform(-22, 24) + 1.2
        hi = max(o, c) + rnd.uniform(3, 16)
        lo = min(o, c) - rnd.uniform(3, 16)
        price = max(40, min(250, c))
        pts.append((o, c, hi, lo))
    x0 = w / n
    for i, (o, c, hi, lo) in enumerate(pts):
        col = "#22c55e" if c >= o else "#ef4444"
        cx = i * x0 + x0 / 2
        y = lambda v: h - v
        items.append(f"<line x1='{cx:.1f}' x2='{cx:.1f}' y1='{y(hi):.1f}' y2='{y(lo):.1f}' stroke='{col}' stroke-width='2'/>"
                     f"<rect x='{cx - x0 * .3:.1f}' y='{y(max(o, c)):.1f}' width='{x0 * .6:.1f}' height='{max(3, abs(c - o)):.1f}' fill='{col}'/>")
    line = " ".join(f"{i * x0 + x0 / 2:.1f},{h - (p[0] + p[1]) / 2:.1f}" for i, p in enumerate(pts))
    candles = (f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {w} {h}' preserveAspectRatio='none' opacity='0.17'>{''.join(items)}"
               f"<polyline points='{line}' fill='none' stroke='#7db3ff' stroke-width='2.5' stroke-linejoin='round'/></svg>")
    bull = ("<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 160 130'>"
            "<path d='M34 40 C6 34 2 10 20 4 C18 18 30 26 46 30 Z' fill='#22c55e'/><path d='M126 40 C154 34 158 10 140 4 C142 18 130 26 114 30 Z' fill='#22c55e'/>"
            "<path d='M40 30 L120 30 L136 52 L128 92 L108 116 L52 116 L32 92 L24 52 Z' fill='#22c55e'/>"
            "<ellipse cx='80' cy='98' rx='34' ry='20' fill='#0b0f16' opacity='.45'/><ellipse cx='68' cy='100' rx='5' ry='7' fill='#0b0f16'/><ellipse cx='92' cy='100' rx='5' ry='7' fill='#0b0f16'/>"
            "<path d='M48 58 L66 64 L64 70 L46 66 Z' fill='#0b0f16'/><path d='M112 58 L94 64 L96 70 L114 66 Z' fill='#0b0f16'/>"
            "<circle cx='58' cy='68' r='3' fill='#0b0f16'/><circle cx='102' cy='68' r='3' fill='#0b0f16'/><circle cx='80' cy='116' r='7' fill='none' stroke='#0b0f16' stroke-width='3' opacity='.5'/></svg>")
    bear = ("<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 160 130'>"
            "<circle cx='38' cy='28' r='20' fill='#ef4444'/><circle cx='122' cy='28' r='20' fill='#ef4444'/><circle cx='38' cy='28' r='9' fill='#0b0f16' opacity='.4'/><circle cx='122' cy='28' r='9' fill='#0b0f16' opacity='.4'/>"
            "<ellipse cx='80' cy='70' rx='56' ry='52' fill='#ef4444'/><ellipse cx='80' cy='88' rx='25' ry='19' fill='#0b0f16' opacity='.4'/>"
            "<ellipse cx='80' cy='80' rx='9' ry='6' fill='#0b0f16'/><path d='M80 86 L80 96 M70 100 Q80 106 90 100' stroke='#0b0f16' stroke-width='3' fill='none' stroke-linecap='round'/>"
            "<circle cx='56' cy='58' r='5' fill='#0b0f16'/><circle cx='104' cy='58' r='5' fill='#0b0f16'/></svg>")
    u = lambda v: "url(\"data:image/svg+xml," + quote(v) + "\")"
    return u(candles), u(bull), u(bear)


_CANDLES, _BULL, _BEAR = _art()
DISCLAIMER = ("Disclaimer: for education and information only. Nothing here is investment advice or a recommendation to buy or sell. "
              "Trading in stocks, futures and options carries a high risk of loss, including all your money. Past results do not guarantee future results. "
              "You alone are responsible for your own decisions.")
_FOOT_CSS = ("#ag-foot{position:fixed;left:0;right:0;bottom:0;z-index:99998;padding:5px 14px;background:rgba(11,15,22,.94);border-top:1px solid #2f3a50;"
             "color:#8d9bb0;font:11px/1.35 system-ui,Arial,sans-serif;text-align:center}")
_FOOT = f"<div id=ag-foot>{DISCLAIMER}</div>"
_IDX_BOX = "<span id=ag-idx style='display:none;gap:6px;flex-wrap:wrap;align-items:center'></span>"
_IDX_JS = ("<script>(function(){var b=document.getElementById('ag-idx');if(!b)return;function f(n){return n==null?'-':n.toLocaleString('en-IN',{maximumFractionDigits:2})}"
           "function go(){fetch('/indices.json',{credentials:'same-origin'}).then(function(r){return r.ok?r.json():null}).then(function(d){"
           "if(!d||!d.rows||!d.rows.length)return;var h='';d.rows.forEach(function(x){if(x.price==null)return;var up=x.pct!=null&&x.pct>=0,c=x.pct==null?'#8d9bb0':(up?'#22c55e':'#ef4444');"
           "h+='<span style=\"background:#161c27;border:1px solid #2f3a50;border-radius:12px;padding:1px 8px;font:12px system-ui,Arial,sans-serif;white-space:nowrap;color:#e6e9ef\">'+x.name+' '+f(x.price)+"
           "' <b style=\"color:'+c+'\">'+(x.pct==null?'':(up?'&#9650; +':'&#9660; ')+x.pct.toFixed(2)+'%')+'</b></span>'});"
           "if(h){b.innerHTML=h;b.style.display='inline-flex';b.title='As of '+(d.asof||'')}}).catch(function(){})}go();setInterval(go,30000)})();</script>")
_BG = "linear-gradient(rgba(6,10,18,.55),rgba(6,10,18,.55)),url(/bg/home.jpg) center/cover no-repeat fixed,#0b0f16"
_ART_CSS = ".b,.g,.t{position:relative;z-index:1}"


_CSS = (_ART_CSS + "body{font-family:system-ui,Arial,sans-serif;background:" + _BG + ";background-attachment:fixed;color:#e6e9ef;display:flex;justify-content:center;padding:40px 16px;min-height:100vh;box-sizing:border-box}"
        ".b{background:rgba(24,29,39,.88);border:1px solid #2f3a50;border-radius:14px;padding:26px;width:100%;max-width:360px;box-shadow:0 12px 40px rgba(0,0,0,.45);align-self:flex-start;backdrop-filter:blur(6px)}"
        "h2{margin:0 0 14px}input{width:100%;box-sizing:border-box;padding:10px;margin:6px 0 12px;border-radius:6px;border:1px solid #2f3748;background:#0f1218;color:#e6e9ef}"
        "button{width:100%;padding:10px;border:0;border-radius:6px;background:#3b82f6;color:#fff;font-size:15px;cursor:pointer}"
        "a{color:#7db3ff}.e{color:#ff8a8a;margin-bottom:10px}.m{color:#8fd19e;margin-bottom:10px}small{color:#8b93a5}"
        "table{width:100%;border-collapse:collapse}td,th{padding:6px;border-bottom:1px solid #2a3140;text-align:left;font-size:14px}"
        ".bt{width:auto;padding:4px 10px;font-size:13px;margin-right:4px}")


def _page(title, body, status=200, wide=False):
    st = _CSS + (".b{max-width:640px}" if wide else ".b{margin-top:110px}")
    html = f"<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><meta name=ag-own content=1><title>{escape(title)}</title><style>{st}{_FOOT_CSS}body{{padding-bottom:70px!important}}</style><div class=b>{body}</div>{_FOOT}"
    return Response(html, status=status, mimetype="text/html")


def _form(title, action, extra_link="", msg="", err="", button="Continue"):
    body = f"<h2>{escape(title)}</h2>"
    if err:
        body += f"<div class=e>{escape(err)}</div>"
    if msg:
        body += f"<div class=m>{escape(msg)}</div>"
    body += (f"<form method=post action='{action}'><input name=u placeholder='Username' autocomplete=username required maxlength=40>"
             f"<input name=p type=password placeholder='Password (min 8 characters)' autocomplete=current-password required maxlength=100>"
             f"<button>{escape(button)}</button></form>{extra_link}")
    return _page(title, body)


def _clean_name(s):
    s = (s or "").strip().lower()
    return s if 3 <= len(s) <= 40 and all(c.isalnum() or c in "._-" for c in s) else ""


def _too_many(ip):
    now = time.time()
    lst = [t for t in _FAILS.get(ip, []) if now - t < 600]
    _FAILS[ip] = lst
    return len(lst) >= 8


def _admin():
    return session.get("role") == "admin"


def install(app):
    _capture_ntfy()
    _t = alert_topic()
    if _t:
        os.environ["NTFY_TOPIC"] = _t
    app.secret_key = _key()
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                      SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")),
                      PERMANENT_SESSION_LIFETIME=60 * 60 * 24 * 30)

    @app.before_request
    def _gate():
        p = request.path
        if p in _OPEN:
            return None
        u = session.get("user")
        if u:
            rec = _load().get(u)
            if rec and rec.get("approved"):
                session["role"] = rec.get("role", "viewer")
                if p.startswith("/admin") and not _admin():
                    return _page("Not allowed", "<h2>Admin only</h2><a href='/'>Back</a>", 403)
                if app.config.get("AG_RESTRICT") and not _admin() and p not in ("/", "/alerts", "/indices.json"):
                    ok = set(rec.get("pages") or [])
                    if not (p in ok or any(p.startswith(r + "/") for r in ok)):
                        return _page("Not allocated", "<h2>This page is not part of your account</h2><p>Ask the admin if you need it.</p><a href='/'>Back to home</a>", 403)
                return None
            session.clear()
        if not _load():
            return redirect("/setup")
        return redirect("/login")

    @app.route("/setup", methods=["GET", "POST"])
    def _setup():
        with _LOCK:
            users = _load()
            if users:
                return redirect("/login")
            if request.method == "POST":
                name, pw = _clean_name(request.form.get("u")), request.form.get("p") or ""
                if not name or len(pw) < 8:
                    return _form("Create the admin account", "/setup", err="Username 3+ characters (letters, numbers . _ -) and password 8+ characters.", button="Create admin")
                users[name] = {"hash": generate_password_hash(pw), "role": "admin", "approved": True, "created": time.strftime("%Y-%m-%d %H:%M")}
                _save(users)
                session.permanent = True
                session["user"], session["role"] = name, "admin"
                return redirect("/")
        return _form("Create the admin account", "/setup", msg="First run: this account will be the admin (only you can approve others).", button="Create admin")

    @app.route("/login", methods=["GET", "POST"])
    def _login():
        link = "<p><small>No account? <a href='/register'>Request access</a></small></p>"
        if request.method == "POST":
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
            if _too_many(ip):
                return _form("Login", "/login", link, err="Too many wrong attempts. Try again in 10 minutes.", button="Login")
            name, pw = _clean_name(request.form.get("u")), request.form.get("p") or ""
            rec = _load().get(name)
            if rec and check_password_hash(rec["hash"], pw):
                if not rec.get("approved"):
                    return _form("Login", "/login", link, err="Your account is waiting for approval.", button="Login")
                session.clear()
                session.permanent = True
                session["user"], session["role"] = name, rec.get("role", "viewer")
                return redirect("/")
            _FAILS.setdefault(ip, []).append(time.time())
            return _form("Login", "/login", link, err="Wrong username or password.", button="Login")
        return _form("Login", "/login", link, button="Login")

    @app.route("/register", methods=["GET", "POST"])
    def _register():
        link = "<p><small><a href='/login'>Back to login</a></small></p>"
        if request.method == "POST":
            name, pw = _clean_name(request.form.get("u")), request.form.get("p") or ""
            if not name or len(pw) < 8:
                return _form("Request access", "/register", link, err="Username 3+ characters (letters, numbers . _ -) and password 8+ characters.", button="Request access")
            with _LOCK:
                users = _load()
                if not users:
                    return redirect("/setup")
                if name in users:
                    return _form("Request access", "/register", link, err="That username is taken.", button="Request access")
                if sum(1 for r in users.values() if not r.get("approved")) >= 30:
                    return _form("Request access", "/register", link, err="Too many pending requests. Try later.", button="Request access")
                users[name] = {"hash": generate_password_hash(pw), "role": "viewer", "approved": False, "created": time.strftime("%Y-%m-%d %H:%M")}
                _save(users)
            return _form("Request access", "/register", link, msg="Request sent. You can log in once the admin approves it.", button="Request access")
        return _form("Request access", "/register", link, button="Request access")

    @app.route("/logout")
    def _logout():
        session.clear()
        return redirect("/login")

    @app.route("/admin/users", methods=["GET", "POST"])
    def _admin_users():
        with _LOCK:
            users = _load()
            if request.method == "POST":
                act, name = request.form.get("a"), request.form.get("n")
                if act in ("topic_set", "topic_new"):
                    t = (request.form.get("topic") or "").strip()
                    if act == "topic_new":
                        t = "scan-" + secrets.token_urlsafe(12).replace("_", "x").replace("-", "y")
                    if 8 <= len(t) <= 64 and all(c.isalnum() or c in "_-" for c in t):
                        _set_topic(t)
                    return redirect("/admin/users")
                rec = users.get(name)
                if rec and name != session.get("user"):
                    if act == "approve":
                        rec["approved"] = True
                    elif act == "remove":
                        users.pop(name, None)
                    _save(users)
                return redirect("/admin/users")
        rows = ""
        for n, r in sorted(users.items()):
            st = "admin" if r.get("role") == "admin" else ("approved" if r.get("approved") else "<b>pending</b>")
            btn = ""
            if n != session.get("user"):
                if app.config.get("AG_RESTRICT"):
                    btn += f"<a class=bt style='display:inline-block;background:#3b82f6;color:#fff;border-radius:6px;text-decoration:none' href='/admin/access/{escape(n)}'>Pages ({len(r.get('pages') or [])})</a> "
                if not r.get("approved"):
                    btn += f"<form method=post style='display:inline'><input type=hidden name=a value=approve><input type=hidden name=n value='{escape(n)}'><button class=bt>Approve</button></form>"
                btn += f"<form method=post style='display:inline'><input type=hidden name=a value=remove><input type=hidden name=n value='{escape(n)}'><button class=bt style='background:#c0392b'>Remove</button></form>"
            rows += f"<tr><td>{escape(n)}</td><td>{st}</td><td>{escape(r.get('created',''))}</td><td>{btn}</td></tr>"
        tp = alert_topic()
        topic_box = (f"<h2>Phone alerts</h2><p>Topic now: <b>{escape(tp) if tp else 'none set'}</b><br><small>Everyone who is logged in sees this on the Alerts page. "
                     "To cut someone off, create a new topic: nobody needs to be told, the Alerts page shows the new name.</small></p>"
                     "<form method=post style='display:inline'><input type=hidden name=a value=topic_new><button class=bt>Create a new random topic</button></form>"
                     "<form method=post><input type=hidden name=a value=topic_set><input name=topic placeholder='or type your own topic name (8-64 letters, numbers, _ -)' maxlength=64>"
                     "<button class=bt>Use this topic</button></form><hr style='border-color:#2a3140'>")
        body = f"{topic_box}<h2>Users</h2><table><tr><th>Name</th><th>Status</th><th>Since</th><th></th></tr>{rows}</table><p><a href='/'>Home</a> · <a href='/alerts'>Alerts page</a> · <a href='/logout'>Logout</a></p>"
        return _page("Users", body, wide=True)


def add_menu(app):
    """Home page "/" for the app: every page in compact sections, CSV downloads as chips beside their page.
    The admin sees everything; any other user sees only the pages the admin allocated to them at /admin/access/<user>.
    Call it at the END of app.py, after all routes exist. Skipped when the app already has its own "/" page."""
    if any(r.rule == "/" for r in app.url_map.iter_rules()):
        return
    app.config["AG_RESTRICT"] = True
    GROUPS = [
        ("Spurt scanners", ["/spurt10", "/spurt10_history", "/spurt_daily", "/spurt_sell", "/spurt_paper", "/spurtpb", "/volspurt", "/spz"]),
        ("Spurt10 sections", ["/spurt10_main", "/spurt10_ob", "/spurt10_wick", "/spurt10_watch", "/spurt10_news", "/spurt10_thin", "/depth"]),
        ("SMA / Trend", ["/sma200", "/sma200now", "/trendnow"]),
        ("Best setups", ["/best", "/bestnow"]),
        ("Scanner / Swing / Gainers", ["/scanner", "/swing", "/gainers"]),
        ("Commodity / Crypto", ["/commodity", "/mcx_sell", "/crypto", "/crypto_sell"]),
        ("Bot / Deals", ["/botlog", "/botpnl", "/deals"]),
            ]
    SKIP = ("/", "/health", "/login", "/logout", "/register", "/setup", "/alerts", "/indices.json")

    def catalog():
        pages, csvs = [], []
        for r in sorted(app.url_map.iter_rules(), key=lambda x: x.rule):
            if "GET" not in r.methods or r.arguments or r.rule in SKIP or r.rule.startswith(("/admin", "/static")):
                continue
            (csvs if r.rule.endswith(".csv") else pages).append(r.rule)
        owner = {}
        for c in csvs:
            base, best = c[:-4], ""
            for p in pages:
                if (base == p or base.startswith(p + "/") or base.startswith(p + "_")) and len(p) > len(best):
                    best = p
            owner[c] = best
        used, groups = set(), []
        for title, plist in GROUPS:
            ps = [p for p in plist if p in pages and p not in used]
            used.update(ps)
            if ps:
                groups.append((title, ps))
        rest = [p for p in pages if p not in used]
        if rest:
            groups.append(("Other", rest))
        orphan = [c for c in csvs if not owner.get(c)]
        return groups, csvs, owner, orphan

    @app.route("/")
    def _menu():
        groups, csvs, owner, orphan = catalog()
        admin = _admin()
        allowed = None if admin else set((_load().get(session.get("user", "")) or {}).get("pages") or [])
        ok = (lambda x: True) if admin else (lambda x: x in allowed)

        def chips(p):
            out = ""
            for c in csvs:
                if owner.get(c) == p and ok(c):
                    lab = c[len(p):-4].lstrip("/_") or "csv"
                    out += f"<a class=ch href='{escape(c)}' title='{escape(c)}'>{escape(lab)}{'' if lab == 'csv' else ' csv'}</a>"
            return out

        cards = ""
        for title, plist in groups:
            rows = "".join(f"<div class=r><a class=pg href='{escape(p)}'>{escape(p[1:])}</a>{chips(p)}</div>" for p in plist if ok(p))
            if rows:
                cards += f"<section><h3>{escape(title)}</h3>{rows}</section>"
        oc = [c for c in orphan if ok(c)]
        if oc:
            cards += "<section><h3>OI zones / MCX / ORB / news CSVs</h3><div class=r>" + "".join(f"<a class=ch href='{escape(c)}'>{escape(c[1:-4])}</a>" for c in oc) + "</div></section>"
        _recent = [r for r in _feed_read(40) if "read before you trade" not in r.get("title", "")][:3]
        cards = ("<section style='grid-column:1/-1'><h3>Latest alerts <a href='/alerts' style='float:right;text-transform:none;font-weight:400'>see all &rarr;</a></h3>"
                 + _feed_html(_recent) + "</section>") + cards
        if not cards.count("<h3>") > 1:
            cards = cards + "<section><h3>Welcome</h3><div class=r>No pages have been allocated to your account yet. Ask the admin.</div></section>"
        who = escape(str(session.get("user", "")))
        top = ("<a href='/alerts'>Alerts</a>" + (" &middot; <a href='/admin/users'>Manage users</a>" if admin else "") + f" &middot; <a href='/logout'>Logout ({who})</a>")
        css = (_ART_CSS + "body{margin:0;background:" + _BG + ";background-attachment:fixed;min-height:100vh;color:#e6e9ef;font-family:system-ui,Arial,sans-serif}.t{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;border-bottom:1px solid #2f3a50;background:rgba(11,15,22,.75);backdrop-filter:blur(6px);font-size:14px;gap:10px;flex-wrap:wrap}"
               ".g{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:10px;padding:12px 14px}section{background:rgba(24,29,39,.82);border:1px solid #2f3a50;border-radius:12px;padding:10px 12px;box-shadow:0 6px 20px rgba(0,0,0,.35);transition:transform .15s,border-color .15s}section:hover{transform:translateY(-2px);border-color:#4a6fb0}"
               "h3{margin:0 0 6px;font-size:13px;color:#8fb4ff;text-transform:uppercase;letter-spacing:.5px}.r{display:flex;flex-wrap:wrap;align-items:center;gap:4px;padding:2px 0;font-size:13px}"
               ".pg{background:linear-gradient(135deg,#2f6fdc,#4a86f0);box-shadow:0 2px 8px rgba(47,111,220,.4);color:#fff;font-weight:600;padding:2px 9px;border-radius:6px;margin-right:4px}.pg:hover{background:#4a86f0}a{color:#7db3ff;text-decoration:none}"
               ".ch{font-size:11px;background:#1d232e;border:1px dashed #3a4558;border-radius:10px;padding:0 6px;color:#8d9bb0}.ch:before{content:\"\\2193 \"}body{padding-bottom:50px}" + _FOOT_CSS)
        note = "(blue = open the scanner, grey \u2193 = download CSV)"
        html = (f"<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><meta name=ag-own content=1><title>Scanner</title><style>{css}</style>"
                f"<div class=t><span><b style='font-size:16px;letter-spacing:.3px'>&#9889; Scanner pages</b> <span style='color:#8d9bb0;font-size:12px'>{note}</span></span>{_IDX_BOX}<span>{top}</span></div><div class=g>{cards}</div>{_FOOT}{_IDX_JS}")
        return Response(html, mimetype="text/html")

    @app.route("/admin/access/<name>", methods=["GET", "POST"])
    def _access(name):
        groups, csvs, owner, orphan = catalog()
        with _LOCK:
            users = _load()
            rec = users.get(name)
            if not rec or rec.get("role") == "admin":
                return redirect("/admin/users")
            if request.method == "POST":
                sel = set(request.form.getlist("p"))
                pages = set()
                for _, plist in groups:
                    for p in plist:
                        if p in sel:
                            pages.add(p)
                            pages.update(c for c in csvs if owner.get(c) == p)
                pages.update(c for c in orphan if c in sel)
                rec["pages"] = sorted(pages)
                _save(users)
                return redirect("/admin/users")
        have = set(rec.get("pages") or [])
        boxes = ""
        for title, plist in groups:
            boxes += f"<h3 style='margin:12px 0 4px;font-size:14px;color:#8fb4ff'>{escape(title)}</h3>"
            for p in plist:
                boxes += f"<label style='display:block;padding:2px 0'><input type=checkbox name=p value='{escape(p)}' {'checked' if p in have else ''} style='width:auto;margin:0 8px 0 0'>{escape(p[1:])}</label>"
        if orphan:
            boxes += "<h3 style='margin:12px 0 4px;font-size:14px;color:#8fb4ff'>OI zones / MCX / ORB / news CSVs</h3>"
            for c in orphan:
                boxes += f"<label style='display:block;padding:2px 0'><input type=checkbox name=p value='{escape(c)}' {'checked' if c in have else ''} style='width:auto;margin:0 8px 0 0'>{escape(c[1:])}</label>"
        js = "<script>function all(v){document.querySelectorAll('input[type=checkbox]').forEach(x=>x.checked=v)}</script>"
        body = (f"<h2>Pages for {escape(name)}</h2><p><small>Tick what this person may open. A page's CSV downloads come with it.</small></p>{js}"
                f"<p><button type=button class=bt onclick='all(true)'>Select all</button><button type=button class=bt onclick='all(false)'>Clear</button></p>"
                f"<form method=post>{boxes}<p><button>Save</button></p></form><p><a href='/admin/users'>Back to users</a></p>")
        return _page("Pages for " + name, body, wide=True)


def add_alerts_page(app):
    """/alerts: the latest scanner alerts as a web page (no phone app needed) + how to get them on a phone."""
    @app.route("/alerts")
    def _alerts():
        tp = alert_topic()
        feed = _feed_html(_feed_read(100))
        if not tp:
            ph = "<p>No phone alert topic has been set yet. Ask the admin.</p>"
        else:
            ph = ("<ol><li>Install the free <b>ntfy</b> app (Android / iPhone).</li><li>Tap <b>+</b> (Subscribe to topic).</li>"
                  f"<li>Enter this topic name exactly:<br><b style='font-size:18px'>{escape(tp)}</b></li></ol>"
                  "<p><small>Do not share this name with people who have no login. If it stops working, come back here: the admin may have changed it.</small></p>")
        body = ("<meta http-equiv=refresh content=60><h2>Latest alerts</h2><p><small>Same alerts as the phone. This page refreshes every minute. "
                "Alerts are information only, not advice to buy or sell.</small></p>" + feed +
                "<details style='margin-top:14px'><summary style='cursor:pointer;color:#7db3ff'>Also get them on your phone (optional)</summary>" + ph + "</details>"
                "<p><a href='/'>Home</a></p>")
        return _page("Alerts", body, wide=True)


def add_theme(app):
    """Gives every existing page of the app the same look: a slim top bar (Home / Phone alerts / Logout) and the faint candlestick
    background with the bull and the bear behind the content. The pages' own tables, colours and numbers are not touched.
    Call it after install(). Works on any HTML page the app serves."""
    import re
    bar = ("<div id=ag-bar style=\"position:sticky;top:0;z-index:99999;display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;"
           "padding:7px 14px;background:rgba(11,15,22,.92);border-bottom:1px solid #2f3a50;font:13px system-ui,Arial,sans-serif;color:#e6e9ef\">"
           "<span><a href='/' style='color:#fff;font-weight:700;text-decoration:none'>&#9889; Home</a></span>" + _IDX_BOX +
           "<span>__LINKS__</span></div>")
    css = ("<style id=ag-theme>html{--agov:rgba(8,12,20,.80)}html::before{content:'';position:fixed;inset:0;z-index:-1;pointer-events:none;"
           "background:linear-gradient(var(--agov),var(--agov)),url(/bg/page.jpg) center/cover no-repeat}" + _FOOT_CSS + "body{padding-bottom:56px!important}</style>")
    js = ("<script id=ag-js>(function(){try{var b=document.body,c=getComputedStyle(b).backgroundColor,m=c.match(/[\\d.]+/g)||[],a=m.length>3?+m[3]:1;"
          "var dark=a>0&&m.length>=3&&(0.299*m[0]+0.587*m[1]+0.114*m[2])<110;var s=document.createElement('style');"
          "s.textContent='html{background:'+(dark?'#0b0f16':'#e8edf5')+'!important}body{background:transparent!important}';if(!dark)document.documentElement.style.setProperty('--agov','rgba(236,241,248,.90)');"
          "document.head.appendChild(s)}catch(e){}})();</script>")

    @app.after_request
    def _theme(resp):
        try:
            if resp.status_code != 200 or resp.mimetype != "text/html" or resp.direct_passthrough:
                return resp
            html = resp.get_data(as_text=True)
            if "ag-theme" in html or "name=ag-own" in html or "<body" not in html.lower() and "<table" not in html.lower():
                return resp
            links = "<a href='/alerts' style='color:#7db3ff;text-decoration:none'>Alerts</a>"
            if _admin():
                links += " &middot; <a href='/admin/users' style='color:#7db3ff;text-decoration:none'>Users</a>"
            links += f" &middot; <a href='/logout' style='color:#7db3ff;text-decoration:none'>Logout ({escape(str(session.get('user', '')))})</a>"
            tb = bar.replace("__LINKS__", links)
            if re.search(r"<body[^>]*>", html, re.I):
                html = re.sub(r"(<body[^>]*>)", lambda m_: m_.group(1) + tb, html, count=1, flags=re.I)
            else:
                html = tb + html
            html = (html + css + _FOOT + js + _IDX_JS) if "</body>" not in html.lower() else re.sub(r"</body>", lambda m_: css + _FOOT + js + _IDX_JS + "</body>", html, count=1, flags=re.I)
            resp.set_data(html)
        except Exception:
            pass
        return resp


def add_backgrounds(app):
    """Serves the two background pictures from the 'bg' folder next to this file (cached by the browser; open to everybody so the login page can use them)."""
    from flask import send_from_directory
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bg")

    @app.route("/bg/<name>")
    def _bg(name):
        if name not in ("home.jpg", "page.jpg"):
            return Response("Not found", status=404)
        r = send_from_directory(folder, name, mimetype="image/jpeg")
        r.headers["Cache-Control"] = "public, max-age=86400"
        return r
