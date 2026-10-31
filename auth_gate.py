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
_OPEN = ("/health", "/login", "/register", "/setup", "/logout")
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


_CSS = ("body{font-family:system-ui,Arial,sans-serif;background:#0f1218;color:#e6e9ef;display:flex;justify-content:center;padding:40px 16px}"
        ".b{background:#181d27;border:1px solid #2a3140;border-radius:10px;padding:24px;width:100%;max-width:360px}"
        "h2{margin:0 0 14px}input{width:100%;box-sizing:border-box;padding:10px;margin:6px 0 12px;border-radius:6px;border:1px solid #2f3748;background:#0f1218;color:#e6e9ef}"
        "button{width:100%;padding:10px;border:0;border-radius:6px;background:#3b82f6;color:#fff;font-size:15px;cursor:pointer}"
        "a{color:#7db3ff}.e{color:#ff8a8a;margin-bottom:10px}.m{color:#8fd19e;margin-bottom:10px}small{color:#8b93a5}"
        "table{width:100%;border-collapse:collapse}td,th{padding:6px;border-bottom:1px solid #2a3140;text-align:left;font-size:14px}"
        ".bt{width:auto;padding:4px 10px;font-size:13px;margin-right:4px}")


def _page(title, body, status=200, wide=False):
    st = _CSS + (".b{max-width:640px}" if wide else "")
    html = f"<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>{escape(title)}</title><style>{st}</style><div class=b>{body}</div>"
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
                if not r.get("approved"):
                    btn += f"<form method=post style='display:inline'><input type=hidden name=a value=approve><input type=hidden name=n value='{escape(n)}'><button class=bt>Approve</button></form>"
                btn += f"<form method=post style='display:inline'><input type=hidden name=a value=remove><input type=hidden name=n value='{escape(n)}'><button class=bt style='background:#c0392b'>Remove</button></form>"
            rows += f"<tr><td>{escape(n)}</td><td>{st}</td><td>{escape(r.get('created',''))}</td><td>{btn}</td></tr>"
        body = f"<h2>Users</h2><table><tr><th>Name</th><th>Status</th><th>Since</th><th></th></tr>{rows}</table><p><a href='/'>Home</a> · <a href='/logout'>Logout</a></p>"
        return _page("Users", body, wide=True)
