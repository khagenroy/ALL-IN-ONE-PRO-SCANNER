"""PAPER-TRADE inside the stock scanner: practice trading with virtual money at live Dhan prices. Places no real orders.
Pages live under /paper (give a student the page "/paper" in Users and they get everything below it). Call install(app) before add_menu(app)."""
import os
import csv
import io
import logging
import threading

from flask import Response, request, session, jsonify, redirect

log = logging.getLogger("paper")


def install(app):
    import paper_engine as pe
    import paper_pages as pages

    def user():
        return session.get("user", "")

    def html(s):
        return Response(s, mimetype="text/html")

    def body():
        return request.get_json(silent=True) or {}

    def add(rule, fn, methods=("GET",), name=None):
        app.add_url_rule(rule, name or ("paper_" + fn.__name__), fn, methods=list(methods))

    def home():
        return html(pages.trade_page(user()))

    def history():
        return html(pages.history_page(user(), pe.stats(pe.acct(user())["trades"])))

    def history_csv():
        out = io.StringIO()
        w = csv.writer(out)
        cols = ["closed", "symbol", "side", "product", "qty", "entry", "exit", "reason", "gross", "cost", "net", "opened"]
        w.writerow(cols)
        for t in pe.acct(user())["trades"]:
            w.writerow([t.get(c, "INTRADAY" if c == "product" else None) for c in cols])
        return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=my_paper_trades.csv"})

    def leaderboard():
        return html(pages.leaderboard_page(user(), pe.leaderboard()))

    def help_():
        return html(pages.help_page())

    def api_state():
        return jsonify(pe.state(user()))

    def api_symbols():
        return jsonify(pe.sym_search(request.args.get("q", "")))

    def api_quote():
        p, err = pe.quote(request.args.get("symbol", ""))
        return jsonify({"ok": p is not None, "price": p, "msg": err})

    def api_chartinfo():
        d, err = pe.chart_info(request.args.get("symbol", ""), request.args.get("tf", "15"),
                               request.args.get("zz"), request.args.get("nob"), request.args.get("zatr"))
        return jsonify({"ok": d is not None, "info": d, "msg": err})

    def api_candles():
        bars, err = pe.candles_json(request.args.get("symbol", ""), request.args.get("tf", "5"))
        return jsonify({"ok": bars is not None, "bars": bars or [], "msg": err})

    def _js(name):
        def view():
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), name), "rb") as f:
                r = Response(f.read(), mimetype="application/javascript")
            r.headers["Cache-Control"] = "private, max-age=300"
            return r
        return view

    def api_order():
        d = body()
        ok, msg = pe.place_order(user(), d.get("symbol"), d.get("side"), d.get("qty"), d.get("sl"), d.get("t1"), d.get("t2"), d.get("limit"),
                                 d.get("product"))
        return jsonify({"ok": ok, "msg": msg})

    def api_modify():
        d = body()
        ok, msg = pe.modify_position(user(), str(d.get("id", "")), d.get("sl"), d.get("t1"), d.get("t2"))
        return jsonify({"ok": ok, "msg": msg})

    def api_modify_order():
        d = body()
        ok, msg = pe.modify_order(user(), str(d.get("id", "")), d.get("limit"), d.get("sl"), d.get("t1"), d.get("t2"))
        return jsonify({"ok": ok, "msg": msg})

    def api_close():
        ok, msg = pe.close_position(user(), str(body().get("id", "")))
        return jsonify({"ok": ok, "msg": msg})

    def api_closeall():
        ok, msg = pe.close_all(user())
        return jsonify({"ok": ok, "msg": msg})

    def api_cancel():
        ok, msg = pe.cancel_order(user(), str(body().get("id", "")))
        return jsonify({"ok": ok, "msg": msg})

    def api_reset():
        ok, msg = pe.reset_account(user())
        return jsonify({"ok": ok, "msg": msg})

    def admin_paper():
        return html(pages.admin_page(pe.leaderboard()))

    def admin_paper_reset(u):
        if u in pe._db["users"]:
            pe.reset_account(u, force=True)
        return redirect("/admin/paper")

    add("/paper", home)
    add("/paper/history", history)
    add("/paper/history.csv", history_csv)
    add("/paper/leaderboard", leaderboard)
    add("/paper/help", help_)
    add("/paper/api/state", api_state)
    add("/paper/api/symbols", api_symbols)
    add("/paper/api/quote", api_quote)
    add("/paper/api/chartinfo", api_chartinfo)
    add("/paper/api/candles", api_candles)
    add("/paper/api/lw.js", _js("paper_lw.js"), name="paper_lw_js")
    add("/paper/api/chart.js", _js("paper_chart.js"), name="paper_chart_js")
    for r, f in (("/paper/api/order", api_order), ("/paper/api/modify", api_modify), ("/paper/api/modify_order", api_modify_order), ("/paper/api/close", api_close), ("/paper/api/closeall", api_closeall),
                 ("/paper/api/cancel", api_cancel), ("/paper/api/reset", api_reset)):
        add(r, f, ("POST",))
    add("/admin/paper", admin_paper)
    add("/admin/paper/reset/<u>", admin_paper_reset, ("POST",))
    if not os.environ.get("PAPER_NO_THREAD"):
        threading.Thread(target=pe.loop, daemon=True, name="paper-engine").start()
    log.info("Practice trading (paper) installed at /paper")
