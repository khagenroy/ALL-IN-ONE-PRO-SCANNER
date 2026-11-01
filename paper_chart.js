/* Practice-trading chart (own chart on Dhan candles, drawn with the open-source Lightweight Charts library). */
(function () {
  var LW = window.LightweightCharts, el = document.getElementById('pchart');
  if (!LW || !el) { if (el) el.innerHTML = '<p style="padding:14px;color:#9aa8bf">The chart library could not load.</p>'; return; }
  var OFF = 19800;                                  // show IST on the time axis
  var IND = { vol: 1, vwap: 1, ema20: 1, zones: 1, spurt: 1, ema9: 0, ema50: 0, sma200: 0, bb: 0, st: 0, rsi: 0, macd: 0, lines: 1 };
  try { var sv = JSON.parse(localStorage.getItem('pchart_ind') || 'null'); if (sv) for (var k in sv) IND[k] = sv[k]; } catch (e) {}
  var TF = '5', sym = '', bars = [], extra = [], plines = [], info = null, S = { levels: {} };
  var chart = LW.createChart(el, {
    layout: { background: { color: '#0f1520' }, textColor: '#c8d1e0', panes: { separatorColor: '#2a344a' } },
    grid: { vertLines: { color: '#182033' }, horzLines: { color: '#182033' } },
    timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#2a344a' }, rightPriceScale: { borderColor: '#2a344a' },
    crosshair: { mode: 0 }, autoSize: true
  });
  var candle = chart.addSeries(LW.CandlestickSeries, { upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });
  var markers = LW.createSeriesMarkers(candle, []);
  var leg = document.getElementById('plegend');
  /* timeframes: '5' '15' '30' '45' '60' '120' '180' '240' (minutes) or 'D' 'W' 'M' */
  function htf() { return TF === 'D' || TF === 'W' || TF === 'M'; }
  function tfMin() { return TF === 'D' ? 1440 : TF === 'W' ? 10080 : TF === 'M' ? 44640 : +TF; }
  function tfLabel() { if (htf()) return TF; var m = +TF; return m >= 60 ? (m / 60) + 'h' : m + 'm'; }

  function fmt(v) { return v == null ? '-' : Number(v).toLocaleString('en-IN', { maximumFractionDigits: 2, minimumFractionDigits: 2 }); }
  chart.subscribeCrosshairMove(function (p) {
    var d = p && p.seriesData ? p.seriesData.get(candle) : null;
    if (!d && bars.length) { var b = bars[bars.length - 1]; d = { open: b.o, high: b.h, low: b.l, close: b.c }; }
    if (!d) return;
    var up = d.close >= d.open;
    leg.innerHTML = '<b>' + sym + '</b> ' + tfLabel() + ' &nbsp;O ' + fmt(d.open) + ' H ' + fmt(d.high) + ' L ' + fmt(d.low) + ' C <b style="color:' + (up ? '#26a69a' : '#ef5350') + '">' + fmt(d.close) + '</b>';
  });


  /* ---------- zones drawn as shaded boxes, like the indicator ---------- */
  function ZonePrim(zs) { this.zs = zs; }
  ZonePrim.prototype.attached = function (p) { this.chart = p.chart; this.series = p.series; this.req = p.requestUpdate; };
  ZonePrim.prototype.detached = function () { this.chart = null; this.series = null; };
  ZonePrim.prototype.updateAllViews = function () {};
  ZonePrim.prototype.paneViews = function () {
    var self = this;
    return [{ zOrder: function () { return 'bottom'; }, renderer: function () { return { draw: function (target) {
      target.useBitmapCoordinateSpace(function (sc) {
        if (!self.chart || !self.series) return;
        var ctx = sc.context, hr = sc.horizontalPixelRatio, vr = sc.verticalPixelRatio, W = sc.bitmapSize.width;
        self.zs.forEach(function (z) {
          var y1 = self.series.priceToCoordinate(z.hi), y2 = self.series.priceToCoordinate(z.lo);
          if (y1 == null || y2 == null) return;
          var x = null; if (z.tt != null) x = self.chart.timeScale().timeToCoordinate(z.tt);
          var x0 = x == null ? 0 : Math.max(0, Math.round(x * hr));
          var sup = z.type === 'SUPPORT';
          ctx.fillStyle = sup ? 'rgba(38,166,154,0.20)' : 'rgba(239,83,80,0.20)';
          ctx.fillRect(x0, Math.round(y1 * vr), W - x0, Math.max(2, Math.round((y2 - y1) * vr)));
          ctx.strokeStyle = sup ? 'rgba(38,166,154,0.9)' : 'rgba(239,83,80,0.9)'; ctx.lineWidth = Math.max(1, Math.round(hr));
          ctx.strokeRect(x0 + 0.5, Math.round(y1 * vr) + 0.5, W - x0, Math.max(2, Math.round((y2 - y1) * vr)));
          ctx.fillStyle = sup ? '#7fe3d6' : '#ff9a97'; ctx.font = Math.round(11 * vr) + 'px system-ui,Arial';
          ctx.fillText((sup ? 'Support OB ' : 'Resistance OB ') + z.lo + ' - ' + z.hi, x0 + 6 * hr, Math.round(y1 * vr) + 13 * vr);
        });
      });
    } }; } }];
  };
  var zprim = null;
  function snap(t0) { if (t0 == null || !bars.length) return null; var tt = t0 + OFF, lo = 0, hi = bars.length - 1, r = 0;
    while (lo <= hi) { var m = (lo + hi) >> 1; if (bars[m].t + OFF <= tt) { r = m; lo = m + 1; } else hi = m - 1; } return bars[r].t + OFF; }

  /* ---------- indicator maths ---------- */
  function ema(vals, n) { var k = 2 / (n + 1), out = [], e = null; for (var i = 0; i < vals.length; i++) { e = e == null ? vals[i] : vals[i] * k + e * (1 - k); out.push(i < n - 1 ? null : e); } return out; }
  function sma(vals, n) { var out = [], s = 0; for (var i = 0; i < vals.length; i++) { s += vals[i]; if (i >= n) s -= vals[i - n]; out.push(i >= n - 1 ? s / n : null); } return out; }
  function rsi(c, n) { var out = [], g = 0, l = 0; for (var i = 0; i < c.length; i++) { if (i === 0) { out.push(null); continue; } var d = c[i] - c[i - 1], u = Math.max(d, 0), dn = Math.max(-d, 0);
      if (i <= n) { g += u; l += dn; if (i === n) { g /= n; l /= n; out.push(l === 0 ? 100 : 100 - 100 / (1 + g / l)); } else out.push(null); }
      else { g = (g * (n - 1) + u) / n; l = (l * (n - 1) + dn) / n; out.push(l === 0 ? 100 : 100 - 100 / (1 + g / l)); } } return out; }
  function atr(b, n) { var tr = [], out = []; for (var i = 0; i < b.length; i++) { tr.push(i ? Math.max(b[i].h - b[i].l, Math.abs(b[i].h - b[i - 1].c), Math.abs(b[i].l - b[i - 1].c)) : b[i].h - b[i].l); }
    var a = null; for (i = 0; i < tr.length; i++) { if (i < n - 1) out.push(null); else if (i === n - 1) { var s = 0; for (var j = 0; j < n; j++) s += tr[j]; a = s / n; out.push(a); } else { a = (a * (n - 1) + tr[i]) / n; out.push(a); } } return out; }
  function day(t) { return Math.floor((t + OFF) / 86400); }
  function T(i) { return bars[i].t + OFF; }

  function clearExtra() { if (zprim) { try { candle.detachPrimitive(zprim); } catch (e) {} zprim = null; } extra.forEach(function (s) { try { chart.removeSeries(s); } catch (e) {} }); extra = []; plines.forEach(function (l) { try { candle.removePriceLine(l); } catch (e) {} }); plines = []; }
  function line(data, color, pane, w, title) {
    var s = chart.addSeries(LW.LineSeries, { color: color, lineWidth: w || 1, priceLineVisible: false, lastValueVisible: !!pane, crosshairMarkerVisible: false, title: title || '' }, pane || 0);
    s.setData(data); extra.push(s); return s;
  }
  function ser(arr, key) { var o = []; for (var i = 0; i < bars.length; i++) if (arr[i] != null) o.push({ time: T(i), value: arr[i] }); return o; }
  function pl(price, color, title, style) { plines.push(candle.createPriceLine({ price: price, color: color, lineWidth: 1, lineStyle: style == null ? 2 : style, axisLabelVisible: true, title: title })); }

  function draw(keepRange) {
    clearExtra();
    chart.applyOptions({ timeScale: { timeVisible: !htf() } });              // dates only on D / W / M
    var cd = bars.map(function (b, i) { return { time: T(i), open: b.o, high: b.h, low: b.l, close: b.c }; });
    candle.setData(cd);
    var closes = bars.map(function (b) { return b.c; });
    if (IND.vol) {
      var vs = chart.addSeries(LW.HistogramSeries, { priceFormat: { type: 'volume' }, priceScaleId: 'vol', lastValueVisible: false, priceLineVisible: false });
      chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
      vs.setData(bars.map(function (b, i) { return { time: T(i), value: b.v, color: b.c >= b.o ? 'rgba(38,166,154,.45)' : 'rgba(239,83,80,.45)' }; })); extra.push(vs);
    }
    if (IND.ema9) line(ser(ema(closes, 9)), '#ffd54f', 0, 1, 'EMA9');
    if (IND.ema20) line(ser(ema(closes, 20)), '#42a5f5', 0, 1, 'EMA20');
    if (IND.ema50) line(ser(ema(closes, 50)), '#ab47bc', 0, 1, 'EMA50');
    if (IND.sma200) line(ser(sma(closes, 200)), '#ff7043', 0, 2, 'SMA200');
    if (IND.vwap && !htf()) { var pv = 0, vv = 0, cd0 = -1, vw = []; bars.forEach(function (b, i) { var d = day(b.t); if (d !== cd0) { cd0 = d; pv = 0; vv = 0; } pv += (b.h + b.l + b.c) / 3 * b.v; vv += b.v; vw.push(vv ? pv / vv : null); }); line(ser(vw), '#ffffff', 0, 1, 'VWAP'); }
    if (IND.bb) { var m = sma(closes, 20), up = [], lo = []; for (var i = 0; i < closes.length; i++) { if (m[i] == null) { up.push(null); lo.push(null); continue; } var s = 0; for (var j = i - 19; j <= i; j++) s += Math.pow(closes[j] - m[i], 2); var sd = Math.sqrt(s / 20); up.push(m[i] + 2 * sd); lo.push(m[i] - 2 * sd); }
      line(ser(m), '#78909c', 0, 1, ''); line(ser(up), '#78909c', 0, 1, ''); line(ser(lo), '#78909c', 0, 1, ''); }
    if (IND.st) { var a = atr(bars, 10), fu = [], fl = [], dir = 1, stU = [], stD = []; for (i = 0; i < bars.length; i++) { if (a[i] == null) { fu.push(null); fl.push(null); continue; }
        var hl = (bars[i].h + bars[i].l) / 2, bu = hl + 3 * a[i], bl = hl - 3 * a[i]; var pu = fu[i - 1], pl_ = fl[i - 1];
        fu.push(pu == null || bu < pu || bars[i - 1].c > pu ? bu : pu); fl.push(pl_ == null || bl > pl_ || bars[i - 1].c < pl_ ? bl : pl_);
        if (dir === 1 && bars[i].c < fl[i]) dir = -1; else if (dir === -1 && bars[i].c > fu[i]) dir = 1;
        stU.push(dir === 1 ? { time: T(i), value: fl[i] } : { time: T(i) }); stD.push(dir === -1 ? { time: T(i), value: fu[i] } : { time: T(i) }); }
      var s1 = chart.addSeries(LW.LineSeries, { color: '#26a69a', lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }); s1.setData(stU); extra.push(s1);
      var s2 = chart.addSeries(LW.LineSeries, { color: '#ef5350', lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }); s2.setData(stD); extra.push(s2); }
    var pane = 1;
    if (IND.rsi) { var r = line(ser(rsi(closes, 14)), '#ba68c8', pane, 1, 'RSI14'); r.createPriceLine({ price: 70, color: '#ef5350', lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: '' }); r.createPriceLine({ price: 30, color: '#26a69a', lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: '' }); try { chart.panes()[pane].setHeight(110); } catch (e) {} pane++; }
    if (IND.macd) { var e12 = ema(closes, 12), e26 = ema(closes, 26), mc = [], k = 0; for (i = 0; i < closes.length; i++) mc.push(e12[i] != null && e26[i] != null ? e12[i] - e26[i] : null);
      var base = mc.map(function (v) { return v == null ? 0 : v; }), sg = ema(base, 9); for (i = 0; i < sg.length; i++) if (mc[i] == null || i < 33) sg[i] = null;
      var hs = chart.addSeries(LW.HistogramSeries, { priceLineVisible: false, lastValueVisible: false }, pane);
      hs.setData(bars.map(function (b, i) { return mc[i] != null && sg[i] != null ? { time: T(i), value: mc[i] - sg[i], color: mc[i] - sg[i] >= 0 ? 'rgba(38,166,154,.6)' : 'rgba(239,83,80,.6)' } : { time: T(i) }; })); extra.push(hs);
      line(ser(mc.map(function (v, i) { return sg[i] != null ? v : null; })), '#42a5f5', pane, 1, 'MACD'); line(ser(sg), '#ffb74d', pane, 1, 'Signal');
      try { chart.panes()[pane].setHeight(110); } catch (e) {} pane++; }
    // spurt markers (volume of the candle / average of the 50 candles before it)
    var mk = [];
    if (IND.spurt) for (i = 50; i < bars.length; i++) { var s = 0; for (j = i - 50; j < i; j++) s += bars[j].v; var base2 = s / 50; if (base2 > 0 && bars[i].v / base2 >= 5) mk.push({ time: T(i), position: bars[i].c >= bars[i].o ? 'belowBar' : 'aboveBar', color: '#ffd54f', shape: 'circle', text: (bars[i].v / base2).toFixed(0) + 'x' }); }
    markers.setMarkers(mk);
    // zones from the scanner rules
    if (IND.zones && info && info.zones && info.zones.length) { zprim = new ZonePrim(info.zones.map(function (z) { return { type: z.type, lo: z.lo, hi: z.hi, tt: snap(z.t0) }; })); candle.attachPrimitive(zprim); }
    // the trade being set up / open trade
    if (IND.lines) {
      var L = S.levels; if (L.entry) pl(L.entry, '#fdd835', 'ENTRY', 0); if (L.sl) pl(L.sl, '#ff5252', 'SL', 0); if (L.t1) pl(L.t1, '#69f0ae', 'T1', 0); if (L.t2) pl(L.t2, '#00e676', 'T2', 0);
    }
    if (!keepRange) chart.timeScale().fitContent();
    if (!keepRange && bars.length > 120) chart.timeScale().setVisibleLogicalRange({ from: bars.length - 110, to: bars.length + 6 });
  }

  /* ---------- live price may only touch the candle that is forming right now ----------
     FIX 2026-11-01: the live price (Dhan LTP, polled every 8 s by the order form) used to be pasted onto the last candle
     at any time - also on weekends, holidays and after 15:30, when the last candle is an old one and the LTP is the
     official closing price. That stretched the old candle (e.g. WIPRO 197.31 <-> 198.31) and it jumped back on every
     20-second reload. Now the live price is used only on a weekday between 09:15 and 15:30 IST, and only when the
     current time falls inside the last candle's own time window. Otherwise the chart shows Dhan's candles as they are. */
  function liveCandleOpen(b) {
    var now = Math.floor(Date.now() / 1000), ist = new Date((now + OFF) * 1000);
    var wd = ist.getUTCDay(), m = ist.getUTCHours() * 60 + ist.getUTCMinutes();
    if (wd === 0 || wd === 6 || m < 9 * 60 + 15 || m >= 15 * 60 + 30) return false;   // market closed
    return now >= b.t && now < b.t + tfMin() * 60;                                           // inside this candle's window
  }

  /* ---------- data ---------- */
  var busy = false;
  function load(keep) {
    if (!sym || busy) return; busy = true;
    fetch('/paper/api/candles?symbol=' + encodeURIComponent(sym) + '&tf=' + TF).then(function (r) { return r.json(); }).then(function (j) {
      busy = false; var st = document.getElementById('pcmsg');
      if (!j.ok) { st.textContent = j.msg; return; } st.textContent = '';
      bars = j.bars; draw(keep);
      var b = bars[bars.length - 1]; if (b) leg.innerHTML = '<b>' + sym + '</b> ' + tfLabel() + ' &nbsp;O ' + fmt(b.o) + ' H ' + fmt(b.h) + ' L ' + fmt(b.l) + ' C ' + fmt(b.c);
    }).catch(function () { busy = false; });
  }
  window.PChart = {
    setSymbol: function (s) { sym = s; info = null; bars = []; document.getElementById('pctitle').textContent = s; var a = document.getElementById('ptvlink'); if (a) a.href = 'https://in.tradingview.com/chart/?symbol=NSE%3A' + encodeURIComponent(s.replace(/-/g, '_')); load(false); },
    setInfo: function (i) { info = i; if (bars.length) draw(true); },
    setLevels: function (l) { S.levels = l || {}; if (bars.length) { var keep = true; draw(keep); } },
    tick: function (price) {
      if (!bars.length || !price) return;
      var b = bars[bars.length - 1];
      if (!liveCandleOpen(b)) return;                                                  // market closed or old candle: leave it alone
      b.c = price; b.h = Math.max(b.h, price); b.l = Math.min(b.l, price);
      candle.update({ time: T(bars.length - 1), open: b.o, high: b.h, low: b.l, close: price });
    },
    refresh: function () { load(true); }
  };
  /* ---------- controls ---------- */
  document.querySelectorAll('#ptf button').forEach(function (b) { b.addEventListener('click', function () { TF = b.dataset.tf; document.querySelectorAll('#ptf button').forEach(function (x) { x.classList.toggle('on', x === b); }); info = null; load(false); if (window.onPTf) window.onPTf(TF); }); });
  var menu = document.getElementById('pind');
  menu.querySelectorAll('input').forEach(function (c) { c.checked = !!IND[c.dataset.k]; c.addEventListener('change', function () { IND[c.dataset.k] = c.checked ? 1 : 0; try { localStorage.setItem('pchart_ind', JSON.stringify(IND)); } catch (e) {} if (bars.length) draw(true); }); });
  document.getElementById('pindbtn').addEventListener('click', function () { menu.style.display = menu.style.display === 'block' ? 'none' : 'block'; });
  var card = document.getElementById('pcard');
  document.getElementById('pbig').addEventListener('click', function () { card.classList.toggle('big'); document.body.style.overflow = card.classList.contains('big') ? 'hidden' : ''; this.textContent = card.classList.contains('big') ? 'Close ✕' : 'Enlarge ⛶'; setTimeout(function () { chart.timeScale().fitContent(); }, 80); });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && card.classList.contains('big')) document.getElementById('pbig').click(); });
  setInterval(function () { if (sym) load(true); }, 20000);
  if (window.onPChartReady) { try { window.onPChartReady(); } catch (e) {} }   // draw the stock the page already picked
})();
