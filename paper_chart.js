/* Practice-trading chart (own chart on Dhan candles, drawn with the open-source Lightweight Charts library). */
(function () {
  var LW = window.LightweightCharts, el = document.getElementById('pchart');
  if (!LW || !el) { if (el) el.innerHTML = '<p style="padding:14px;color:#9aa8bf">The chart library could not load.</p>'; return; }
  var OFF = 19800;                                  // show IST on the time axis
  var IND = { vol: 1, vwap: 1, ema20: 1, zones: 1, spurt: 1, ema9: 0, ema50: 0, sma200: 0, bb: 0, st: 0, rsi: 0, macd: 0, lines: 1 };
  try { var sv = JSON.parse(localStorage.getItem('pchart_ind') || 'null'); if (sv) for (var k in sv) IND[k] = sv[k]; } catch (e) {}
  /* ---------- indicator settings (like TradingView): every indicator has its own settings, saved on this device ---------- */
  var DEF = {
    vol:    { up: '#26a69a', dn: '#ef5350' },
    vwap:   { color: '#ffffff', w: 1 },
    ema9:   { len: 9,   color: '#ffd54f', w: 1 },
    ema20:  { len: 20,  color: '#42a5f5', w: 1 },
    ema50:  { len: 50,  color: '#ab47bc', w: 1 },
    sma200: { len: 200, color: '#ff7043', w: 2 },
    bb:     { len: 20,  mult: 2, color: '#78909c', w: 1 },
    st:     { len: 10,  mult: 3, up: '#26a69a', dn: '#ef5350', w: 2 },
    rsi:    { len: 14,  ob: 70, os: 30, color: '#ba68c8', w: 1 },
    macd:   { fast: 12, slow: 26, sig: 9, mcol: '#42a5f5', scol: '#ffb74d' },
    spurt:  { mult: 5,  len: 50, color: '#ffd54f' },
    zones:  { zz: 9, nob: 2, atr: 14, sup: '#26a69a', res: '#ef5350', op: 20, lab: 1 },
    lines:  {}
  };
  /* rows of the Indicators menu: key, name, settings fields [field, label, type, min, max, step] */
  var ROWS = [
    ['vol', 'Volume', [['up', 'Up colour', 'color'], ['dn', 'Down colour', 'color']]],
    ['vwap', 'VWAP', [['color', 'Colour', 'color'], ['w', 'Line width', 'num', 1, 4, 1]]],
    ['ema9', 'EMA', [['len', 'Length', 'num', 1, 500, 1], ['color', 'Colour', 'color'], ['w', 'Line width', 'num', 1, 4, 1]]],
    ['ema20', 'EMA', [['len', 'Length', 'num', 1, 500, 1], ['color', 'Colour', 'color'], ['w', 'Line width', 'num', 1, 4, 1]]],
    ['ema50', 'EMA', [['len', 'Length', 'num', 1, 500, 1], ['color', 'Colour', 'color'], ['w', 'Line width', 'num', 1, 4, 1]]],
    ['sma200', 'SMA', [['len', 'Length', 'num', 1, 500, 1], ['color', 'Colour', 'color'], ['w', 'Line width', 'num', 1, 4, 1]]],
    ['bb', 'Bollinger Bands', [['len', 'Length', 'num', 2, 200, 1], ['mult', 'Std dev', 'num', 0.5, 5, 0.1], ['color', 'Colour', 'color']]],
    ['st', 'Supertrend', [['len', 'ATR length', 'num', 1, 100, 1], ['mult', 'Factor', 'num', 0.5, 10, 0.1], ['up', 'Up colour', 'color'], ['dn', 'Down colour', 'color']]],
    ['rsi', 'RSI', [['len', 'Length', 'num', 2, 100, 1], ['ob', 'Overbought', 'num', 50, 100, 1], ['os', 'Oversold', 'num', 0, 50, 1], ['color', 'Colour', 'color']]],
    ['macd', 'MACD', [['fast', 'Fast length', 'num', 1, 100, 1], ['slow', 'Slow length', 'num', 2, 200, 1], ['sig', 'Signal length', 'num', 1, 50, 1], ['mcol', 'MACD colour', 'color'], ['scol', 'Signal colour', 'color']]],
    ['spurt', 'Volume spurt marks', [['mult', 'Min. spurt (x average)', 'num', 1, 100, 0.5], ['len', 'Average of last N candles', 'num', 5, 200, 1], ['color', 'Colour', 'color']]],
    ['zones', 'Support / resistance zones', [['zz', 'Swing length', 'num', 3, 50, 1], ['nob', 'Zones per side', 'num', 1, 5, 1], ['atr', 'Zone height (ATR length)', 'num', 2, 100, 1],
      ['sup', 'Support colour', 'color'], ['res', 'Resistance colour', 'color'], ['op', 'Fill opacity %', 'num', 5, 80, 1], ['lab', 'Show labels', 'bool']]],
    ['lines', 'Entry / SL / target lines', []]
  ];
  var CFG = JSON.parse(JSON.stringify(DEF));
  try { var sc = JSON.parse(localStorage.getItem('pchart_cfg') || 'null'); if (sc) for (var k2 in sc) if (CFG[k2]) for (var f in sc[k2]) CFG[k2][f] = sc[k2][f]; } catch (e) {}
  function saveCfg() { try { localStorage.setItem('pchart_cfg', JSON.stringify(CFG)); } catch (e) {} }
  function rowLabel(r) {
    var c = CFG[r[0]];
    switch (r[0]) {
      case 'ema9': case 'ema20': case 'ema50': case 'sma200': return r[1] + ' ' + c.len;
      case 'bb': return 'Bollinger Bands (' + c.len + ', ' + c.mult + ')';
      case 'st': return 'Supertrend (' + c.len + ', ' + c.mult + ')';
      case 'rsi': return 'RSI ' + c.len;
      case 'macd': return 'MACD (' + c.fast + ', ' + c.slow + ', ' + c.sig + ')';
      case 'spurt': return 'Volume spurt marks (' + c.mult + 'x+)';
      case 'zones': return 'Support / resistance zones (' + c.zz + ')';
      default: return r[1];
    }
  }

  var TF = '5', sym = '', bars = [], extra = [], plines = [], info = null, S = { levels: {} };
  var chart = LW.createChart(el, {
    layout: { background: { color: '#0f1520' }, textColor: '#c8d1e0', panes: { separatorColor: '#2a344a' } },
    grid: { vertLines: { color: '#182033' }, horzLines: { color: '#182033' } },
    timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#2a344a', rightOffset: 8 }, rightPriceScale: { borderColor: '#2a344a' },
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
          var sup = z.type === 'SUPPORT', zc = CFG.zones, col = sup ? zc.sup : zc.res;
          ctx.fillStyle = rgba(col, (+zc.op) / 100);
          ctx.fillRect(x0, Math.round(y1 * vr), W - x0, Math.max(2, Math.round((y2 - y1) * vr)));
          ctx.strokeStyle = rgba(col, 0.9); ctx.lineWidth = Math.max(1, Math.round(hr));
          ctx.strokeRect(x0 + 0.5, Math.round(y1 * vr) + 0.5, W - x0, Math.max(2, Math.round((y2 - y1) * vr)));
          if (!+zc.lab) return;
          ctx.fillStyle = col; ctx.font = Math.round(11 * vr) + 'px system-ui,Arial';
          ctx.fillText((sup ? 'Support OB ' : 'Resistance OB ') + z.lo + ' - ' + z.hi, x0 + 6 * hr, Math.round(y1 * vr) + 13 * vr);
        });
      });
    } }; } }];
  };
  var zprim = null;
  function rgba(hex, a) { var h = String(hex || '#888888').replace('#', ''); if (h.length === 3) h = h.replace(/./g, '$&$&');
    var n = parseInt(h, 16); return 'rgba(' + ((n >> 16) & 255) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')'; }
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
      vs.setData(bars.map(function (b, i) { return { time: T(i), value: b.v, color: (b.c >= b.o ? CFG.vol.up : CFG.vol.dn) + '73' }; })); extra.push(vs);
    }
    ['ema9', 'ema20', 'ema50'].forEach(function (k) { var c = CFG[k]; if (IND[k]) line(ser(ema(closes, +c.len)), c.color, 0, +c.w, 'EMA' + c.len); });
    if (IND.sma200) { var cs = CFG.sma200; line(ser(sma(closes, +cs.len)), cs.color, 0, +cs.w, 'SMA' + cs.len); }
    if (IND.vwap && !htf()) { var pv = 0, vv = 0, cd0 = -1, vw = []; bars.forEach(function (b, i) { var d = day(b.t); if (d !== cd0) { cd0 = d; pv = 0; vv = 0; } pv += (b.h + b.l + b.c) / 3 * b.v; vv += b.v; vw.push(vv ? pv / vv : null); }); line(ser(vw), CFG.vwap.color, 0, +CFG.vwap.w, 'VWAP'); }
    var i, j;
    if (IND.bb) { var bL = +CFG.bb.len, bM = +CFG.bb.mult, m = sma(closes, bL), up = [], lo = []; for (i = 0; i < closes.length; i++) { if (m[i] == null) { up.push(null); lo.push(null); continue; } var s = 0; for (j = i - bL + 1; j <= i; j++) s += Math.pow(closes[j] - m[i], 2); var sd = Math.sqrt(s / bL); up.push(m[i] + bM * sd); lo.push(m[i] - bM * sd); }
      line(ser(m), CFG.bb.color, 0, 1, ''); line(ser(up), CFG.bb.color, 0, 1, ''); line(ser(lo), CFG.bb.color, 0, 1, ''); }
    if (IND.st) { var sF = +CFG.st.mult, a = atr(bars, +CFG.st.len), fu = [], fl = [], dir = 1, stU = [], stD = []; for (i = 0; i < bars.length; i++) { if (a[i] == null) { fu.push(null); fl.push(null); continue; }
        var hl = (bars[i].h + bars[i].l) / 2, bu = hl + sF * a[i], bl = hl - sF * a[i]; var pu = fu[i - 1], pl_ = fl[i - 1];
        fu.push(pu == null || bu < pu || bars[i - 1].c > pu ? bu : pu); fl.push(pl_ == null || bl > pl_ || bars[i - 1].c < pl_ ? bl : pl_);
        if (dir === 1 && bars[i].c < fl[i]) dir = -1; else if (dir === -1 && bars[i].c > fu[i]) dir = 1;
        stU.push(dir === 1 ? { time: T(i), value: fl[i] } : { time: T(i) }); stD.push(dir === -1 ? { time: T(i), value: fu[i] } : { time: T(i) }); }
      var s1 = chart.addSeries(LW.LineSeries, { color: CFG.st.up, lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }); s1.setData(stU); extra.push(s1);
      var s2 = chart.addSeries(LW.LineSeries, { color: CFG.st.dn, lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }); s2.setData(stD); extra.push(s2); }
    var pane = 1;
    if (IND.rsi) { var r = line(ser(rsi(closes, +CFG.rsi.len)), CFG.rsi.color, pane, +CFG.rsi.w, 'RSI' + CFG.rsi.len); r.createPriceLine({ price: +CFG.rsi.ob, color: '#ef5350', lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: '' }); r.createPriceLine({ price: +CFG.rsi.os, color: '#26a69a', lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: '' }); try { chart.panes()[pane].setHeight(110); } catch (e) {} pane++; }
    if (IND.macd) { var mF = +CFG.macd.fast, mS = +CFG.macd.slow, mG = +CFG.macd.sig, e12 = ema(closes, mF), e26 = ema(closes, mS), mc = []; for (i = 0; i < closes.length; i++) mc.push(e12[i] != null && e26[i] != null ? e12[i] - e26[i] : null);
      var base = mc.map(function (v) { return v == null ? 0 : v; }), sg = ema(base, mG), first = Math.max(mF, mS) + mG - 2; for (i = 0; i < sg.length; i++) if (mc[i] == null || i < first) sg[i] = null;
      var hs = chart.addSeries(LW.HistogramSeries, { priceLineVisible: false, lastValueVisible: false }, pane);
      hs.setData(bars.map(function (b, i) { return mc[i] != null && sg[i] != null ? { time: T(i), value: mc[i] - sg[i], color: mc[i] - sg[i] >= 0 ? 'rgba(38,166,154,.6)' : 'rgba(239,83,80,.6)' } : { time: T(i) }; })); extra.push(hs);
      line(ser(mc.map(function (v, i) { return sg[i] != null ? v : null; })), CFG.macd.mcol, pane, 1, 'MACD'); line(ser(sg), CFG.macd.scol, pane, 1, 'Signal');
      try { chart.panes()[pane].setHeight(110); } catch (e) {} pane++; }
    // spurt markers (volume of the candle / average of the 50 candles before it)
    var mk = [];
    var spN = +CFG.spurt.len, spX = +CFG.spurt.mult;
    if (IND.spurt) for (i = spN; i < bars.length; i++) { var s = 0; for (j = i - spN; j < i; j++) s += bars[j].v; var base2 = s / spN; if (base2 > 0 && bars[i].v / base2 >= spX) mk.push({ time: T(i), position: bars[i].c >= bars[i].o ? 'belowBar' : 'aboveBar', color: CFG.spurt.color, shape: 'circle', text: (bars[i].v / base2).toFixed(0) + 'x' }); }
    markers.setMarkers(mk);
    // zones from the scanner rules
    if (IND.zones && info && info.zones && info.zones.length) { zprim = new ZonePrim(info.zones.map(function (z) { return { type: z.type, lo: z.lo, hi: z.hi, tt: snap(z.t0) }; })); candle.attachPrimitive(zprim); }
    // the trade being set up / open trade
    if (IND.lines) {
      var L = S.levels; if (L.entry) pl(L.entry, '#fdd835', 'ENTRY', 0); if (L.sl) pl(L.sl, '#ff5252', 'SL', 0); if (L.t1) pl(L.t1, '#69f0ae', 'T1', 0); if (L.t2) pl(L.t2, '#00e676', 'T2', 0);
    }
    if (!keepRange) fitRecent();
  }

  /* FIX: while the new stock / timeframe was downloading, the old candles stayed on screen under the new name
     (e.g. RELIANCE monthly candles titled ITC M). Now the chart is emptied at once and says it is loading. */
  function blank() {
    bars = []; clearExtra();
    try { candle.setData([]); markers.setMarkers([]); } catch (e) {}
    leg.innerHTML = '<b>' + sym + '</b> ' + tfLabel() + ' &nbsp;<span style="color:#ffd54f">loading candles...</span>';
  }

  /* show the latest candles with some empty space on the right, like TradingView (also used by Enlarge) */
  function fitRecent() {
    var n = bars.length; if (!n) return;
    /* FIX: once the price axis was dragged / zoomed by hand, the chart stopped auto-scaling, and the next stock or timeframe
       opened with the OLD price range (SBIN at 961 shown on ITC's 100-600 scale, candles cut off at the top).
       Every new stock / timeframe now switches auto-scale back on, so the candles always fit the height. */
    try { candle.priceScale().applyOptions({ autoScale: true }); } catch (e) {}
    try { chart.priceScale('right').applyOptions({ autoScale: true }); } catch (e) {}
    var show = TF === 'D' ? 160 : htf() ? 120 : 110;
    chart.timeScale().setVisibleLogicalRange({ from: Math.max(-2, n - show), to: n + 8 });
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
  /* FIX: a slow answer for the previous stock / timeframe could arrive after you switched and be drawn under the new name
     (e.g. WIPRO candles titled ITC), and a switch made while the 20-second refresh was running was skipped. Now every request
     is numbered and only the newest answer is drawn; a stock or timeframe switch always loads and re-fits the chart. */
  var reqId = 0, inflight = 0, needFit = false;
  function load(keep) {
    if (!sym) return;
    if (keep && inflight) return;                    // the 20-second refresh waits while a request is still running
    if (!keep) needFit = true;
    var my = ++reqId, s = sym, t = TF;
    inflight++;
    if (!keep) document.getElementById('pcmsg').textContent = 'Loading ' + s + ' ' + tfLabel() + ' candles...';
    fetch('/paper/api/candles?symbol=' + encodeURIComponent(s) + '&tf=' + t).then(function (r) { return r.json(); }).then(function (j) {
      inflight--;
      if (my !== reqId || s !== sym || t !== TF) return;   // an older answer: ignore it
      var st = document.getElementById('pcmsg');
      if (!j.ok) { st.textContent = j.msg; leg.innerHTML = '<b>' + sym + '</b> ' + tfLabel() + ' &nbsp;<span style="color:#ff6b6b">' + j.msg + '</span>'; return; } st.textContent = '';
      bars = j.bars; draw(!needFit); needFit = false;
      var b = bars[bars.length - 1]; if (b) leg.innerHTML = '<b>' + sym + '</b> ' + tfLabel() + ' &nbsp;O ' + fmt(b.o) + ' H ' + fmt(b.h) + ' L ' + fmt(b.l) + ' C ' + fmt(b.c);
    }).catch(function () { inflight--; });
  }
  window.PChart = {
    setSymbol: function (s) { sym = s; info = null; blank(); document.getElementById('pctitle').textContent = s; var a = document.getElementById('ptvlink'); if (a) a.href = 'https://in.tradingview.com/chart/?symbol=NSE%3A' + encodeURIComponent(s.replace(/-/g, '_')); load(false); },
    setInfo: function (i) { info = i; if (bars.length) draw(true); },
    setLevels: function (l) { S.levels = l || {}; if (bars.length) { var keep = true; draw(keep); } },
    tick: function (price) {
      if (!bars.length || !price) return;
      var b = bars[bars.length - 1];
      if (!liveCandleOpen(b)) return;                                                  // market closed or old candle: leave it alone
      b.c = price; b.h = Math.max(b.h, price); b.l = Math.min(b.l, price);
      candle.update({ time: T(bars.length - 1), open: b.o, high: b.h, low: b.l, close: price });
    },
    refresh: function () { load(true); },
    zoneQuery: function () { var z = CFG.zones; return '&zz=' + z.zz + '&nob=' + z.nob + '&zatr=' + z.atr; }
  };
  /* ---------- controls ---------- */
  document.querySelectorAll('#ptf button').forEach(function (b) { b.addEventListener('click', function () { TF = b.dataset.tf; document.querySelectorAll('#ptf button').forEach(function (x) { x.classList.toggle('on', x === b); }); info = null; blank(); load(false); if (window.onPTf) window.onPTf(TF); }); });
  var menu = document.getElementById('pind'), setp = null;
  function buildMenu() {
    menu.innerHTML = ROWS.map(function (r) {
      return '<div class=irow><label><input type=checkbox data-k=' + r[0] + (IND[r[0]] ? ' checked' : '') + '>' + rowLabel(r) + '</label>' +
             (r[2].length ? '<button class=igear data-k=' + r[0] + ' title="Settings">&#9881;</button>' : '') + '</div>';
    }).join('') + '<div class=ifoot><button id=ireset>Reset all to defaults</button></div>';
    menu.querySelectorAll('input[type=checkbox]').forEach(function (c) { c.addEventListener('change', function () { IND[c.dataset.k] = c.checked ? 1 : 0; try { localStorage.setItem('pchart_ind', JSON.stringify(IND)); } catch (e) {} if (bars.length) draw(true); }); });
    menu.querySelectorAll('.igear').forEach(function (b) { b.addEventListener('click', function (e) { e.preventDefault(); e.stopPropagation(); openSettings(b.dataset.k); }); });
    document.getElementById('ireset').addEventListener('click', function (e) { e.stopPropagation(); if (!confirm('Reset every indicator setting to its default?')) return; CFG = JSON.parse(JSON.stringify(DEF)); saveCfg(); closeSettings(); buildMenu(); if (bars.length) draw(true); if (window.onZoneCfg) window.onZoneCfg(); });
  }
  function closeSettings() { if (setp) { setp.remove(); setp = null; } }
  function openSettings(k) {
    closeSettings();
    var r = ROWS.filter(function (x) { return x[0] === k; })[0], c = CFG[k];
    setp = document.createElement('div'); setp.id = 'pset';
    setp.innerHTML = '<div class=sh><b>' + r[1] + ' settings</b><button class=sx title=Close>&#10005;</button></div>' + r[2].map(function (f) {
      var v = c[f[0]];
      return '<label class=sf><span>' + f[1] + '</span>' + (f[2] === 'color' ? '<input type=color data-f=' + f[0] + ' value="' + v + '">' :
             f[2] === 'bool' ? '<input type=checkbox data-f=' + f[0] + (+v ? ' checked' : '') + '>' :
             '<input type=number data-f=' + f[0] + ' value="' + v + '" min=' + f[3] + ' max=' + f[4] + ' step=' + f[5] + '>') + '</label>';
    }).join('') + '<div class=sb><button class=sd>Defaults</button><button class=sc>Cancel</button><button class=so>OK</button></div>';
    menu.parentNode.appendChild(setp);
    setp.addEventListener('click', function (e) { e.stopPropagation(); });
    setp.querySelector('.sx').onclick = setp.querySelector('.sc').onclick = closeSettings;
    setp.querySelector('.sd').onclick = function () { r[2].forEach(function (f) { var inp = setp.querySelector('[data-f=' + f[0] + ']'); if (f[2] === 'bool') inp.checked = !!DEF[k][f[0]]; else inp.value = DEF[k][f[0]]; }); };
    setp.querySelector('.so').onclick = function () {
      var bad = '', n = {};                          // checked on a copy, so a wrong value never half-changes the settings
      r[2].forEach(function (f) {
        var inp = setp.querySelector('[data-f=' + f[0] + ']'), v = f[2] === 'bool' ? (inp.checked ? 1 : 0) : inp.value;
        if (f[2] === 'num') { v = parseFloat(v); if (isNaN(v) || v < f[3] || v > f[4]) { bad = bad || (f[1] + ' must be between ' + f[3] + ' and ' + f[4]); return; } if (f[5] >= 1) v = Math.round(v); }
        n[f[0]] = v;
      });
      if (!bad && k === 'macd' && +n.fast >= +n.slow) bad = 'Fast length must be smaller than Slow length';
      if (!bad && k === 'rsi' && +n.os >= +n.ob) bad = 'Oversold must be below Overbought';
      if (bad) { alert(bad); return; }
      for (var f2 in n) c[f2] = n[f2];
      IND[k] = 1; try { localStorage.setItem('pchart_ind', JSON.stringify(IND)); } catch (e) {}
      saveCfg(); closeSettings(); buildMenu(); if (bars.length) draw(true);
      if (k === 'zones' && window.onZoneCfg) window.onZoneCfg();          // swing length / zones per side / ATR: ask the server again
    };
    setp.querySelectorAll('input').forEach(function (inp) { inp.addEventListener('keydown', function (e) { if (e.key === 'Enter') setp.querySelector('.so').click(); if (e.key === 'Escape') closeSettings(); }); });
  }
  buildMenu();
  menu.addEventListener('click', function (e) { e.stopPropagation(); });
  document.getElementById('pindbtn').addEventListener('click', function (e) { e.stopPropagation(); var open = menu.style.display === 'block'; menu.style.display = open ? 'none' : 'block'; if (open) closeSettings(); });
  document.addEventListener('click', function () { if (setp) return; menu.style.display = 'none'; });
  var card = document.getElementById('pcard');
  document.getElementById('pbig').addEventListener('click', function () { card.classList.toggle('big'); document.body.style.overflow = card.classList.contains('big') ? 'hidden' : ''; this.textContent = card.classList.contains('big') ? 'Close ✕' : 'Enlarge ⛶'; setTimeout(fitRecent, 80); });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && card.classList.contains('big')) document.getElementById('pbig').click(); });
  setInterval(function () { if (sym) load(true); }, 20000);
  if (window.onPChartReady) { try { window.onPChartReady(); } catch (e) {} }   // draw the stock the page already picked
})();
