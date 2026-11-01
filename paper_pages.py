"""HTML pages of the paper-trade app (own CSS, mobile friendly). The login bar, background and footer are added by auth_gate.add_theme."""
from html import escape as e

CSS = """<style>
*{box-sizing:border-box}body{margin:0;font:14px system-ui,Arial,sans-serif;color:#e6e9ef}
.w{max-width:1300px;margin:0 auto;padding:12px 14px 60px}
nav{display:flex;gap:6px;flex-wrap:wrap;margin:8px 0 12px}nav a{padding:7px 14px;border-radius:8px;background:rgba(29,42,68,.85);color:#cfe0ff;text-decoration:none;border:1px solid #2f3a50}
nav a.on{background:#2d5bd6;color:#fff;border-color:#2d5bd6}
.card{background:rgba(20,26,38,.88);border:1px solid #2f3a50;border-radius:10px;padding:12px 14px;margin:0 0 12px}
h2{margin:0 0 8px;font-size:17px}h3{margin:0 0 6px;font-size:15px}
.k{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}.k div{background:rgba(255,255,255,.05);border-radius:8px;padding:8px 10px}
.k b{display:block;font-size:19px;margin-top:2px}.k span{font-size:12px;color:#9aa8bf}
.g{color:#4cd07d}.r{color:#ff6b6b}.m{color:#9aa8bf}
table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #2a344a;padding:6px 8px;text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}th{color:#9aa8bf;font-weight:600;font-size:12px}.sc{overflow-x:auto}
input,select{background:#0f1520;color:#e6e9ef;border:1px solid #37445f;border-radius:7px;padding:8px 9px;font:inherit;width:100%}
label{display:block;font-size:12px;color:#9aa8bf;margin:0 0 3px}
.f{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;align-items:end}
button{border:0;border-radius:8px;padding:9px 16px;font:inherit;font-weight:600;cursor:pointer;color:#fff;background:#2d5bd6}
button.buy{background:#1f9d57}button.sell{background:#d6453d}button.sm{padding:4px 10px;font-size:12px;background:#47546f}button.gh{background:#47546f}
.seg{display:flex;gap:6px}.seg button{flex:1;background:#47546f}.seg button.on.b{background:#1f9d57}.seg button.on.s{background:#d6453d}.seg button.on.p{background:#2d5bd6}
.seg button:disabled{opacity:.35;cursor:not-allowed}
.tg{display:inline-block;padding:1px 6px;border-radius:6px;font-size:11px;font-weight:700;background:#47546f;color:#fff}.tg.cnc{background:#7b4fd6}
#msg{margin-top:8px;font-weight:600}.note{font-size:12px;color:#9aa8bf}.warn{background:#5a3b12;border:1px solid #a8741f;color:#ffe2a8;padding:8px 10px;border-radius:8px;margin:0 0 10px}
.me td{background:rgba(45,91,214,.18)}
.cols{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(300px,1fr);gap:12px;align-items:start}.cols>div{min-width:0}
@media(max-width:900px){.cols{grid-template-columns:1fr}}
#pchart{height:520px;border-radius:8px;overflow:hidden;background:#0f1520}
.ch{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:2px 4px 8px}.ch b{font-size:16px;margin-right:4px}
#ptfbtn{min-width:62px}#ptf{display:none;position:absolute;left:0;top:32px;z-index:55;background:#141a26;border:1px solid #37445f;border-radius:8px;padding:8px;width:210px;grid-template-columns:repeat(4,1fr);gap:4px;box-shadow:0 8px 24px rgba(0,0,0,.5)}#ptf.open{display:grid}#ptf .tfh{grid-column:1/-1;font-size:11px;color:#9aa8bf;text-transform:uppercase;letter-spacing:.4px;margin:4px 0 0}#ptf .tfh:first-child{margin-top:0}#ptf button{background:#47546f;padding:6px 0;border-radius:6px;font-size:12px}#ptf button.on{background:#2d5bd6}
#pind{display:none;position:absolute;left:0;top:30px;z-index:50;background:#141a26;border:1px solid #37445f;border-radius:8px;padding:8px 12px;width:230px;max-height:340px;overflow:auto}
#pind label{display:flex;gap:8px;align-items:center;color:#e6e9ef;font-size:13px;margin:5px 0;cursor:pointer}#pind input{width:auto}
#plegend{margin:0 4px 4px;min-height:16px;font-size:12px}
#pcard.big{position:fixed;inset:0;z-index:100001;background:#0b0f16;border-radius:0;padding:10px;display:flex;flex-direction:column;margin:0}#pcard.big #pchart{flex:1;height:auto}#pcard.big p.note{display:none}
.bd{display:inline-block;padding:2px 8px;border-radius:10px;font-size:12px;font-weight:700}.bd.sup{background:#1f9d57;color:#fff}.bd.res{background:#d6453d;color:#fff}
.zr{display:flex;justify-content:space-between;gap:8px;padding:4px 0;border-bottom:1px solid #2a344a}
#pcs{position:relative;display:inline-block}#pcs .ic{position:absolute;left:9px;top:50%;transform:translateY(-50%);color:#9aa8bf;font-size:13px;pointer-events:none}
#pcsearch{width:190px;padding:6px 10px 6px 30px;font-weight:700;font-size:15px;text-transform:uppercase;background:#0f1520;border:1px solid #37445f;border-radius:8px;cursor:pointer}
#pcsearch:focus{cursor:text;border-color:#2d5bd6;outline:none}
#pcdd{display:none;position:absolute;top:38px;left:0;z-index:60;width:340px;max-width:85vw;max-height:340px;overflow:auto;background:#141a26;border:1px solid #37445f;border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.5)}
#pcdd div{padding:8px 12px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;gap:12px;border-bottom:1px solid #222c40}
#pcdd div.on,#pcdd div:hover{background:#2d5bd6}#pcdd small{color:#9aa8bf;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#pcdd div.on small,#pcdd div:hover small{color:#dfe8ff}
#pcdd i{font-style:normal;font-size:11px;color:#9aa8bf;margin-left:6px}
</style>"""

DISCLAIMER = ("<p class=note>Practice only: no real money and no real orders. Prices are live from Dhan; fills happen at the live price seen by the app "
              "(no queue, no slippage), so real results will be a little worse. Costs are a flat estimate. This is education, not investment advice.</p>")


def nav(active):
    items = [("/paper", "Trade"), ("/paper/history", "My results"), ("/paper/leaderboard", "Leaderboard"), ("/paper/help", "How it works")]
    try:
        from flask import session
        if session.get("role") == "admin":
            items.append(("/admin/paper", "Admin"))
    except Exception:
        pass
    return "<nav>" + "".join(f"<a href='{u}'{' class=on' if u == active else ''}>{t}</a>" for u, t in items) + "</nav>"


def shell(title, active, body):
    return (f"<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>{e(title)}</title>{CSS}</head>"
            f"<body><div class=w>{nav(active)}{body}{DISCLAIMER}</div></body></html>")


def money(v, sign=False):
    s = f"{abs(v):,.0f}"
    if sign:
        return ("+" if v > 0 else "-" if v < 0 else "") + "Rs " + s
    return ("-" if v < 0 else "") + "Rs " + s


def cls(v):
    return "g" if v > 0 else "r" if v < 0 else "m"


def ptag(product):
    return "<span class='tg cnc'>CNC</span>" if product == "POSITIONAL" else "<span class=tg>MIS</span>"


TRADE_JS = r"""<script src="/paper/api/lw.js"></script>
<script src="/paper/api/chart.js" defer></script>
<script>
var side='BUY',prod='INTRADAY',ltp=null,S=null,cur='',tvOk=false,ptf='5';window.onPTf=function(t){ptf=t;var b=document.getElementById('ptfbtn'),m=+t;if(b)b.innerHTML=(isNaN(m)?t:(m>=60?(m/60)+'h':m+'m'))+' &#9662;';var d=document.getElementById('ptf');if(d)d.classList.remove('open');if(cur)loadInfo(cur)};
function $(i){return document.getElementById(i)}
function fm(v,d){return v==null?'-':Number(v).toLocaleString('en-IN',{maximumFractionDigits:d==null?0:d,minimumFractionDigits:d==null?0:d})}
function sg(v){return (v>0?'+':v<0?'-':'')+'Rs '+fm(Math.abs(v))}
function cl(v){return v>0?'g':v<0?'r':'m'}
function tag(p){return p==='POSITIONAL'?'<span class="tg cnc">CNC</span>':'<span class=tg>MIS</span>'}
function setSide(s){
  if(s=='SELL'&&prod=='POSITIONAL'){say('Positional (delivery) is BUY only. Switch to Intraday to short.',false);return}
  side=s;$('bb').className='b'+(s=='BUY'?' on':'');$('bs').className='s'+(s=='SELL'?' on':'');calc()}
function setProd(p){
  prod=p;$('pmis').className='p'+(p=='INTRADAY'?' on':'');$('pcnc').className='p'+(p=='POSITIONAL'?' on':'');
  $('bs').disabled=(p=='POSITIONAL');if(p=='POSITIONAL'&&side=='SELL')setSide('BUY');
  $('pnote').innerHTML=p=='POSITIONAL'?'<b>Positional (CNC / delivery):</b> the full value is blocked, BUY only, carried overnight - no 15:20 square-off. SL and targets keep working every market day.':'<b>Intraday (MIS):</b> '+((S&&S.leverage)||5)+'x margin, buy or short, squared off automatically at 15:20.';
  calc()}
function esc(t){var d=document.createElement('div');d.textContent=t==null?'':t;return d.innerHTML}
function say(t,ok){var m=$('msg');m.textContent=t;m.className=ok?'g':'r'}
function post(u,b){return fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b||{})}).then(function(r){return r.json()})}
function calc(){
  var q=+$('qty').value,sl=+$('sl').value,t1=+$('t1').value,t2=+$('t2').value,lim=$('otype').value=='LIMIT'?+$('lim').value:0;
  $('limw').style.display=$('otype').value=='LIMIT'?'':'none';
  var ref=lim||ltp,o='',lev=prod=='POSITIONAL'?1:((S&&S.leverage)||5);
  if(ref&&q>0){
    o=(prod=='POSITIONAL'?'Amount blocked (full value): Rs ':'Margin blocked: Rs ')+fm(ref*q/lev);
    if(sl>0){var risk=Math.abs(ref-sl)*q;o+=' &middot; Risk to SL: <b class=r>Rs '+fm(risk)+'</b>';
      if(t1>0){var rw=Math.abs(t1-ref)*q;o+=' &middot; Reward at T1: <b class=g>Rs '+fm(rw)+'</b> (1 : '+(risk?(rw/risk).toFixed(1):'-')+')'}}
  }
  $('est').innerHTML=o;
  if(window.PChart&&cur){var ent=0;if(S)S.positions.forEach(function(x){if(x.symbol===cur)ent=x.entry});PChart.setLevels({entry:ent||lim||0,sl:sl>0?sl:0,t1:t1>0?t1:0,t2:t2>0?t2:0})}
}
function getQuote(){
  var s=$('sym').value.trim().toUpperCase();ltp=null;$('q').textContent='';if(!s){calc();return}
  if(s!==cur&&document.activeElement!==$('sym'))symChanged(s);
  fetch('/paper/api/quote?symbol='+encodeURIComponent(s)).then(function(r){return r.json()}).then(function(j){
    if(j.ok){ltp=j.price;$('q').innerHTML='Live price: <b>'+fm(j.price,2)+'</b>';if(window.PChart)PChart.tick(j.price)}else{$('q').textContent=j.msg}calc()})
}

function showChart(sym){if(window.PChart)PChart.setSymbol(sym)}
/* the chart script loads after this one; when it is ready it calls this, so the first stock is never missed */
window.onPChartReady=function(){if(cur){showChart(cur);loadInfo(cur)}};
function zr(label,z,cls2){return z?'<div class=zr><span>'+label+'</span><span><b>'+fm(z.lo,2)+' - '+fm(z.hi,2)+'</b> <span class=m>('+z.dist+'% away)</span></span></div>':'<div class=zr><span>'+label+'</span><span class=m>none found</span></div>'}
function loadInfo(sym){
  var el=$('info');if(!el||!sym)return;
  el.innerHTML='<span class=note>Reading zones and volume...</span>';
  var t0=ptf;fetch('/paper/api/chartinfo?symbol='+encodeURIComponent(sym)+'&tf='+t0).then(function(r){return r.json()}).then(function(j){
    if(sym!==cur||t0!==ptf)return;   /* an answer for a stock / timeframe you already left */
    if(!j.ok){el.innerHTML='<span class=note>'+esc(j.msg)+'</span>';return}
    var i=j.info,h='<div class=zr><span><b>'+esc(i.symbol)+'</b> '+fm(i.price,2)+'</span><span class='+cl(i.day_chg||0)+'>'+(i.day_chg==null?'':(i.day_chg>0?'+':'')+i.day_chg+'% today')+'</span></div>';
    if(i.at_zone)h+='<div style="margin:6px 0"><span class="bd '+(i.at_zone=='SUPPORT'?'sup':'res')+'">Price is AT a '+i.at_zone.toLowerCase()+' zone</span></div>';
    h+=zr('Nearest support',i.support)+zr('Nearest resistance',i.resistance);
    if(i.spurt){var m=i.spurt.multiple;h+='<div class=zr><span>Volume spurt (last 10-min candle, '+i.spurt.time+')</span><span><b'+(m>=5?' class=g':'')+'>'+m+'x</b></span></div>'}
    if(i.spurt_peak)h+='<div class=zr><span>Peak spurt today</span><span><b>'+i.spurt_peak.multiple+'x</b> <span class=m>at '+i.spurt_peak.time+'</span></span></div>';
    h+='<p class=note style="margin:6px 0 0">Zones use the same order-block rules as your indicator, built on the candles of the timeframe selected on the chart. Spurt = candle volume divided by the average of the 50 candles before it.</p>';
    el.innerHTML=h;if(window.PChart)PChart.setInfo(i)});
}
function symChanged(s){
  s=s.trim().toUpperCase();if(!s||s===cur)return;cur=s;showChart(s);loadInfo(s);pcFill();
}
/* ---------- TradingView-style stock search on the chart ---------- */
var ddI=-1,ddL=[],ddT=null;
function pcFill(){var b=$('pcsearch');if(b&&document.activeElement!==b)b.value=cur||''}
function pcHide(){$('pcdd').style.display='none'}
function pcDraw(){var d=$('pcdd');
  if(!ddL.length){d.innerHTML='<div class=m>No NSE stock found</div>';d.style.display='block';return}
  d.innerHTML=ddL.map(function(x,i){return '<div data-i='+i+(i==ddI?' class=on':'')+'><span><b>'+esc(x.symbol)+'</b><i>NSE</i></span><small>'+esc(x.name)+'</small></div>'}).join('');d.style.display='block';
  var on=d.querySelector('.on');if(on)on.scrollIntoView({block:'nearest'})}
function pcPick(s){pcHide();$('pcsearch').blur();if(!s){pcFill();return}s=s.toUpperCase();$('sym').value=s;symChanged(s);getQuote();pcFill()}
(function(){
  var b=$('pcsearch'),d=$('pcdd');
  b.addEventListener('focus',function(){b.select()});
  b.addEventListener('input',function(){clearTimeout(ddT);var v=b.value.trim();if(!v){pcHide();return}
    ddT=setTimeout(function(){fetch('/paper/api/symbols?q='+encodeURIComponent(v)).then(function(r){return r.json()}).then(function(j){
      if(b.value.trim()!==v)return;ddL=j||[];ddI=ddL.length?0:-1;pcDraw()})},150)});
  b.addEventListener('keydown',function(e){
    if(e.key==='ArrowDown'||e.key==='ArrowUp'){if(!ddL.length)return;e.preventDefault();ddI=(ddI+(e.key==='ArrowDown'?1:-1)+ddL.length)%ddL.length;pcDraw()}
    else if(e.key==='Enter'){e.preventDefault();pcPick(ddI>=0&&ddL[ddI]?ddL[ddI].symbol:b.value.trim())}
    else if(e.key==='Escape'){pcHide();b.blur();pcFill()}});
  b.addEventListener('blur',function(){setTimeout(function(){pcHide();pcFill()},150)});
  d.addEventListener('mousedown',function(e){e.preventDefault();var t=e.target.closest('[data-i]');if(t)pcPick(ddL[+t.dataset.i].symbol)});
})();
var tm=null;
function suggest(){
  var v=$('sym').value.trim();if(v.length<1)return;
  fetch('/paper/api/symbols?q='+encodeURIComponent(v)).then(function(r){return r.json()}).then(function(j){
    $('dl').innerHTML=j.map(function(x){return '<option value="'+esc(x.symbol)+'">'+esc(x.name)+'</option>'}).join('')})
}
function place(){
  say('Placing...',true);
  post('/paper/api/order',{symbol:$('sym').value,side:side,product:prod,qty:$('qty').value,sl:$('sl').value,t1:$('t1').value,t2:$('t2').value,
     limit:$('otype').value=='LIMIT'?$('lim').value:''}).then(function(j){say(j.msg,j.ok);if(j.ok)load()})
}
function act(u,b){post(u,b).then(function(j){say(j.msg,j.ok);load()})}
function load(){
  fetch('/paper/api/state').then(function(r){return r.json()}).then(function(s){
    S=s;
    $('k1').innerHTML=fm(s.equity);$('k2').innerHTML='<span class='+cl(s.total_pnl)+'>'+sg(s.total_pnl)+'</span>';
    $('k3').innerHTML='<span class='+cl(s.day_pnl)+'>'+sg(s.day_pnl)+'</span>';$('k4').innerHTML=fm(s.cash);$('k5').innerHTML='<span class='+cl(s.unrealized)+'>'+sg(s.unrealized)+'</span>';
    $('mk').innerHTML=s.market_open?'':'<div class=warn>The market is closed. Orders open at 09:15 IST (Monday to Friday); new orders stop at 15:15. Intraday trades are squared off at 15:20; positional trades stay open overnight.</div>';
    $('er').innerHTML=(s.symbols_loaded?'':'<div class=warn>The share list is still loading'+(s.symbols_err?' ('+esc(s.symbols_err)+')':'')+'. Try again in a minute.</div>')+(s.price_err?'<div class=warn>Live prices problem: '+esc(s.price_err)+'</div>':'');
    var h='';
    s.positions.forEach(function(p){
      var tg=(p.t1?fm(p.t1,2):'-')+(p.t2?' / '+fm(p.t2,2):'');
      h+='<tr><td><b>'+esc(p.symbol)+'</b> <span class=m>'+p.side+'</span> '+tag(p.product)+(p.product==='POSITIONAL'?' <span class=m>since '+esc((p.opened||'').slice(5,10))+'</span>':'')+'</td><td>'+fm(p.qty)+'</td><td>'+fm(p.entry,2)+'</td><td>'+(p.ltp?fm(p.ltp,2):'-')+'</td><td>'+fm(p.sl,2)+(p.t1_done?' <span class=m>(cost)</span>':'')+'</td><td>'+tg+'</td><td class='+cl(p.pnl)+'>'+sg(p.pnl)+' <span class=m>('+p.pnl_pct+'%)</span></td><td><button class=sm onclick="act(\'/paper/api/close\',{id:\''+p.id+'\'})">Close</button></td></tr>'});
    $('pos').innerHTML=h||'<tr><td colspan=8 class=m style="text-align:left">No open trades.</td></tr>';
    var o='';
    s.pending.forEach(function(x){o+='<tr><td><b>'+esc(x.symbol)+'</b> <span class=m>'+x.side+' limit</span> '+tag(x.product)+'</td><td>'+fm(x.qty)+'</td><td>'+fm(x.limit,2)+'</td><td>'+fm(x.sl,2)+'</td><td>'+(x.t1?fm(x.t1,2):'-')+(x.t2?' / '+fm(x.t2,2):'')+'</td><td><button class=sm onclick="act(\'/paper/api/cancel\',{id:\''+x.id+'\'})">Cancel</button></td></tr>'});
    $('pend').innerHTML=o?'<h3>Waiting limit orders</h3><div class=sc><table><tr><th>Stock</th><th>Qty</th><th>Limit</th><th>SL</th><th>Targets</th><th></th></tr>'+o+'</table></div>':'';
    $('notes').innerHTML=s.notes.map(function(n){return '<div class=note>'+esc(n.t)+' &middot; '+esc(n.m)+'</div>'}).join('');
    calc();
  }).catch(function(){})
}
$('sym').addEventListener('input',function(){clearTimeout(tm);tm=setTimeout(suggest,200)});
$('sym').addEventListener('change',function(){symChanged($('sym').value);getQuote()});
['qty','sl','t1','t2','lim','otype'].forEach(function(i){$(i).addEventListener('input',calc)});
$('otype').addEventListener('change',calc);
(function(){var b=$('ptfbtn'),d=$('ptf');b.addEventListener('click',function(e){e.stopPropagation();d.classList.toggle('open');var pi=$('pind');if(pi)pi.style.display='none'});d.addEventListener('click',function(e){e.stopPropagation()});document.addEventListener('click',function(){d.classList.remove('open')});document.addEventListener('keydown',function(e){if(e.key==='Escape')d.classList.remove('open')})})();
setSide('BUY');setProd('INTRADAY');load();cur='RELIANCE';pcFill();if(!$('sym').value.trim())$('sym').value=cur;getQuote();setTimeout(function(){showChart('RELIANCE');loadInfo('RELIANCE')},300);setInterval(function(){if(cur)loadInfo(cur)},60000);setInterval(load,4000);setInterval(function(){if($('sym').value)getQuote()},8000);
</script>"""


def trade_page(user):
    body = f"""
<div id=mk></div><div id=er></div>
<div class=card><h2>Hello {e(user)} - your practice account</h2>
<div class=k><div><span>Account value</span><b id=k1>-</b></div><div><span>Total profit / loss</span><b id=k2>-</b></div>
<div><span>Today</span><b id=k3>-</b></div><div><span>Free cash</span><b id=k4>-</b></div><div><span>Open trades P&amp;L</span><b id=k5>-</b></div></div></div>
<div class=cols>
<div>
<div class=card id=pcard style="padding:8px">
<div class=ch><b id=pctitle style="display:none">Pick a stock</b>
<span id=pcs><span class=ic>&#128269;</span><input id=pcsearch autocomplete=off spellcheck=false placeholder="Search stock" title="Search any NSE stock"><div id=pcdd></div></span>
<span style="position:relative"><button id=ptfbtn class=sm title="Timeframe">5m &#9662;</button><div id=ptf><div class=tfh>Minutes</div><button data-tf=5 class=on>5m</button><button data-tf=15>15m</button><button data-tf=30>30m</button><button data-tf=45>45m</button><div class=tfh>Hours</div><button data-tf=60>1h</button><button data-tf=120>2h</button><button data-tf=180>3h</button><button data-tf=240>4h</button><div class=tfh>Days</div><button data-tf=D>D</button><button data-tf=W>W</button><button data-tf=M>M</button></div></span>
<span style="position:relative"><button id=pindbtn class=sm>Indicators &#9662;</button><div id=pind>
<label><input type=checkbox data-k=vol>Volume</label><label><input type=checkbox data-k=vwap>VWAP</label>
<label><input type=checkbox data-k=ema9>EMA 9</label><label><input type=checkbox data-k=ema20>EMA 20</label><label><input type=checkbox data-k=ema50>EMA 50</label><label><input type=checkbox data-k=sma200>SMA 200</label>
<label><input type=checkbox data-k=bb>Bollinger Bands</label><label><input type=checkbox data-k=st>Supertrend (10,3)</label>
<label><input type=checkbox data-k=rsi>RSI 14</label><label><input type=checkbox data-k=macd>MACD</label>
<label><input type=checkbox data-k=spurt>Volume spurt marks (5x+)</label><label><input type=checkbox data-k=zones>Support / resistance zones</label><label><input type=checkbox data-k=lines>Entry / SL / target lines</label></div></span>
<button id=pbig class=sm>Enlarge &#9974;</button><a id=ptvlink href="https://in.tradingview.com/" target=_blank rel=noopener class=note style="color:#7db3ff">Open in TradingView &#8599;</a></div>
<div id=plegend class=note></div><div id=pchart></div><div id=pcmsg class=note></div>
<p class=note style="margin:6px 4px 0">Our own chart on live Dhan data. Pick a stock in the order form (or open one from your scanner pages). Choose indicators with the <b>Indicators</b> button; they are remembered on this device. Yellow circles mark candles with 5x or more volume. Green and red boxes are the support and resistance zones (order blocks) as in the indicator; your entry, SL and targets are drawn as you type them.</p></div>
<div class=card><h3>Zones and volume (from the scanner rules)</h3><div id=info><span class=note>Pick a stock to see its support / resistance zones and volume spurt.</span></div></div>
</div>
<div>
<div class=card><h2>New order</h2>
<div class=f>
<div style="grid-column:1/-1"><label>Stock (NSE)</label><input id=sym list=dl autocomplete=off placeholder="type e.g. RELIANCE"><datalist id=dl></datalist></div>
<div style="grid-column:1/-1"><label>Product</label><div class=seg><button id=pmis class="p on" onclick="setProd('INTRADAY')">INTRADAY (MIS)</button><button id=pcnc class=p onclick="setProd('POSITIONAL')">POSITIONAL (CNC)</button></div></div>
<div style="grid-column:1/-1"><label>Buy or sell</label><div class=seg><button id=bb class="b on" onclick="setSide('BUY')">BUY</button><button id=bs class=s onclick="setSide('SELL')">SELL</button></div></div>
<div><label>Quantity</label><input id=qty type=number min=1 value=10></div>
<div><label>Order type</label><select id=otype><option value=MARKET>Market (now)</option><option value=LIMIT>Limit (wait for price)</option></select></div>
<div id=limw style="display:none"><label>Limit price</label><input id=lim type=number step=0.05></div>
<div><label>Stop-loss (required)</label><input id=sl type=number step=0.05></div>
<div><label>Target 1</label><input id=t1 type=number step=0.05></div>
<div><label>Target 2 (optional)</label><input id=t2 type=number step=0.05></div>
<div style="grid-column:1/-1"><button onclick="place()" style="width:100%">Place order</button></div></div>
<div id=q class=note style="margin-top:8px"></div><div id=est class=note></div><div id=msg></div>
<p id=pnote class=note></p>
<p class=note>With Target 2 set: half is booked at Target 1 and your stop-loss moves to your entry price (the trailing idea). Without Target 2 the whole trade exits at Target 1.</p></div>
</div></div>
<div class=card><h2>Open trades</h2><div class=sc><table><tr><th>Stock</th><th>Qty</th><th>Entry</th><th>Price now</th><th>Stop-loss</th><th>Targets</th><th>Profit / loss</th><th></th></tr><tbody id=pos></tbody></table></div>
<div style="margin-top:8px"><button class=gh onclick="act('/paper/api/closeall')">Close all trades</button></div><div id=pend style="margin-top:12px"></div></div>
<div class=card><h3>Recent activity</h3><div id=notes></div></div>
{TRADE_JS}"""
    return shell("Paper trade", "/paper", body)


def _curve_svg(curve):
    if len(curve) < 2:
        return "<p class=note>The equity curve appears after your second closed trade.</p>"
    w, h, pad = 700, 180, 8
    lo, hi = min(0, min(curve)), max(0, max(curve))
    span = (hi - lo) or 1
    pts = [(pad + i * (w - 2 * pad) / (len(curve) - 1), h - pad - (v - lo) / span * (h - 2 * pad)) for i, v in enumerate(curve)]
    zero = h - pad - (0 - lo) / span * (h - 2 * pad)
    col = "#4cd07d" if curve[-1] >= 0 else "#ff6b6b"
    return (f"<svg viewBox='0 0 {w} {h}' style='width:100%;height:auto;background:rgba(255,255,255,.04);border-radius:8px'>"
            f"<line x1=0 x2={w} y1={zero:.1f} y2={zero:.1f} stroke='#47546f' stroke-dasharray='4 4'/>"
            f"<polyline fill=none stroke='{col}' stroke-width=2 points='{' '.join(f'{x:.1f},{y:.1f}' for x, y in pts)}'/></svg>")


def history_page(user, st):
    pf = "-" if st["profit_factor"] is None else st["profit_factor"]
    k = (f"<div class=k><div><span>Closed trades</span><b>{st['trades']}</b></div>"
         f"<div><span>Win rate</span><b>{st['win_rate']}%</b></div>"
         f"<div><span>Net profit (after costs)</span><b class={cls(st['net'])}>{money(st['net'], True)}</b></div>"
         f"<div><span>Average win</span><b class=g>{money(st['avg_win'], True)}</b></div>"
         f"<div><span>Average loss</span><b class=r>{money(st['avg_loss'], True)}</b></div>"
         f"<div><span>Profit factor</span><b>{pf}</b></div>"
         f"<div><span>Expectancy / trade</span><b class={cls(st['expectancy'])}>{money(st['expectancy'], True)}</b></div>"
         f"<div><span>Biggest drawdown</span><b class=r>{money(-st['max_dd'])}</b></div>"
         f"<div><span>Costs paid</span><b>{money(st['cost'])}</b></div></div>")
    teach = ("<p class=note><b>Profit factor</b> = total won / total lost (above 1 is profitable). <b>Expectancy</b> = what an average trade earns. "
             "A win rate below 50% can still make money if your average win is bigger than your average loss.</p>")
    rows = ""
    for c in reversed(st["closed"][-200:]):
        rows += (f"<tr><td><b>{e(c['symbol'])}</b> <span class=m>{c['side']}</span></td><td>{ptag(c.get('product'))}</td><td>{c['qty0']}</td><td>{c['entry']:g}</td><td>{c.get('exit', 0):g}</td>"
                 f"<td class={cls(c['net'])}>{money(c['net'], True)}</td><td>{e(', '.join(c['reasons']))}</td><td>{e(c['opened'][5:16])}</td><td>{e(c['closed'][5:16])}</td></tr>")
    days = "".join(f"<tr><td>{e(d)}</td><td class={cls(v)}>{money(v, True)}</td></tr>" for d, v in reversed(st["by_day"]))
    br = ", ".join(f"{e(k)}: {v}" for k, v in sorted(st["by_reason"].items(), key=lambda x: -x[1]))
    body = (f"<div class=card><h2>My results</h2>{k}{teach}</div>"
            f"<div class=card><h3>Profit curve (closed trades)</h3>{_curve_svg(st['curve'])}<p class=note>How your trades ended: {br or '-'}</p></div>"
            f"<div class=card><h3>Profit by day</h3><div class=sc><table><tr><th>Day</th><th>Net</th></tr>{days or '<tr><td colspan=2 class=m>No trades yet.</td></tr>'}</table></div></div>"
            f"<div class=card><h3>Trade list (latest 200)</h3><div class=sc><table><tr><th>Stock</th><th>Type</th><th>Qty</th><th>Entry</th><th>Exit</th><th>Net</th><th>How it ended</th><th>Opened</th><th>Closed</th></tr>"
            f"{rows or '<tr><td colspan=9 class=m style=text-align:left>No closed trades yet.</td></tr>'}</table></div>"
            f"<p><a href='/paper/history.csv' style='color:#7db3ff'>Download my trades (CSV)</a></p></div>"
            "<div class=card><h3>Start again</h3><p class=note>Resets your account to the starting money and clears your trade list (your old totals stay noted for the teacher). Close open trades first.</p>"
            "<button class=gh onclick=\"if(confirm('Reset your practice account?'))fetch('/paper/api/reset',{method:'POST'}).then(function(r){return r.json()}).then(function(j){alert(j.msg);location.reload()})\">Reset my account</button></div>")
    return shell("My results", "/paper/history", body)


def leaderboard_page(user, rows):
    tr = ""
    for i, r in enumerate(rows, 1):
        pf = "-" if r["pf"] is None else r["pf"]
        tr += (f"<tr{' class=me' if r['user'] == user else ''}><td>{i}. <b>{e(r['user'])}</b></td><td class={cls(r['pnl_pct'])}>{r['pnl_pct']:+.2f}%</td>"
               f"<td class={cls(r['pnl'])}>{money(r['pnl'], True)}</td><td>{r['trades']}</td><td>{r['win_rate']}%</td><td>{pf}</td></tr>")
    body = ("<div class=card><h2>Leaderboard</h2><p class=note>Ranked by profit as % of the starting money (open trades included). "
            "Look at win rate and profit factor too: a few lucky trades are not the same as a good method.</p>"
            f"<div class=sc><table><tr><th>Student</th><th>Profit %</th><th>Profit</th><th>Trades</th><th>Win rate</th><th>Profit factor</th></tr>{tr or '<tr><td colspan=6 class=m>Nobody has traded yet.</td></tr>'}</table></div></div>")
    return shell("Leaderboard", "/paper/leaderboard", body)


def help_page():
    from paper_engine import START_CASH, LEVERAGE, COST_PCT, COST_PCT_CNC
    body = f"""<div class=card><h2>How this practice app works</h2><ul style="line-height:1.7">
<li>You start with <b>Rs {START_CASH:,.0f}</b> of virtual money. No real orders are ever placed.</li>
<li>Prices are <b>live NSE prices</b>. New orders between 09:15 and 15:15 IST, Monday to Friday.</li>
<li>Every order is either <b>Intraday (MIS)</b> or <b>Positional (CNC / delivery)</b>:
<ul>
<li><b>Intraday:</b> margin is {LEVERAGE}x (a trade worth Rs 1,00,000 blocks Rs {100000 // LEVERAGE:,}). You can <b>buy</b> or <b>sell/short</b>. Anything still open is closed automatically at <b>15:20</b>. Cost {COST_PCT}% per side.</li>
<li><b>Positional:</b> the <b>full value</b> is blocked (no leverage) and the trade is <b>carried overnight</b> for as many days as you like - no 15:20 square-off. <b>BUY only</b> (on NSE you cannot hold a short in delivery). Cost {COST_PCT_CNC}% per side (delivery taxes are higher). Your SL and targets keep working every market day; if the stock opens past your SL after a gap, you exit at the opening price - just like real life.</li>
</ul></li>
<li><b>A stop-loss is required</b> on every order. It exits the trade if the price goes against you.</li>
<li><b>Targets:</b> Target 1 only = the whole trade exits there. Target 1 + Target 2 = half exits at Target 1, your stop-loss moves to your entry price (you can no longer lose on the rest), and the rest runs to Target 2.</li>
<li><b>Market order</b> fills immediately at the live price. <b>Limit order</b> waits until the price reaches your level (limit orders are for the day only and are cancelled at 15:20 if not filled).</li>
<li>One trade per stock at a time.</li>
<li>Use <b>My results</b> to see your win rate, average win and loss, profit factor and profit curve. Judge yourself on these, not on one lucky day.</li></ul></div>"""
    return shell("How it works", "/paper/help", body)


def admin_page(rows):
    tr = "".join(f"<tr><td>{e(r['user'])}</td><td>{money(r['equity'])}</td><td class={cls(r['pnl'])}>{money(r['pnl'], True)}</td><td>{r['trades']}</td><td>{r['resets']}</td>"
                 f"<td><form method=post action='/admin/paper/reset/{e(r['user'])}' onsubmit=\"return confirm('Reset {e(r['user'])}?')\"><button class=sm>Reset</button></form></td></tr>" for r in rows)
    body = (f"<div class=card><h2>Practice accounts (admin)</h2><div class=sc><table><tr><th>Student</th><th>Account value</th><th>Profit</th><th>Trades</th><th>Resets</th><th></th></tr>{tr or '<tr><td colspan=6>No accounts yet.</td></tr>'}</table></div>"
            "<p class=note>Users are approved at <a href='/admin/users' style='color:#7db3ff'>Users</a>. A reset discards that student's open trades and starts the account again.</p></div>")
    return shell("Admin practice accounts", "/paper", body)
