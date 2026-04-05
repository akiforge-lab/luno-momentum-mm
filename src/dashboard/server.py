"""
Lightweight read-only dashboard served on localhost.

One aiohttp app, two routes:
  GET /          — single-page HTML (auto-refreshes every 3 s via fetch)
  GET /api/state — JSON snapshot of all pair states

State is a plain module-level dict updated by the quoting loop on every tick.
No locking needed: Python's GIL serialises the dict writes, and the dashboard
only ever reads stale-by-one-tick data at worst.
"""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any

from aiohttp import web

from src.utils.logger import get_logger

log = get_logger(__name__)

# ── Shared state (written by quoting loop, read by HTTP handler) ─────────────

_bot_state: dict[str, Any] = {
    "mode": "DRY-RUN",
    "offline": False,
    "started_at": time.time(),
    "pairs": {},   # pair → dict
}


def set_mode(mode: str) -> None:
    _bot_state["mode"] = mode


def set_offline(offline: bool) -> None:
    _bot_state["offline"] = offline


def update_pair(pair: str, data: dict) -> None:
    _bot_state["pairs"][pair] = data


# ── Stub data for --no-auth mode ──────────────────────────────────────────────

_STUB_PAIRS = {
    "XBTMYR": {"mid": 480_000, "spread": 96, "signal": "LONG",    "slope": 87.4,  "r2": 0.81, "score": 70.8,  "inv": 0.0012,  "tgt": 0.0025, "rpnl":  12.40, "cost": 475_000},
    "ETHMYR": {"mid":  10_200, "spread":  51, "signal": "NEUTRAL", "slope":  -8.2,  "r2": 0.38, "score": -3.1,  "inv": 0.0,     "tgt": 0.0,    "rpnl":  -2.10, "cost":   0},
    "XRPMYR": {"mid":      2.3,"spread": 0.1, "signal": "SHORT",   "slope": -41.5,  "r2": 0.74, "score": -30.7, "inv": 0.0,     "tgt": 0.0,    "rpnl":   0.0,  "cost":   0},
}

_stub_tick = 0


def seed_stub_data() -> None:
    """Populate _bot_state with animated mock data for --no-auth mode."""
    global _stub_tick
    _stub_tick += 1
    t = _stub_tick * 0.05

    for pair, s in _STUB_PAIRS.items():
        jitter = math.sin(t + hash(pair) % 7) * s["spread"] * 0.3
        mid = s["mid"] + jitter
        half = s["spread"] / 2
        bid_px = mid - half
        ask_px = mid + half
        book_spread_bps = s["spread"] / s["mid"] * 10_000

        is_long = s["signal"] == "LONG"
        mtm = s["inv"] * (mid - s["cost"]) if s["inv"] > 0 else 0.0
        inv_ratio = (s["inv"] - s["tgt"]) / 0.005 if s["tgt"] > 0 else 0.0
        inv_ratio = max(-1.0, min(1.0, inv_ratio))

        update_pair(pair, {
            "pair": pair,
            # Signal (clearly labeled as stub)
            "signal":          s["signal"] + " [STUB]",
            "slope_ann_pct":   s["slope"],
            "r2":              s["r2"],
            "momentum_score":  s["score"],
            # Market
            "mid":             round(mid, 4),
            "book_spread_bps": round(book_spread_bps, 1),
            # Quote
            "reservation_mid": round(mid * 1.0008 if is_long else mid, 4),
            "bid_price":       round(bid_px, 4) if is_long else None,
            "bid_size":        0.0005 if is_long else 0.0,
            "ask_price":       round(ask_px, 4) if s["inv"] > 0 else None,
            "ask_size":        0.0005 if s["inv"] > 0 else 0.0,
            "spread_bps":      round(book_spread_bps, 1),
            # Inventory
            "inventory":       s["inv"],
            "target_inventory":s["tgt"],
            "max_inventory":   0.005,
            "dynamic_max_inventory": 0.005,
            "allocation_pct":  0.20,
            "portfolio_value_myr": 10000.0,
            # PnL
            "realized_pnl_myr":s["rpnl"],
            "mtm_pnl_myr":     round(mtm, 4),
            # Breakdown
            "inv_ratio":       round(inv_ratio, 4),
            "inv_shift_bps":   round(-(inv_ratio * 1.5 * half / s["mid"] * 10_000), 2),
            "mom_shift_bps":   8.0 if is_long else (-8.0 if s["signal"] == "SHORT" else 0.0),
            "half_bps":        round(half / s["mid"] * 10_000, 2),
            # Status
            "ws_connected":    False,
            "ws_age_sec":      0.0,
            "quoting":         is_long,
            "halt_reason":     "" if is_long else "no_auth_stub",
            "active_bid":      is_long,
            "active_ask":      s["inv"] > 0,
            "updated_at":      time.time(),
        })


# ── HTTP handlers ─────────────────────────────────────────────────────────────


async def _handle_index(_request: web.Request) -> web.Response:
    return web.Response(text=_HTML, content_type="text/html", charset="utf-8")


async def _handle_state(_request: web.Request) -> web.Response:
    uptime = time.time() - _bot_state["started_at"]
    payload = {
        "mode": _bot_state["mode"],
        "offline": _bot_state["offline"],
        "uptime_sec": round(uptime, 1),
        "pairs": list(_bot_state["pairs"].values()),
    }
    return web.json_response(payload)


# ── Server lifecycle ──────────────────────────────────────────────────────────


async def run(host: str = "127.0.0.1", port: int = 8088, offline: bool = False) -> None:
    """Start the dashboard HTTP server. Run as an asyncio task."""
    set_offline(offline)

    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/state", _handle_state)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    log.info('"dashboard started"', extra={"url": f"http://{host}:{port}", "offline": offline})

    if offline:
        # Drive animated stub data while server runs
        seed_stub_data()   # populate immediately so first load is non-empty
        while True:
            await asyncio.sleep(2)
            seed_stub_data()
    else:
        while True:
            await asyncio.sleep(3600)


# ── Embedded single-page HTML ─────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Luno MM</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0e1117;color:#cdd6f4;font:12px/1.5 "JetBrains Mono","Courier New",monospace}
a{color:inherit;text-decoration:none}
header{padding:10px 18px;background:#1e2030;border-bottom:1px solid #313244;display:flex;gap:14px;align-items:center}
h1{font-size:14px;color:#cba6f7;letter-spacing:.5px}
.badge{font-size:10px;padding:2px 8px;border-radius:3px;font-weight:bold}
.dry{background:#f9e2af22;color:#f9e2af}.live{background:#f3817022;color:#f38170}.offline{background:#cba6f722;color:#cba6f7}
#offlinebar{display:none;background:#cba6f722;border-bottom:1px solid #cba6f744;padding:6px 18px;font-size:11px;color:#cba6f7;letter-spacing:.3px}
#ts{margin-left:auto;color:#45475a;font-size:10px}
main{padding:14px 18px}
table{width:100%;border-collapse:collapse;font-size:11px}
th{padding:5px 7px;text-align:right;color:#585b70;font-weight:normal;border-bottom:1px solid #313244;white-space:nowrap;cursor:default}
th:first-child,th:nth-child(2){text-align:left}
td{padding:4px 7px;text-align:right;border-bottom:1px solid #181825;white-space:nowrap}
td:first-child,td:nth-child(2){text-align:left}
tbody tr{cursor:pointer;transition:background .1s}
tbody tr:hover td{background:#1e2030}
tbody tr.sel td{background:#181825;border-bottom-color:#313244}
.LONG{color:#a6e3a1}.SHORT{color:#f38170}.NEUTRAL{color:#585b70}
.ok{color:#a6e3a1}.warn{color:#f9e2af}.err{color:#f38170}
.pos{color:#a6e3a1}.neg{color:#f38170}.zero{color:#585b70}
#detail{margin-top:14px;background:#1e2030;border:1px solid #313244;border-radius:5px;padding:14px;display:none}
#dtitle{font-size:12px;color:#cba6f7;margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:7px}
.card{background:#181825;border-radius:4px;padding:9px 11px}
.clabel{color:#585b70;font-size:10px;margin-bottom:3px}
.cval{font-size:12px}
.arrow{color:#45475a;margin:0 3px}
footer{padding:6px 18px;font-size:10px;color:#45475a;border-top:1px solid #181825;margin-top:12px}
</style>
</head>
<body>
<div id="offlinebar">OFFLINE / STUB MODE — no exchange connections, no real data. Values are simulated for UI preview only.</div>
<header>
  <h1>⬡ Luno Momentum MM</h1>
  <span id="badge" class="badge">…</span>
  <span id="uptime"></span>
  <span id="ts">—</span>
</header>
<main>
<table>
<thead><tr>
  <th>Pair</th><th>Signal</th>
  <th>Slope%/yr</th><th>R²</th><th>Score</th>
  <th>Mid</th><th>ResMid</th>
  <th>Bid Px</th><th>Bid Sz</th>
  <th>Ask Px</th><th>Ask Sz</th>
  <th>Inv</th><th>Tgt Inv</th>
  <th>RPnL</th><th>MTM</th>
  <th>WS</th><th>Quoting</th>
</tr></thead>
<tbody id="tbody"></tbody>
</table>
<div id="detail">
  <div id="dtitle"></div>
  <div class="grid" id="dgrid"></div>
</div>
</main>
<footer id="foot">Auto-refresh every 3 s</footer>
<script>
'use strict';
let sel=null, cache={};

const f=(v,d=2)=>v==null?'—':Number(v).toFixed(d);
const fpnl=v=>{if(v==null)return'—';const c=v>0.001?'pos':v<-0.001?'neg':'zero';return`<span class="${c}">${v>=0?'+':''}${Number(v).toFixed(4)}</span>`};
const fws=d=>{if(!d.ws_connected)return['err','✗ off'];if(d.ws_age_sec>30)return['warn',`⚠ ${d.ws_age_sec.toFixed(0)}s`];return['ok',`✓ ${d.ws_age_sec.toFixed(1)}s`]};
const fq=d=>d.quoting?['ok','✓']:['err',`✗ ${d.halt_reason||''}`];

function renderTable(pairs){
  const tb=document.getElementById('tbody');
  tb.innerHTML='';
  pairs.sort((a,b)=>a.pair.localeCompare(b.pair));
  pairs.forEach(d=>{
    cache[d.pair]=d;
    const tr=document.createElement('tr');
    if(d.pair===sel)tr.className='sel';
    tr.onclick=()=>{sel=sel===d.pair?null:d.pair;renderDetail();renderTable(Object.values(cache))};
    const[wc,wl]=fws(d);const[qc,ql]=fq(d);
    tr.innerHTML=`<td><b>${d.pair}</b></td>`+
      `<td class="${d.signal||'NEUTRAL'}">${d.signal||'—'}</td>`+
      `<td>${f(d.slope_ann_pct,1)}%</td><td>${f(d.r2,3)}</td><td>${f(d.momentum_score,1)}</td>`+
      `<td>${f(d.mid,2)}</td><td>${f(d.reservation_mid,2)}</td>`+
      `<td>${f(d.bid_price,2)}</td><td>${f(d.bid_size,6)}</td>`+
      `<td>${f(d.ask_price,2)}</td><td>${f(d.ask_size,6)}</td>`+
      `<td>${f(d.inventory,6)}</td><td>${f(d.target_inventory,6)}</td>`+
      `<td>${fpnl(d.realized_pnl_myr)}</td><td>${fpnl(d.mtm_pnl_myr)}</td>`+
      `<td class="${wc}">${wl}</td>`+
      `<td class="${qc}">${ql}</td>`;
    tb.appendChild(tr);
  });
}

function card(label,value){return`<div class="card"><div class="clabel">${label}</div><div class="cval">${value}</div></div>`}

function renderDetail(){
  const el=document.getElementById('detail');
  if(!sel||!cache[sel]){el.style.display='none';return}
  el.style.display='block';
  const d=cache[sel];
  document.getElementById('dtitle').textContent=`${d.pair}  —  quote-decision breakdown`;
  const sgn=v=>v==null?'—':`${v>=0?'+':''}${Number(v).toFixed(2)} bps`;
  const dynMax=d.dynamic_max_inventory!=null?f(d.dynamic_max_inventory,6)+' base':'—';
  const allocPct=d.allocation_pct!=null?(d.allocation_pct*100).toFixed(0)+'%':'—';
  const portVal=d.portfolio_value_myr!=null?f(d.portfolio_value_myr,2)+' MYR':'—';
  const cards=[
    ['Portfolio value', portVal],
    ['Allocation %', allocPct],
    ['Dynamic max inventory', dynMax],
    ['Mid (book)', f(d.mid,4)+' MYR'],
    ['Book spread', f(d.book_spread_bps,1)+' bps'],
    ['Half-spread (post-widening)', f(d.half_bps,2)+' bps'],
    ['Signal', `<span class="${d.signal||'NEUTRAL'}">${d.signal||'NEUTRAL'}</span>`],
    ['Momentum shift (mom_shift)', sgn(d.mom_shift_bps)],
    ['Inventory', f(d.inventory,6)+' base'],
    ['Target inventory', f(d.target_inventory,6)+' base'],
    ['Inventory ratio', f(d.inv_ratio,4)+
      (d.inv_ratio!=null?(d.inv_ratio<0?' (below target → raise res_mid)':d.inv_ratio>0?' (above target → lower res_mid)':''):'')],
    ['Inventory shift (inv_shift)', sgn(d.inv_shift_bps)],
    ['Reservation mid', f(d.reservation_mid,4)+' MYR'],
    ['Bid  price × size', f(d.bid_price,4)+' × '+f(d.bid_size,6)],
    ['Ask  price × size', f(d.ask_price,4)+' × '+f(d.ask_size,6)],
    ['Quote spread', f(d.spread_bps,1)+' bps'],
    ['Slope %/yr', f(d.slope_ann_pct,2)+'%'],
    ['R²', f(d.r2,4)],
    ['Momentum score', f(d.momentum_score,2)],
    ['Realized PnL', f(d.realized_pnl_myr,4)+' MYR'],
    ['MTM PnL', f(d.mtm_pnl_myr,4)+' MYR'],
    ['WS age', f(d.ws_age_sec,1)+'s'],
    ['Active orders', (d.active_bid?'BID ':'')+(d.active_ask?'ASK':'')+'&nbsp;'],
  ];
  document.getElementById('dgrid').innerHTML=cards.map(([l,v])=>card(l,v)).join('');
}

function fmtUptime(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=Math.floor(s%60);return`${h}h ${m}m ${sec}s`}

async function refresh(){
  try{
    const r=await fetch('/api/state');
    const data=await r.json();
    const badge=document.getElementById('badge');
    const offlinebar=document.getElementById('offlinebar');
    if(data.offline){badge.textContent='OFFLINE';badge.className='badge offline';offlinebar.style.display='block';}
    else{badge.textContent=data.mode;badge.className='badge '+(data.mode==='DRY-RUN'?'dry':'live');offlinebar.style.display='none';}
    document.getElementById('uptime').textContent='up '+fmtUptime(data.uptime_sec);
    document.getElementById('ts').textContent='Updated '+new Date().toLocaleTimeString();
    renderTable(data.pairs||[]);
    renderDetail();
    document.getElementById('foot').textContent='Auto-refresh every 3 s — '+data.pairs.length+' pair(s) active';
  }catch(e){document.getElementById('foot').textContent='⚠ '+e;}
}
refresh();setInterval(refresh,3000);
</script>
</body>
</html>"""
