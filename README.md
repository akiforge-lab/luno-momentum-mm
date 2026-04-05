# luno-momentum-mm

A momentum-aware spot market-making bot for [Luno Malaysia](https://www.luno.com/en-my/).

Classifies BTC/ETH/XRP momentum from Luno daily candle data (OLS regression, same algorithm as the Hyperliquid Momentum Scanner) and quotes passively around a reservation price that skews with both momentum signal and inventory position.

---

## How it works

```
Luno daily candles  ──►  SignalAdapter (OLS momentum)  ──►  LONG / SHORT / NEUTRAL
                                                                      │
Luno WebSocket (live book)  ──►  QuotingEngine  ◄─────────────────────
                                      │
                          reservation_mid = mid
                            × (1 + momentum_shift_bps / 10 000)
                            × (1 + inventory_shift_bps / 10 000)
                          bid = reservation_mid × (1 − half_spread)
                          ask = reservation_mid × (1 + half_spread)
                                      │
                              RiskManager checks
                                      │
                          LunoClient POST /api/1/postorder
```

**Signal rules (spot-only)**

| Signal | Bid | Ask |
|--------|-----|-----|
| LONG   | placed at `bid_price` | placed at `ask_price` |
| SHORT / NEUTRAL | suppressed (0) | placed to reduce existing inventory |

**Dynamic inventory limit**

```
max_inventory = min(portfolio_value_myr × allocation_pct / mid, hard_cap_units)
```

Portfolio value is recomputed every tick from live MYR balance + all base positions × current mid.

---

## Project layout

```
luno-momentum-mm/
├── main.py                      CLI entry point
├── config.yaml                  All tunable parameters
├── .env.example                 Credential template
├── requirements.txt
├── src/
│   ├── exchange/
│   │   ├── luno_client.py       Async REST client (rate-limited, retried)
│   │   └── luno_ws.py           WebSocket order-book handler (auto-reconnect)
│   ├── signal/
│   │   └── signal_adapter.py   Self-contained momentum classifier (Luno candles)
│   ├── quoting/
│   │   └── quoting_engine.py   Reservation-price quoting with inventory + momentum skew
│   ├── orders/
│   │   └── order_manager.py    Order lifecycle, cancel/replace, fill tracking
│   ├── risk/
│   │   └── risk_manager.py     Inventory limits, daily-loss, kill-switch, dynamic sizing
│   ├── state/
│   │   └── state_store.py      Atomic JSON persistence (survives restarts)
│   ├── dashboard/
│   │   └── server.py           Local aiohttp dashboard (http://127.0.0.1:8088)
│   └── utils/
│       └── logger.py           Structured JSON logging
└── simulation/
    └── replay.py               Historical JSONL replay / fill simulator
```

---

## Setup

### Requirements

- Python 3.11+
- Luno Malaysia account with an API key (read + trade permissions)

### Install

```bash
git clone https://github.com/<you>/luno-momentum-mm
cd luno-momentum-mm
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configure credentials

```bash
cp .env.example .env
# Edit .env — add LUNO_API_KEY and LUNO_API_SECRET
```

### Review config

Open `config.yaml` and check the `pairs:` section:

- Enable/disable pairs with `enabled: true/false`
- Tune `base_spread_bps`, `quote_size_base`, `allocation_pct`, `hard_cap_units`

---

## Running

### Preview UI without credentials

```bash
python main.py --no-auth
# Open http://127.0.0.1:8088
```

Starts only the dashboard with animated stub data. Zero exchange calls.

### Dry-run (paper trading)

```bash
python main.py --dry-run
# Open http://127.0.0.1:8088
```

Connects to Luno WebSocket for real market data, classifies momentum from live candles, logs every order *would* be placed. No real orders sent. Fills are simulated locally.

### Record order-book snapshots

```bash
python main.py --dry-run --record
# Writes data/recordings/<PAIR>.jsonl
```

### Replay a recording

```bash
python -m simulation.replay \
  --file data/recordings/XBTMYR.jsonl \
  --pair XBTMYR \
  --signal LONG \
  --config config.yaml
```

### Live trading

Only after you are satisfied with dry-run behavior:

```bash
python main.py --live --confirm-live
```

Trade only specific pairs:

```bash
python main.py --live --confirm-live --pairs XBTMYR
```

### Emergency stop

```bash
touch kill.txt     # triggers global kill switch — bot cancels all orders and halts
```

Or set `global_kill_switch: true` in `config.yaml` before restarting.

---

## Risk controls

| Control | Config key | Default |
|---------|-----------|---------|
| Global kill switch | `risk.global_kill_switch` | `false` |
| Runtime kill file | `kill.txt` in CWD | — |
| Max daily loss (MYR) | `risk.max_daily_loss_myr` | 500 |
| Max open orders | `risk.max_open_orders_total` | 20 |
| Stale orderbook (sec) | `risk.stale_orderbook_threshold_sec` | 30 |
| Portfolio allocation | `pairs.<P>.allocation_pct` | per pair |
| Hard cap (base units) | `pairs.<P>.hard_cap_units` | per pair |
| Max notional (MYR) | `pairs.<P>.max_notional_myr` | per pair |
| Cancel on shutdown | `risk.cancel_on_shutdown` | `true` |

---

## Supported pairs

| Pair | Base |
|------|------|
| XBTMYR | BTC |
| ETHMYR | ETH |
| XRPMYR | XRP |
| SOLMYR | SOL (disabled by default) |
| LTCMYR | LTC (disabled by default) |

---

## Notes

- All orders use Luno's post-only endpoint (`POST /api/1/postorder`). The bot never places taker orders except via the emergency inventory-reduction path.
- `SHORT` and `NEUTRAL` signals suppress bid placement entirely; only asks are quoted to work down any existing inventory.
- The momentum classifier runs OLS regression on Luno daily candles — no external scanner dependency required.
- Candle data is cached locally under `data/candles/` (12-hour TTL by default).
- State (inventory, realized PnL, fills) persists across restarts in `data/state/bot_state.json`.

---

## License

MIT
