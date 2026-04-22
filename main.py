"""
Luno Momentum Market-Maker — CLI entrypoint.

Usage
─────
  # Paper mode (default — safe, no real orders)
  python main.py --dry-run

  # Live trading (requires confirmed acknowledgement)
  python main.py --live --confirm-live

  # Record order-book snapshots for later replay
  python main.py --dry-run --record

  # Trade only specific pairs
  python main.py --dry-run --pairs XBTMYR,ETHMYR

  # Override config file
  python main.py --config /path/to/my_config.yaml

  # Activate global kill switch at runtime: create kill.txt in CWD

Shutdown
────────
  Ctrl-C / SIGTERM → graceful shutdown (cancels open orders if configured).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

from src.dashboard import server as _dashboard
from src.exchange.luno_client import LunoClient, MarketInfo
from src.exchange.luno_ws import LunoWebSocket
from src.orders.order_manager import OrderManager
from src.quoting.quoting_engine import PairQuoteConfig, QuotingEngine
from src.risk.risk_manager import PairRiskConfig, RiskManager
from src.signal.signal_adapter import SignalAdapter
from src.state.state_store import StateStore
from src.utils.logger import get_logger

load_dotenv()
log = get_logger("main", level=os.getenv("LOG_LEVEL", "INFO"))

KILL_FILE = Path("kill.txt")   # touch this file to trigger global kill switch


# ── Config loading ────────────────────────────────────────────────────────────


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def enabled_pairs(config: dict, filter_pairs: Optional[list[str]] = None) -> list[str]:
    pairs = [
        p for p, v in config.get("pairs", {}).items()
        if v.get("enabled", False)
    ]
    if filter_pairs:
        pairs = [p for p in pairs if p in filter_pairs]
    return pairs


# ── Main bot class ────────────────────────────────────────────────────────────


class MomentumMarketMaker:
    def __init__(
        self,
        config: dict,
        api_key: str,
        api_secret: str,
        dry_run: bool = True,
        record: bool = False,
        filter_pairs: Optional[list[str]] = None,
    ) -> None:
        self._cfg = config
        self._api_key = api_key
        self._api_secret = api_secret
        self._dry_run = dry_run
        self._record = record

        self._pairs = enabled_pairs(config, filter_pairs)
        if not self._pairs:
            log.warning('"no pairs enabled"')

        dash_cfg = config.get("dashboard", {})
        self._dash_enabled = bool(dash_cfg.get("enabled", True))
        self._dash_host = dash_cfg.get("host", "127.0.0.1")
        self._dash_port = int(dash_cfg.get("port", 8088))
        _dashboard.set_mode("DRY-RUN" if dry_run else "LIVE")

        # ── Core components ────────────────────────────────────────────────

        self._client = LunoClient(api_key, api_secret)

        sig_cfg = config.get("signal", {})
        self._signals = SignalAdapter(
            client=self._client,
            pairs=self._pairs,
            min_r2=float(sig_cfg.get("min_r2", 0.5)),
            candle_cache_ttl_hours=float(sig_cfg.get("candle_cache_ttl_hours", 12)),
            cache_dir=sig_cfg.get("cache_dir", "data/candles"),
            fallback_direction=sig_cfg.get("fallback_direction", "NEUTRAL"),
        )

        risk_cfg = config.get("risk", {})
        pair_risk_cfgs = {
            p: PairRiskConfig.from_dict(p, config["pairs"][p])
            for p in self._pairs
        }
        self._risk = RiskManager(
            pair_configs=pair_risk_cfgs,
            max_daily_loss_myr=float(risk_cfg.get("max_daily_loss_myr", 500.0)),
            max_open_orders_total=int(risk_cfg.get("max_open_orders_total", 20)),
            stale_ob_threshold_sec=float(risk_cfg.get("stale_orderbook_threshold_sec", 30.0)),
            global_kill_switch=bool(risk_cfg.get("global_kill_switch", False)),
        )

        self._order_mgr = OrderManager(
            client=self._client,
            dry_run=dry_run,
        )
        for pair in self._pairs:
            cooldown = float(config["pairs"][pair].get("order_cooldown_sec", 2.0))
            self._order_mgr.set_pair_cooldown(pair, cooldown)

        self._engine = QuotingEngine()

        self._state = StateStore()

        # ── WebSocket handlers ─────────────────────────────────────────────

        self._ws: dict[str, LunoWebSocket] = {}
        for pair in self._pairs:
            self._ws[pair] = self._create_ws(pair)

        # ── Market info cache ──────────────────────────────────────────────

        self._market_info: dict[str, MarketInfo] = {}
        self._fee_info: dict[str, dict] = {}

        # ── Portfolio tracking ────────────────────────────────────────────
        self._myr_balance: Decimal = Decimal("0")

        # ── Universe ranking ──────────────────────────────────────────────
        sig_cfg = config.get("signal", {})
        self._max_long_pairs: int = int(sig_cfg.get("max_active_long_pairs", 2))
        # Pairs currently allowed to place new bids (top-N LONG by score)
        self._bid_enabled_pairs: set[str] = set()
        # Observability: mode string per pair
        self._pair_modes: dict[str, str] = {}
        # Rank among LONG candidates (1 = highest score); 0 = not LONG
        self._pair_ranks: dict[str, int] = {}
        # Pairs excluded from universe with reasons (for dashboard/logs)
        self._excluded_pairs: dict[str, str] = {}
        # Pairs watch-only: auto-discovered but insufficient candle history
        self._watch_pairs: dict[str, MarketInfo] = {}

        # ── Timing ────────────────────────────────────────────────────────

        self._quote_interval = float(config.get("quote_interval_sec", 2.0))
        self._signal_refresh_interval = float(config.get("signal_refresh_interval_sec", 300))
        self._state_save_interval = float(config.get("state_save_interval_sec", 15))
        self._reconcile_interval = float(config.get("order_reconcile_interval_sec", 30))
        self._market_info_refresh_interval = float(config.get("market_info_refresh_interval_sec", 3600))
        self._fee_refresh_interval = float(config.get("fee_refresh_interval_sec", 3600))

        # ── Shutdown ───────────────────────────────────────────────────────

        self._stop = asyncio.Event()
        self._cancel_on_shutdown = bool(risk_cfg.get("cancel_on_shutdown", True))

        # ── Timestamps for periodic tasks ──────────────────────────────────

        self._last_signal_refresh: float = 0.0
        self._last_state_save: float = 0.0
        self._last_reconcile: float = 0.0
        self._last_market_info_refresh: float = 0.0
        self._last_fee_refresh: float = 0.0
        self._last_balance_sync: float = 0.0

    def _create_ws(self, pair: str) -> LunoWebSocket:
        rec_path = f"data/recordings/{pair}.jsonl" if self._record else None
        return LunoWebSocket(
            pair=pair,
            api_key=self._api_key,
            api_secret=self._api_secret,
            on_trade=self._order_mgr.on_trade,
            record_path=rec_path,
        )

    # ── Startup / shutdown ─────────────────────────────────────────────────

    async def start(self) -> None:
        log.info(
            '"bot starting"',
            extra={"dry_run": self._dry_run, "pairs": self._pairs},
        )

        # Load persisted state
        saved = self._state.load()
        if saved.get("risk"):
            self._risk.load_state_dict(saved["risk"])
            log.info('"state restored from disk"')

        # Discover universe and fetch all market info in a single API call.
        # If auto_discover is enabled, also expands self._pairs with new MYR pairs.
        await self._discover_universe()

        # Validate pairs against live Luno markets — drop unsupported/inactive
        await self._validate_active_pairs()

        await self._refresh_fees()

        # Cancel any pre-existing exchange orders before placing new quotes
        await self._cancel_startup_orders()

        # Sync real balances before first quote tick
        await self._sync_balances()

        # Run initial momentum classification and rank bid-enabled pairs
        await self._signals.refresh()
        self._update_bid_enabled_pairs()

    async def shutdown(self) -> None:
        log.info('"shutting down"')
        self._stop.set()
        for ws in self._ws.values():
            ws.stop()

        if self._cancel_on_shutdown and not self._dry_run:
            log.info('"cancelling all open orders"')
            tasks = [self._order_mgr.cancel_pair(p) for p in self._pairs]
            await asyncio.gather(*tasks, return_exceptions=True)
        elif self._dry_run:
            log.info('"[DRY-RUN] would cancel all open orders on shutdown"')

        # Final state save
        self._save_state()

        await self._client.close()
        log.info('"shutdown complete"')

    # ── Main run loop ──────────────────────────────────────────────────────

    async def run(self) -> None:
        await self.start()

        # Launch WS tasks
        ws_tasks = [asyncio.create_task(ws.run()) for ws in self._ws.values()]

        # Brief pause to let WS connections get their first snapshot
        await asyncio.sleep(2.0)

        tasks = [
            asyncio.create_task(self._quote_loop()),
            asyncio.create_task(self._periodic_task_loop()),
            *ws_tasks,
        ]
        if self._dash_enabled:
            tasks.append(
                asyncio.create_task(
                    _dashboard.run(host=self._dash_host, port=self._dash_port)
                )
            )

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    # ── Quoting loop ──────────────────────────────────────────────────────

    async def _quote_loop(self) -> None:
        while not self._stop.is_set():
            t0 = time.monotonic()

            # Check kill file (runtime emergency stop)
            if KILL_FILE.exists():
                log.warning('"kill file detected, activating global kill switch"')
                self._risk.set_global_kill(True)

            # Compute portfolio value: MYR balance + sum(inventory × mid) for all pairs
            total_inv_value = Decimal("0")
            for p in self._pairs:
                ws_p = self._ws.get(p)
                if ws_p:
                    mid_p = ws_p.orderbook.mid()
                    if mid_p:
                        total_inv_value += self._risk.inventory(p) * mid_p
            portfolio_value = self._myr_balance + total_inv_value
            self._risk.update_portfolio_value(portfolio_value)
            self._risk.update_total_inventory_value(total_inv_value)

            for pair in self._pairs:
                try:
                    await self._quote_pair(pair)
                except Exception as exc:
                    log.error(
                        '"quote loop error"',
                        extra={"pair": pair, "error": str(exc)},
                    )

            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, self._quote_interval - elapsed)
            await asyncio.sleep(sleep_for)

    async def _quote_pair(self, pair: str) -> None:
        ws = self._ws.get(pair)
        if ws is None:
            return
        orderbook = ws.orderbook
        market = self._market_info.get(pair)
        if market is None:
            return

        # ── Risk check ─────────────────────────────────────────────────────
        ok, reason = self._risk.can_quote(
            pair, orderbook, self._order_mgr.active_order_count()
        )
        if not ok:
            if reason not in ("pair_disabled",):
                log.warning('"quote suppressed"', extra={"pair": pair, "reason": reason})
            await self._order_mgr.cancel_pair(pair)
            self._risk.record_quote_tick(pair, False)
            self._push_dashboard(pair, None, None, orderbook, ws, reason,
                                  self._pair_modes.get(pair, "inactive"))
            return

        # ── Get signal ─────────────────────────────────────────────────────
        signal = self._signals.get(pair)
        inventory = self._risk.inventory(pair)

        pair_dict = self._cfg["pairs"][pair]
        qcfg = PairQuoteConfig.from_dict(pair, pair_dict)

        # ── Update dynamic inventory limit ─────────────────────────────────
        mid_now = orderbook.mid()
        if mid_now:
            self._risk.update_dynamic_max(pair, mid_now)
        qcfg.max_inventory_base = self._risk.effective_max_inventory(pair)

        # ── Compute quote ──────────────────────────────────────────────────
        quote = self._engine.compute(orderbook, signal, inventory, qcfg, market)

        if quote is None:
            await self._order_mgr.sync(pair, None, qcfg.cancel_replace_threshold_bps)
            self._risk.record_quote_tick(pair, False)
            self._push_dashboard(pair, signal, None, orderbook, ws, "no_quote",
                                  self._pair_modes.get(pair, "standby"))
            return

        # ── Pre-order risk checks ──────────────────────────────────────────
        if quote.bid_size > 0:
            ok_bid, reason_bid = self._risk.can_place_bid(pair, quote.bid_price, quote.bid_size)
            if not ok_bid:
                quote.bid_size = Decimal("0")
                log.debug('"bid suppressed"', extra={"pair": pair, "reason": reason_bid})

        if quote.ask_size > 0:
            ok_ask, reason_ask = self._risk.can_place_ask(pair, quote.ask_size)
            if not ok_ask:
                quote.ask_size = Decimal("0")
                log.debug('"ask suppressed"', extra={"pair": pair, "reason": reason_ask})

        # ── Rank-based bid suppression ─────────────────────────────────────
        # Even if signal is LONG and risk permits, suppress bids for pairs
        # outside the top-N cap. Asks are always allowed to reduce inventory.
        if quote.bid_size > 0 and pair not in self._bid_enabled_pairs:
            quote.bid_size = Decimal("0")
            log.debug(
                '"bid suppressed by rank"',
                extra={"pair": pair, "mode": self._pair_modes.get(pair, "standby")},
            )

        # Effective mode for dashboard: refine to inventory-only when applicable
        pair_mode = self._pair_modes.get(pair, "standby")
        if pair not in self._bid_enabled_pairs and inventory >= market.min_volume:
            pair_mode = "inventory-only"

        both_quoted = quote.bid_size > 0 and quote.ask_size > 0
        self._risk.record_quote_tick(pair, both_quoted)

        log.debug(
            '"quote computed"',
            extra={
                "pair": pair,
                "signal": signal.direction,
                "mid": str(quote.mid),
                "bid": f"{quote.bid_price} x {quote.bid_size}",
                "ask": f"{quote.ask_price} x {quote.ask_size}",
                "spread_bps": f"{quote.spread_bps:.1f}",
            },
        )

        # ── Sync orders ────────────────────────────────────────────────────
        await self._order_mgr.sync(pair, quote, qcfg.cancel_replace_threshold_bps)

        # ── Dry-run fill simulation ────────────────────────────────────────
        if self._dry_run:
            bb = orderbook.best_bid()
            ba = orderbook.best_ask()
            if bb and ba:
                _ts = int(time.time() * 1000)
                new_fills = _simulate_fills_dryrun(
                    pair, self._order_mgr, bb, ba, _ts
                )
                for fill in new_fills:
                    self._risk.process_fill(fill)

        self._push_dashboard(pair, signal, quote, orderbook, ws, "", pair_mode)

    # ── Dashboard state push ──────────────────────────────────────────────

    def _push_dashboard(self, pair, signal, quote, orderbook, ws, halt_reason,
                        pair_mode: str = "standby"):
        if not self._dash_enabled:
            return
        m = self._risk.metrics(pair)
        mid = orderbook.mid()
        inventory = self._risk.inventory(pair)
        pair_dict = self._cfg["pairs"].get(pair, {})
        _dashboard.update_pair(pair, {
            "pair": pair,
            # Signal
            "signal":          signal.direction if signal else "NEUTRAL",
            "slope_ann_pct":   signal.slope_ann_pct if signal else 0.0,
            "r2":              signal.r2 if signal else 0.0,
            "momentum_score":  signal.momentum_score if signal else 0.0,
            # Market
            "mid":             float(mid) if mid else None,
            "book_spread_bps": quote.book_spread_bps if quote else None,
            # Quote prices / sizes
            "reservation_mid": float(quote.reservation_mid) if quote else None,
            "bid_price":       float(quote.bid_price) if quote else None,
            "bid_size":        float(quote.bid_size) if quote else None,
            "ask_price":       float(quote.ask_price) if quote else None,
            "ask_size":        float(quote.ask_size) if quote else None,
            "spread_bps":      quote.spread_bps if quote else None,
            # Inventory
            "inventory":       float(inventory),
            "target_inventory":float(quote.target_inventory) if quote else 0.0,
            "max_inventory":   float(self._risk.effective_max_inventory(pair)),
            "dynamic_max_inventory": float(self._risk.effective_max_inventory(pair)),
            "allocation_pct":  float(self._cfg["pairs"].get(pair, {}).get("allocation_pct", 0.0)),
            "portfolio_value_myr": float(self._risk.portfolio_value_myr),
            # PnL
            "realized_pnl_myr":float(m.realized_pnl_myr) if m else 0.0,
            "mtm_pnl_myr":     float(self._risk.mtm_pnl(pair, mid)) if mid else 0.0,
            # Quote-decision breakdown
            "inv_ratio":       quote.inv_ratio if quote else None,
            "inv_shift_bps":   quote.inv_shift_bps if quote else None,
            "mom_shift_bps":   quote.mom_shift_bps if quote else None,
            "half_bps":        quote.half_bps if quote else None,
            # Universe / ranking
            "pair_mode":       pair_mode,
            "bid_enabled":     pair in self._bid_enabled_pairs,
            "bid_rank":        self._pair_ranks.get(pair, 0),
            # Status
            "ws_connected":    ws.is_connected,
            "ws_age_sec":      orderbook.last_update_age_sec(),
            "quoting":         quote is not None and (quote.bid_size > 0 or quote.ask_size > 0),
            "halt_reason":     halt_reason,
            "active_bid":      self._order_mgr.get_active(pair, "BID") is not None,
            "active_ask":      self._order_mgr.get_active(pair, "ASK") is not None,
            "updated_at":      time.time(),
        })

    # ── Periodic tasks ─────────────────────────────────────────────────────

    async def _periodic_task_loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()

            if now - self._last_signal_refresh >= self._signal_refresh_interval:
                await self._signals.refresh()
                self._update_bid_enabled_pairs()
                self._last_signal_refresh = now

            if now - self._last_state_save >= self._state_save_interval:
                self._save_state()
                self._last_state_save = now

            if now - self._last_reconcile >= self._reconcile_interval:
                await self._reconcile_orders()
                self._last_reconcile = now

            if now - self._last_market_info_refresh >= self._market_info_refresh_interval:
                await self._refresh_market_info()
                self._last_market_info_refresh = now

            if now - self._last_fee_refresh >= self._fee_refresh_interval:
                await self._refresh_fees()
                self._last_fee_refresh = now

            if now - self._last_balance_sync >= 60:
                await self._sync_balances()
                self._last_balance_sync = now

            self._risk.log_status()

            await asyncio.sleep(5.0)

    async def _reconcile_orders(self) -> None:
        for pair in self._pairs:
            try:
                await self._order_mgr.reconcile(pair)
            except Exception as exc:
                log.warning('"reconcile error"', extra={"pair": pair, "error": str(exc)})

    async def _refresh_market_info(self) -> None:
        """Refresh market info for all active pairs — single bulk API call."""
        try:
            all_infos = await self._client.get_all_market_infos()
        except Exception as exc:
            log.error('"market info refresh error"', extra={"error": str(exc)})
            return
        for pair in self._pairs:
            info = all_infos.get(pair)
            if info is None:
                log.error('"market info missing"', extra={"pair": pair})
                continue
            self._market_info[pair] = info
            log.info(
                '"market info loaded"',
                extra={
                    "pair": pair,
                    "min_vol": str(info.min_volume),
                    "price_scale": info.price_scale,
                    "status": info.status,
                },
            )

    async def _fetch_and_count_candles(self, pair: str) -> int:
        """
        Return the number of valid daily closes available for *pair*.

        Checks the SignalAdapter candle cache first; only calls the Luno API
        if the cache is absent or stale.  Writes fetched candles to cache in
        the same format used by SignalAdapter so the two never double-fetch on
        the same startup.
        """
        sig_cfg = self._cfg.get("signal", {})
        cache_dir = sig_cfg.get("cache_dir", "data/candles")
        cache_ttl = float(sig_cfg.get("candle_cache_ttl_hours", 12)) * 3600

        cache_path = Path(cache_dir) / f"{pair}.json"
        now = time.time()

        # Serve from cache if fresh enough
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if now - cached.get("updated_at", 0) < cache_ttl:
                    return len(cached.get("closes", []))
            except Exception:
                pass  # corrupt cache — fall through to API fetch

        # Fetch from Luno
        lookback_days = int(sig_cfg.get("lookback_days", SignalAdapter.LOOKBACK_DAYS))
        since_ms = int((now - lookback_days * 86400) * 1000)
        try:
            raw = await self._client.get_candles(pair, since_ms=since_ms, duration_sec=86400)
        except Exception as exc:
            log.warning(
                '"candle fetch failed during discovery"',
                extra={"pair": pair, "error": str(exc)},
            )
            return 0

        if not raw:
            return 0

        raw_sorted = sorted(raw, key=lambda c: int(c["timestamp"]))
        closes_with_ts = [
            (int(c["timestamp"]), float(c["close"]))
            for c in raw_sorted
            if float(c["close"]) > 0
        ]

        # Write to cache (same format as SignalAdapter._save_cache)
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(".tmp")
        payload = {"updated_at": now, "closes": closes_with_ts}
        try:
            tmp_path.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp_path, cache_path)
        except Exception as exc:
            log.warning(
                '"candle cache write failed"',
                extra={"pair": pair, "error": str(exc)},
            )

        log.info('"candles fetched for discovery"', extra={"pair": pair, "n": len(closes_with_ts)})
        return len(closes_with_ts)

    async def _discover_universe(self) -> None:
        """
        Single-call market discovery. Always:
          - Fetches all market infos from Luno in one API call.
          - Pre-populates self._market_info for all pre-configured pairs.
        If universe.auto_discover is enabled, also:
          - Scans all ACTIVE MYR pairs not already in self._pairs.
          - Applies exclusion list and config_disabled filter.
          - Injects default config for newly discovered pairs.
          - Creates WS handlers, registers with risk manager.
          - Logs each pair's fate (auto-discovered / excluded / reason).
        """
        uni_cfg = self._cfg.get("universe", {})
        auto_discover = bool(uni_cfg.get("auto_discover", False))
        exclude_set = set(uni_cfg.get("exclude_pairs", []))
        default_params: dict = uni_cfg.get("default_params", {})
        q_mult = Decimal(str(default_params.get("quote_size_multiplier", "1")))
        hc_mult = Decimal(str(default_params.get("hard_cap_multiplier", "200")))
        sig_cfg = self._cfg.get("signal", {})
        min_closes = int(sig_cfg.get("min_closes", SignalAdapter.MIN_CLOSES))

        # Single bulk API call
        try:
            all_markets = await self._client.get_all_market_infos()
        except Exception as exc:
            log.error('"universe discovery failed — falling back to per-pair fetch"',
                      extra={"error": str(exc)})
            await self._refresh_market_info()
            return

        # Always: populate market info for pre-configured pairs
        for pair in list(self._pairs):
            info = all_markets.get(pair)
            if info is not None:
                self._market_info[pair] = info
                log.info(
                    '"market info loaded"',
                    extra={
                        "pair": pair,
                        "min_vol": str(info.min_volume),
                        "price_scale": info.price_scale,
                        "status": info.status,
                    },
                )
            else:
                log.error('"market info missing"', extra={"pair": pair})

        if not auto_discover:
            return

        pre_existing = set(self._pairs)
        added: list[str] = []
        excluded: dict[str, str] = {}

        for pair in sorted(all_markets.keys()):
            if not pair.endswith("MYR"):
                continue

            if pair in pre_existing:
                continue  # already configured

            market = all_markets[pair]

            # Exclusion checks (logged below)
            if pair in exclude_set:
                excluded[pair] = "explicit_exclude"
                continue

            cfg_pair = self._cfg.get("pairs", {}).get(pair, {})
            if cfg_pair.get("enabled") is False:
                excluded[pair] = "config_disabled"
                continue

            if market.status != "ACTIVE":
                excluded[pair] = f"not_active:{market.status}"
                continue

            # History gate: only promote if momentum can be computed.
            # A pair with fewer than min_closes daily closes stays watch-only
            # until it accumulates enough history. Promotion happens automatically
            # on the next bot restart once the threshold is crossed.
            n_closes = await self._fetch_and_count_candles(pair)
            if n_closes < min_closes:
                excluded[pair] = "watch_only"
                self._watch_pairs[pair] = market
                log.info(
                    '"pair watch-only: insufficient candle history"',
                    extra={"pair": pair, "n_closes": n_closes, "required": min_closes},
                )
                continue

            # Build injected default config
            quote_size = market.min_volume * q_mult
            hard_cap = market.min_volume * hc_mult
            base_asset = pair[:-3]  # strip "MYR"

            injected: dict = {
                "enabled": True,
                "hl_symbol": base_asset,
                "quote_size_base": str(quote_size),
                "max_inventory_base": str(quote_size),
                "hard_cap_units": str(hard_cap),
            }
            for k, v in default_params.items():
                if k not in ("quote_size_multiplier", "hard_cap_multiplier"):
                    injected.setdefault(k, v)

            # Merge: explicit config keys win over defaults
            if pair not in self._cfg["pairs"]:
                self._cfg["pairs"][pair] = injected
            else:
                for k, v in injected.items():
                    if k not in self._cfg["pairs"][pair]:
                        self._cfg["pairs"][pair][k] = v

            # Register pair
            self._pairs.append(pair)
            self._market_info[pair] = market
            self._ws[pair] = self._create_ws(pair)
            prc = PairRiskConfig.from_dict(pair, self._cfg["pairs"][pair])
            self._risk.add_pair(prc)
            cooldown = float(self._cfg["pairs"][pair].get("order_cooldown_sec", 2.0))
            self._order_mgr.set_pair_cooldown(pair, cooldown)
            added.append(pair)

            log.info(
                '"pair auto-discovered"',
                extra={
                    "pair": pair,
                    "quote_size": str(quote_size),
                    "min_volume": str(market.min_volume),
                    "max_notional": str(default_params.get("max_notional_myr", 500)),
                },
            )

        # Update signal adapter with the now-complete pair list
        self._signals.set_pairs(self._pairs)

        # Store for dashboard
        self._excluded_pairs = excluded

        # Layer 3: validate accumulation pairs — all must be in the trading set.
        shock_cfg = self._cfg.get("shock_regime", {})
        accum_pairs: list[str] = shock_cfg.get("accumulation_pairs", [])
        if accum_pairs:
            trading_set = set(self._pairs)
            missing = [p for p in accum_pairs if p not in trading_set]
            if missing:
                log.warning(
                    '"accumulation pairs not in trading set — shock-regime would be unsafe"',
                    extra={"missing": missing},
                )

        # Summary log
        myr_total = len([p for p in all_markets if p.endswith("MYR")])
        watch_only_pairs = sorted(p for p, r in excluded.items() if r == "watch_only")
        log.info(
            '"universe discovery complete"',
            extra={
                "layer1_exchange_discovered": myr_total,
                "layer2_trading": len(self._pairs),
                "layer2_watch_only": len(watch_only_pairs),
                "layer3_accumulation": accum_pairs,
                "newly_added": sorted(added),
                "excluded_other": sorted(p for p, r in excluded.items() if r != "watch_only"),
            },
        )
        for pair, reason in sorted(excluded.items()):
            log.info(
                '"pair excluded from universe"',
                extra={"pair": pair, "reason": reason},
            )

        # Update dashboard with excluded pairs and accumulation config
        _dashboard.update_universe(excluded)
        _dashboard.update_accumulation_pairs(accum_pairs)

    async def _refresh_fees(self) -> None:
        for pair in self._pairs:
            try:
                fee = await self._client.get_fee_info(pair)
                self._fee_info[pair] = {
                    "maker": fee.maker_fee,
                    "taker": fee.taker_fee,
                }
                # Warn if base spread is less than 2× maker fee
                pair_cfg = self._cfg["pairs"].get(pair, {})
                spread_bps = float(pair_cfg.get("base_spread_bps", 0))
                maker_bps = float(fee.maker_fee) * 10_000
                if spread_bps < 2 * maker_bps:
                    log.warning(
                        '"spread may be unprofitable"',
                        extra={
                            "pair": pair,
                            "spread_bps": spread_bps,
                            "min_needed_bps": 2 * maker_bps,
                        },
                    )
            except Exception as exc:
                log.warning('"fee info error"', extra={"pair": pair, "error": str(exc)})

    async def _sync_balances(self) -> None:
        """Sync inventory from REST balance (truth source). Runs in all modes — read-only."""
        try:
            balances = await self._client.get_balances()
            self._myr_balance = balances.get("MYR", Decimal("0"))
            for pair in self._pairs:
                base = _base_asset(pair)
                bal = balances.get(base, Decimal("0"))
                self._risk.sync_inventory(pair, bal)
        except Exception as exc:
            log.warning('"balance sync error"', extra={"error": str(exc)})

    def _update_bid_enabled_pairs(self) -> None:
        """
        Rank active pairs by momentum score and determine which ones may place
        new bids. Called after every signal refresh.

        Mode strings (stored in self._pair_modes, shown in dashboard/logs):
          "bid-enabled"     — LONG, ranked within max_active_long_pairs cap
          "rank-cutoff"     — LONG but exceeded cap; asks still allowed
          "not-long"        — SHORT or NEUTRAL; asks still allowed
          "below-min-size"  — configured quote_size_base < pair min_volume
          "inactive"        — market info missing (failed validation)
        """
        modes: dict[str, str] = {}
        ranks: dict[str, int] = {}
        long_candidates: list[tuple[float, str]] = []  # (score, pair)

        for pair in self._pairs:
            market = self._market_info.get(pair)
            if market is None:
                modes[pair] = "inactive"
                continue

            # Eligibility: configured quote size must meet exchange minimum
            pair_cfg = self._cfg["pairs"].get(pair, {})
            try:
                quote_size = Decimal(str(pair_cfg.get("quote_size_base", "0")))
            except Exception:
                quote_size = Decimal("0")
            if quote_size < market.min_volume:
                modes[pair] = "below-min-size"
                log.warning(
                    '"pair below min quote size"',
                    extra={"pair": pair, "quote_size": str(quote_size), "min_volume": str(market.min_volume)},
                )
                continue

            sig = self._signals.get(pair)
            if sig.direction == "LONG":
                long_candidates.append((sig.momentum_score, pair))
                modes[pair] = "long-candidate"   # tentative; updated below
            else:
                modes[pair] = "not-long"

        # Rank LONG candidates by score descending; assign bid-enabled up to cap
        long_candidates.sort(key=lambda x: x[0], reverse=True)
        new_bid_enabled: set[str] = set()
        for rank_idx, (score, pair) in enumerate(long_candidates):
            rank_1based = rank_idx + 1
            ranks[pair] = rank_1based
            if rank_idx < self._max_long_pairs:
                new_bid_enabled.add(pair)
                modes[pair] = "bid-enabled"
            else:
                modes[pair] = "rank-cutoff"

        self._bid_enabled_pairs = new_bid_enabled
        self._pair_modes = modes
        self._pair_ranks = ranks

        log.info(
            '"universe ranked"',
            extra={
                "bid_enabled": sorted(new_bid_enabled),
                "long_ranked": [f"{p}({s:.1f})" for s, p in long_candidates],
                "max_long_cap": self._max_long_pairs,
            },
        )
        _dashboard.update_universe(self._excluded_pairs)

    async def _validate_active_pairs(self) -> None:
        """
        Remove from self._pairs any pair whose market info is missing or
        whose trading_status is not ACTIVE. Runs once at startup after
        _refresh_market_info().
        """
        valid = []
        for pair in list(self._pairs):
            info = self._market_info.get(pair)
            if info is None:
                log.warning('"pair not found on Luno, skipping"', extra={"pair": pair})
                continue
            if info.status != "ACTIVE":
                log.warning(
                    '"pair not ACTIVE, skipping"',
                    extra={"pair": pair, "status": info.status},
                )
                continue
            valid.append(pair)
        removed = set(self._pairs) - set(valid)
        if removed:
            log.info('"pairs removed after validation"', extra={"removed": list(removed)})
        self._pairs = valid
        # Re-align dependent structures
        for pair in removed:
            self._ws.pop(pair, None)

    async def _cancel_startup_orders(self) -> None:
        """
        Cancel all open exchange orders for enabled pairs before placing any
        new quotes. Handles stale orders from prior sessions. Dry-run logs
        without cancelling.
        """
        if self._dry_run:
            log.info('"[DRY-RUN] startup order cleanup — would cancel all open orders"',
                     extra={"pairs": self._pairs})
            return
        total = 0
        for pair in self._pairs:
            try:
                orders = await self._client.get_open_orders(pair)
                for order in orders:
                    ok = await self._client.cancel_order(order.order_id)
                    log.info(
                        '"startup cancel"',
                        extra={
                            "pair": pair,
                            "order_id": order.order_id,
                            "side": order.side,
                            "price": str(order.price),
                            "size": str(order.volume),
                            "success": ok,
                        },
                    )
                    total += 1
            except Exception as exc:
                log.warning(
                    '"startup cancel error"', extra={"pair": pair, "error": str(exc)}
                )
        log.info('"startup order cleanup complete"', extra={"cancelled": total})

    def _save_state(self) -> None:
        risk_state = self._risk.to_state_dict()
        fills = self._state.fills_to_dicts(self._order_mgr.fills)
        self._state.save(risk_state, fills)


# ── Dry-run fill simulation helper ────────────────────────────────────────────


def _simulate_fills_dryrun(
    pair: str,
    order_mgr: OrderManager,
    best_bid: Decimal,
    best_ask: Decimal,
    ts_ms: int,
) -> list:
    """
    In dry-run, naively simulate fills:
    - Our bid fills if best_ask crosses down to our bid price
    - Our ask fills if best_bid rises to our ask price
    Returns FillRecord-like objects.
    """
    from src.orders.order_manager import FillRecord

    fills = []
    for side in ("BID", "ASK"):
        order = order_mgr.get_active(pair, side)
        if order is None:
            continue
        if side == "BID" and best_ask <= order.price:
            fills.append(
                FillRecord(
                    order_id=order.order_id,
                    pair=pair,
                    side="BID",
                    price=order.price,
                    volume=order.volume,
                    fee=Decimal("0"),
                    ts=ts_ms / 1000,
                )
            )
            # Remove from active
            from src.orders.order_manager import OrderManager as OM
            key = OM._key(pair, "BID")
            order_mgr._active.pop(key, None)
        elif side == "ASK" and best_bid >= order.price:
            fills.append(
                FillRecord(
                    order_id=order.order_id,
                    pair=pair,
                    side="ASK",
                    price=order.price,
                    volume=order.volume,
                    fee=Decimal("0"),
                    ts=ts_ms / 1000,
                )
            )
            from src.orders.order_manager import OrderManager as OM
            key = OM._key(pair, "ASK")
            order_mgr._active.pop(key, None)
    return fills


# ── Utility ───────────────────────────────────────────────────────────────────


def _base_asset(luno_pair: str) -> str:
    """Extract base asset from Luno pair code, e.g. XBTMYR → XBT."""
    # Known suffixes
    for suffix in ("MYR", "USDC", "USDT", "EUR", "GBP", "IDR", "NGN", "ZAR"):
        if luno_pair.endswith(suffix):
            return luno_pair[: -len(suffix)]
    return luno_pair[:3]


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Luno Momentum Market-Maker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Paper mode: connect to market data but place NO real orders (default)",
    )
    mode.add_argument(
        "--live",
        action="store_true",
        help="Enable live order placement. MUST be combined with --confirm-live",
    )
    mode.add_argument(
        "--no-auth",
        action="store_true",
        help="Start the dashboard only — no credentials required, no exchange connections, stub data",
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required acknowledgement when using --live",
    )
    parser.add_argument(
        "--config",
        default=os.getenv("CONFIG_FILE", "config.yaml"),
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--pairs",
        help="Comma-separated list of pairs to trade, e.g. XBTMYR,ETHMYR",
    )
    parser.add_argument(
        "--record",
        action="store_true",
        help="Record WS order-book snapshots to data/recordings/<pair>.jsonl",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


async def _async_main_no_auth(args: argparse.Namespace) -> None:
    """
    Offline dashboard mode — no credentials, no exchange connections.
    Serves the dashboard with clearly labeled stub data.
    """
    config = load_config(args.config)
    dash_cfg = config.get("dashboard", {})
    host = dash_cfg.get("host", "127.0.0.1")
    port = int(dash_cfg.get("port", 8088))

    _dashboard.set_mode("OFFLINE")
    await _dashboard.run(host=host, port=port, offline=True)


async def _async_main(args: argparse.Namespace) -> None:
    config = load_config(args.config)

    api_key = os.getenv("LUNO_API_KEY", "")
    api_secret = os.getenv("LUNO_API_SECRET", "")
    if not api_key or not api_secret:
        log.error('"LUNO_API_KEY and LUNO_API_SECRET must be set in .env"')
        sys.exit(1)

    dry_run = not args.live
    filter_pairs = [p.strip() for p in args.pairs.split(",")] if args.pairs else None

    bot = MomentumMarketMaker(
        config=config,
        api_key=api_key,
        api_secret=api_secret,
        dry_run=dry_run,
        record=args.record,
        filter_pairs=filter_pairs,
    )

    loop = asyncio.get_running_loop()

    def _sig_handler(*_):
        log.info('"signal received, shutting down"')
        bot._stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            # Windows — fall back to KeyboardInterrupt
            pass

    await bot.run()


def main() -> None:
    args = _parse_args()

    # Set log level from CLI
    import logging
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    dry_run = not args.live

    if args.no_auth:
        print("\n" + "=" * 60)
        print("  OFFLINE / STUB MODE - dashboard only, no credentials")
        print("=" * 60 + "\n")
        try:
            asyncio.run(_async_main_no_auth(args))
        except KeyboardInterrupt:
            pass
        return

    if args.live:
        if not args.confirm_live:
            print(
                "\n[ERROR] --live requires --confirm-live. "
                "Only use live mode after testing in dry-run.\n"
            )
            sys.exit(1)
        print("\n" + "=" * 60)
        print("  LIVE MODE - real orders will be placed on Luno")
        print("=" * 60 + "\n")
    else:
        print("\n" + "=" * 60)
        print("  DRY-RUN MODE - no real orders will be placed")
        print("=" * 60 + "\n")

    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
