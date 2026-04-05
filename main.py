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
            rec_path = f"data/recordings/{pair}.jsonl" if record else None
            ws = LunoWebSocket(
                pair=pair,
                api_key=api_key,
                api_secret=api_secret,
                on_trade=self._order_mgr.on_trade,
                record_path=rec_path,
            )
            self._ws[pair] = ws

        # ── Market info cache ──────────────────────────────────────────────

        self._market_info: dict[str, MarketInfo] = {}
        self._fee_info: dict[str, dict] = {}

        # ── Portfolio tracking ────────────────────────────────────────────
        self._myr_balance: Decimal = Decimal("0")

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

        # Fetch initial market info and fees
        await self._refresh_market_info()
        await self._refresh_fees()

        # Sync real balances before first quote tick
        await self._sync_balances()

        # Run initial momentum classification
        await self._signals.refresh()

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
            portfolio_value = self._myr_balance
            for p in self._pairs:
                ws_p = self._ws.get(p)
                if ws_p:
                    mid_p = ws_p.orderbook.mid()
                    if mid_p:
                        portfolio_value += self._risk.inventory(p) * mid_p
            self._risk.update_portfolio_value(portfolio_value)

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
            self._push_dashboard(pair, None, None, orderbook, ws, reason)
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
            self._push_dashboard(pair, signal, None, orderbook, ws, "no_quote")
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

        self._push_dashboard(pair, signal, quote, orderbook, ws, "")

    # ── Dashboard state push ──────────────────────────────────────────────

    def _push_dashboard(self, pair, signal, quote, orderbook, ws, halt_reason):
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
        for pair in self._pairs:
            try:
                info = await self._client.get_market_info(pair)
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
            except Exception as exc:
                log.error('"market info error"', extra={"pair": pair, "error": str(exc)})

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
