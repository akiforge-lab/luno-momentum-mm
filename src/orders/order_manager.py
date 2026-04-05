"""
Order manager — owns the lifecycle of every open order.

Design
──────
• At most one BID and one ASK active per pair at any time.
• "Sync" on each quoting tick:
    1. If an order drifts beyond cancel_replace_threshold → cancel it.
    2. If no order exists for a side that needs quoting → place a new one.
• Per-side cooldown prevents order-churn if market is noisy.
• Dry-run mode logs actions but never calls the REST API.
• Fill detection via WebSocket trade_updates callback (registered in main).
• Periodic REST reconciliation corrects any divergence from our local view.

State kept here (in memory):
  active_orders   {pair_side_key: Order}
  cancel_pending  {order_id}  — orders we've asked to cancel but not confirmed
  fill_records    [FillRecord] — appended on every detected fill
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from src.exchange.luno_client import LunoClient, Order
from src.quoting.quoting_engine import Quote
from src.utils.logger import get_logger

log = get_logger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class FillRecord:
    order_id: str
    pair: str
    side: str            # "BID" | "ASK"
    price: Decimal
    volume: Decimal      # filled volume
    fee: Decimal
    ts: float = field(default_factory=time.time)

    @property
    def notional_myr(self) -> Decimal:
        return self.price * self.volume


# ── Order manager ─────────────────────────────────────────────────────────────


class OrderManager:
    SIDE_BID = "BID"
    SIDE_ASK = "ASK"

    def __init__(
        self,
        client: LunoClient,
        dry_run: bool = True,
        order_cooldown_sec: float = 2.0,
    ) -> None:
        self._client = client
        self._dry_run = dry_run
        self._default_cooldown = order_cooldown_sec
        # pair+side → Order
        self._active: dict[str, Order] = {}
        # order_ids we've asked to cancel
        self._cancel_pending: set[str] = set()
        # cooldown: key → monotonic time after which next action is allowed
        self._cooldown_until: dict[str, float] = {}
        # fill history
        self.fills: list[FillRecord] = []
        # per-pair cooldown overrides from config
        self._pair_cooldowns: dict[str, float] = {}

    # ── Configuration ─────────────────────────────────────────────────────

    def set_pair_cooldown(self, pair: str, seconds: float) -> None:
        self._pair_cooldowns[pair] = seconds

    # ── Main sync entry point ─────────────────────────────────────────────

    async def sync(
        self,
        pair: str,
        quote: Optional[Quote],
        cancel_replace_threshold_bps: float,
    ) -> None:
        """
        Synchronise the active orders for `pair` with the desired `quote`.
        If quote is None, cancel everything for the pair.
        """
        bid_key = self._key(pair, self.SIDE_BID)
        ask_key = self._key(pair, self.SIDE_ASK)

        # No quote → cancel all
        if quote is None:
            for key in (bid_key, ask_key):
                if key in self._active:
                    await self._cancel(key)
            return

        # Sync each side
        await self._sync_side(
            key=bid_key,
            pair=pair,
            side=self.SIDE_BID,
            target_price=quote.bid_price,
            target_size=quote.bid_size,
            threshold_bps=cancel_replace_threshold_bps,
        )
        await self._sync_side(
            key=ask_key,
            pair=pair,
            side=self.SIDE_ASK,
            target_price=quote.ask_price,
            target_size=quote.ask_size,
            threshold_bps=cancel_replace_threshold_bps,
        )

    # ── Cancel all orders for a pair ─────────────────────────────────────

    async def cancel_pair(self, pair: str) -> None:
        for side in (self.SIDE_BID, self.SIDE_ASK):
            key = self._key(pair, side)
            if key in self._active:
                await self._cancel(key)
        # Also cancel via REST to be safe
        if not self._dry_run:
            await self._client.cancel_all_open_orders(pair)

    async def cancel_all(self, pairs: list[str]) -> None:
        for pair in pairs:
            await self.cancel_pair(pair)

    # ── Fill callback (called by WS trade handler) ────────────────────────

    def on_trade(self, pair: str, trade: dict) -> None:
        """
        Handle a WS trade_update. If maker_order_id matches one of ours,
        record a fill and remove the order from active tracking.
        """
        maker_oid = trade.get("maker_order_id")
        if not maker_oid:
            return

        for key, order in list(self._active.items()):
            if order.order_id == maker_oid:
                try:
                    base_filled = Decimal(str(trade.get("base", "0")))
                    counter_filled = Decimal(str(trade.get("counter", "0")))
                    price = counter_filled / base_filled if base_filled else order.price
                except Exception:
                    base_filled = order.volume
                    price = order.price

                fill = FillRecord(
                    order_id=maker_oid,
                    pair=pair,
                    side=order.side,
                    price=price,
                    volume=base_filled,
                    fee=Decimal("0"),  # fee info not in WS — reconcile later
                )
                self.fills.append(fill)
                log.info(
                    '"fill detected"',
                    extra={
                        "pair": pair,
                        "side": order.side,
                        "price": str(price),
                        "size": str(base_filled),
                        "order_id": maker_oid,
                    },
                )
                del self._active[key]
                break

    # ── Reconcile with REST ───────────────────────────────────────────────

    async def reconcile(self, pair: str) -> None:
        """
        Fetch actual open orders from REST and update our local view.
        Removes locally-tracked orders that are no longer open (filled/cancelled).
        """
        if self._dry_run:
            return
        try:
            real_orders = await self._client.get_open_orders(pair)
        except Exception as exc:
            log.warning('"reconcile failed"', extra={"pair": pair, "error": str(exc)})
            return

        real_ids = {o.order_id for o in real_orders}
        for key in list(self._active.keys()):
            if not key.startswith(pair):
                continue
            order = self._active[key]
            if order.order_id not in real_ids:
                log.info(
                    '"order gone on reconcile"',
                    extra={"pair": pair, "order_id": order.order_id, "key": key},
                )
                del self._active[key]

    # ── Queries ───────────────────────────────────────────────────────────

    def active_order_count(self) -> int:
        return len(self._active)

    def get_active(self, pair: str, side: str) -> Optional[Order]:
        return self._active.get(self._key(pair, side))

    def pop_fills_since(self, since_ts: float) -> list[FillRecord]:
        new_fills = [f for f in self.fills if f.ts >= since_ts]
        return new_fills

    # ── Private helpers ───────────────────────────────────────────────────

    async def _sync_side(
        self,
        key: str,
        pair: str,
        side: str,
        target_price: Decimal,
        target_size: Decimal,
        threshold_bps: float,
    ) -> None:
        existing = self._active.get(key)

        # Side should not be quoted
        if target_size == 0:
            if existing and existing.order_id not in self._cancel_pending:
                await self._cancel(key)
            return

        # Existing order — check if repricing needed
        if existing:
            from src.quoting.quoting_engine import QuotingEngine
            if QuotingEngine.needs_reprice(existing.price, target_price, threshold_bps):
                await self._cancel(key)
                # Next iteration will place the replacement (cooldown applies)
            return   # either left it or just cancelled; placement happens below

        # No existing order — check cooldown then place
        if self._in_cooldown(key):
            return

        await self._place(key, pair, side, target_price, target_size)

    async def _place(
        self,
        key: str,
        pair: str,
        side: str,
        price: Decimal,
        size: Decimal,
    ) -> None:
        if self._dry_run:
            fake_id = f"DRY-{uuid.uuid4().hex[:8]}"
            log.info(
                '"[DRY-RUN] would place"',
                extra={
                    "pair": pair,
                    "side": side,
                    "price": str(price),
                    "size": str(size),
                    "fake_id": fake_id,
                },
            )
            self._active[key] = Order(
                order_id=fake_id,
                pair=pair,
                side=side,
                price=price,
                volume=size,
            )
            self._set_cooldown(key, pair)
            return

        order = await self._client.place_post_only_order(pair, side, price, size)
        if order:
            self._active[key] = order
            self._set_cooldown(key, pair)
        else:
            # Order rejected (would cross) — set a short cooldown and let next
            # tick recalculate
            self._set_cooldown(key, pair, override_sec=1.0)

    async def _cancel(self, key: str) -> None:
        order = self._active.get(key)
        if not order:
            return
        self._cancel_pending.add(order.order_id)

        if self._dry_run:
            log.info(
                '"[DRY-RUN] would cancel"',
                extra={"order_id": order.order_id, "key": key},
            )
        else:
            await self._client.cancel_order(order.order_id)

        self._cancel_pending.discard(order.order_id)
        del self._active[key]
        # Set a short cooldown after cancel to avoid immediate re-placement
        self._set_cooldown(key, order.pair, override_sec=0.5)

    # ── Cooldown helpers ──────────────────────────────────────────────────

    @staticmethod
    def _key(pair: str, side: str) -> str:
        return f"{pair}_{side}"

    def _in_cooldown(self, key: str) -> bool:
        until = self._cooldown_until.get(key, 0.0)
        return time.monotonic() < until

    def _set_cooldown(
        self, key: str, pair: str = "", override_sec: Optional[float] = None
    ) -> None:
        secs = override_sec
        if secs is None:
            secs = self._pair_cooldowns.get(pair, self._default_cooldown)
        self._cooldown_until[key] = time.monotonic() + secs
