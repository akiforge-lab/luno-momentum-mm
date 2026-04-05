"""
Quoting engine — computes target bid/ask prices and sizes.

Quoting model
─────────────
  1. Mid price from order book (best bid + best ask) / 2.

  2. Reservation price  — shift mid to account for:
       a) Inventory skew: if long (inventory > target), lower res_price to
          incentivise selling; if short of target, raise to incentivise buying.
       b) Momentum skew: LONG signal raises res_price (bids more aggressive,
          asks wider); SHORT signal lowers it.

  3. Spread around res_price — configurable half-spread (base_spread_bps / 2).

  4. Volatility widening — if the current book spread exceeds
     vol_widening_threshold_bps, widen our half-spread proportionally
     (capped at max_vol_widening_factor).

  5. Post-only enforcement — bid must be strictly below best_ask,
     ask strictly above best_bid, adjusted by one tick if needed.

  6. Quote sizes — configurable per pair, zeroed out on the side
     we don't want to quote (e.g. no bids on STRONG SHORT when
     quote_only_with_signal is enabled).

Spot-specific note
──────────────────
  In spot trading you cannot go net short. "SHORT" signal means:
  * Target inventory = 0 (sell down any existing base position aggressively).
  * Quote asks more aggressively to dump inventory.
  * Reduce or skip bid quoting to avoid accumulating more.
  * Reserve the taker path only for emergency inventory reduction.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Optional

from src.exchange.luno_client import MarketInfo
from src.exchange.luno_ws import OrderBook
from src.signal.signal_adapter import Signal
from src.utils.logger import get_logger

log = get_logger(__name__)


# ── Config dataclass for one pair ────────────────────────────────────────────


@dataclass
class PairQuoteConfig:
    pair: str
    base_spread_bps: float
    min_spread_bps: float
    momentum_long_skew_bps: float
    momentum_short_skew_bps: float
    inventory_skew_factor: float        # 0 = no skew, 2 = aggressive mean-revert
    cancel_replace_threshold_bps: float
    quote_size_base: Decimal
    max_inventory_base: Decimal
    target_inventory_long_ratio: float   # fraction of max_inventory to target when LONG
    target_inventory_neutral_ratio: float
    target_inventory_short_ratio: float
    quote_only_with_signal: bool
    volatility_widening: bool
    vol_widening_threshold_bps: float
    max_vol_widening_factor: float

    @classmethod
    def from_dict(cls, pair: str, d: dict) -> "PairQuoteConfig":
        return cls(
            pair=pair,
            base_spread_bps=float(d["base_spread_bps"]),
            min_spread_bps=float(d["min_spread_bps"]),
            momentum_long_skew_bps=float(d["momentum_long_skew_bps"]),
            momentum_short_skew_bps=float(d["momentum_short_skew_bps"]),
            inventory_skew_factor=float(d["inventory_skew_factor"]),
            cancel_replace_threshold_bps=float(d["cancel_replace_threshold_bps"]),
            quote_size_base=Decimal(str(d["quote_size_base"])),
            max_inventory_base=Decimal(str(d["max_inventory_base"])),
            target_inventory_long_ratio=float(d.get("target_inventory_long_ratio", 0.5)),
            target_inventory_neutral_ratio=float(d.get("target_inventory_neutral_ratio", 0.25)),
            target_inventory_short_ratio=float(d.get("target_inventory_short_ratio", 0.0)),
            quote_only_with_signal=bool(d.get("quote_only_with_signal", False)),
            volatility_widening=bool(d.get("volatility_widening", True)),
            vol_widening_threshold_bps=float(d.get("vol_widening_threshold_bps", 50.0)),
            max_vol_widening_factor=float(d.get("max_vol_widening_factor", 3.0)),
        )


# ── Output dataclass ──────────────────────────────────────────────────────────


@dataclass
class Quote:
    pair: str
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    mid: Decimal
    reservation_mid: Decimal
    signal: str
    spread_bps: float
    book_spread_bps: float
    # Decision breakdown (for dashboard / observability)
    target_inventory: Decimal = Decimal("0")
    inv_ratio: float = 0.0
    inv_shift_bps: float = 0.0
    mom_shift_bps: float = 0.0
    half_bps: float = 0.0


# ── Engine ────────────────────────────────────────────────────────────────────


class QuotingEngine:
    def compute(
        self,
        orderbook: OrderBook,
        signal: Signal,
        inventory: Decimal,           # current base position (≥ 0 in spot)
        config: PairQuoteConfig,
        market: MarketInfo,
    ) -> Optional[Quote]:
        """
        Compute target quotes. Returns None if quoting should be skipped
        (bad data, market not active, quote_only_with_signal + NEUTRAL, etc.)
        """

        # ── Guard checks ─────────────────────────────────────────────────

        if market.status != "ACTIVE":
            log.warning(
                '"market not active"', extra={"pair": config.pair, "status": market.status}
            )
            return None

        if config.quote_only_with_signal and signal.direction == "NEUTRAL":
            return None

        bb = orderbook.best_bid()
        ba = orderbook.best_ask()
        if bb is None or ba is None or bb <= 0 or ba <= 0:
            return None
        if ba <= bb:
            return None

        mid = (bb + ba) / 2
        book_spread_bps = float((ba - bb) / mid * 10000)

        # ── Inventory ratio ───────────────────────────────────────────────

        target_inv = self._target_inventory(signal.direction, config)
        inv_deviation = inventory - target_inv
        if config.max_inventory_base > 0:
            inv_ratio = float(inv_deviation / config.max_inventory_base)
            inv_ratio = max(-1.0, min(1.0, inv_ratio))
        else:
            inv_ratio = 0.0

        # ── Spread half ───────────────────────────────────────────────────

        half_bps = config.base_spread_bps / 2

        # Volatility widening
        if config.volatility_widening and book_spread_bps > config.vol_widening_threshold_bps:
            widen = book_spread_bps / config.vol_widening_threshold_bps
            widen = min(widen, config.max_vol_widening_factor)
            half_bps = half_bps * widen

        # Minimum spread floor
        min_half = config.min_spread_bps / 2
        half_bps = max(half_bps, min_half)

        # ── Reservation price ─────────────────────────────────────────────

        # Inventory mean-reversion: if long relative to target, push res_price
        # down so bids become less competitive and asks more competitive.
        inv_shift_bps = -(inv_ratio * config.inventory_skew_factor * half_bps)

        # Momentum directional shift
        if signal.direction == "LONG":
            mom_shift_bps = config.momentum_long_skew_bps
        elif signal.direction == "SHORT":
            mom_shift_bps = -config.momentum_short_skew_bps
        else:
            mom_shift_bps = 0.0

        total_shift_bps = inv_shift_bps + mom_shift_bps
        res_mid = mid * Decimal(1 + total_shift_bps / 10_000)

        # ── Raw bid/ask ───────────────────────────────────────────────────

        bid_price = res_mid * Decimal(1 - half_bps / 10_000)
        ask_price = res_mid * Decimal(1 + half_bps / 10_000)

        # ── Post-only enforcement ─────────────────────────────────────────

        tick = market.tick_size
        if bid_price >= ba:
            bid_price = ba - tick
        if ask_price <= bb:
            ask_price = bb + tick

        # Sanity
        if bid_price <= 0 or ask_price <= 0 or bid_price >= ask_price:
            log.warning(
                '"quote sanity failed"',
                extra={"pair": config.pair, "bid": str(bid_price), "ask": str(ask_price)},
            )
            return None

        # ── Sizes ─────────────────────────────────────────────────────────

        bid_size = config.quote_size_base
        ask_size = config.quote_size_base

        # Only accumulate new inventory when signal is explicitly LONG.
        # NEUTRAL and SHORT: no bids; manage existing inventory via asks only.
        if signal.direction != "LONG":
            bid_size = Decimal("0")
        # If we have no inventory to sell, suppress ask
        if inventory < market.min_volume:
            ask_size = Decimal("0")

        # ── Round to market precision ─────────────────────────────────────

        bid_price = _round_price(bid_price, market.price_scale)
        ask_price = _round_price(ask_price, market.price_scale)
        bid_size = _round_volume(bid_size, market.volume_scale)
        ask_size = _round_volume(ask_size, market.volume_scale)

        if bid_size < market.min_volume:
            bid_size = Decimal("0")
        if ask_size < market.min_volume:
            ask_size = Decimal("0")

        spread_bps = float((ask_price - bid_price) / mid * 10_000) if mid > 0 else 0.0

        return Quote(
            pair=config.pair,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
            mid=mid,
            reservation_mid=res_mid,
            signal=signal.direction,
            spread_bps=spread_bps,
            book_spread_bps=book_spread_bps,
            target_inventory=target_inv,
            inv_ratio=inv_ratio,
            inv_shift_bps=inv_shift_bps,
            mom_shift_bps=mom_shift_bps,
            half_bps=half_bps,
        )

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _target_inventory(direction: str, cfg: PairQuoteConfig) -> Decimal:
        # Only hold inventory when LONG. NEUTRAL and SHORT both target 0
        # so the inventory skew pushes ask prices down to exit the position.
        if direction == "LONG":
            ratio = cfg.target_inventory_long_ratio
        else:
            ratio = cfg.target_inventory_short_ratio   # should be 0.0
        return cfg.max_inventory_base * Decimal(str(ratio))

    @staticmethod
    def needs_reprice(
        existing_price: Decimal,
        target_price: Decimal,
        threshold_bps: float,
    ) -> bool:
        """Return True if the existing order price deviates beyond threshold."""
        if target_price == 0:
            return False
        dev_bps = abs(existing_price - target_price) / target_price * 10_000
        return float(dev_bps) > threshold_bps


# ── Rounding helpers ──────────────────────────────────────────────────────────


def _round_price(price: Decimal, scale: int) -> Decimal:
    quantizer = Decimal(10) ** -scale
    return price.quantize(quantizer, rounding=ROUND_DOWN)


def _round_volume(volume: Decimal, scale: int) -> Decimal:
    quantizer = Decimal(10) ** -scale
    return volume.quantize(quantizer, rounding=ROUND_DOWN)
