"""
Risk manager — enforces all per-pair and global limits.

Checks performed before every quoting/order action:
  • Global kill switch (config or runtime)
  • Per-pair kill switch
  • Max daily realized loss (MYR)
  • Max inventory (base currency)
  • Max notional exposure (MYR)
  • Max total open orders
  • Stale order-book data

Also tracks:
  • Per-pair inventory (net base position, always ≥ 0 in spot)
  • Per-pair realized PnL (in MYR)
  • Mark-to-market unrealized PnL
  • Daily PnL reset at midnight UTC
  • Total fees paid
  • Quote uptime (fraction of ticks where both sides were quoted)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from src.exchange.luno_ws import OrderBook
from src.orders.order_manager import FillRecord
from src.utils.logger import get_logger

log = get_logger(__name__)

_ZERO = Decimal("0")


# ── Per-pair config ────────────────────────────────────────────────────────────


@dataclass
class PairRiskConfig:
    pair: str
    max_inventory_base: Decimal
    max_notional_myr: Decimal
    allocation_pct: float = 0.0        # fraction of portfolio to allocate (0 = static only)
    hard_cap_units: Decimal = _ZERO    # absolute ceiling in base units (0 = use max_inventory_base)
    hl_symbol: str = ""
    enabled: bool = True
    kill_switch: bool = False

    @classmethod
    def from_dict(cls, pair: str, d: dict) -> "PairRiskConfig":
        hard_cap = Decimal(str(d.get("hard_cap_units", d["max_inventory_base"])))
        return cls(
            pair=pair,
            max_inventory_base=Decimal(str(d["max_inventory_base"])),
            max_notional_myr=Decimal(str(d["max_notional_myr"])),
            allocation_pct=float(d.get("allocation_pct", 0.0)),
            hard_cap_units=hard_cap,
            hl_symbol=d.get("hl_symbol", ""),
            enabled=bool(d.get("enabled", True)),
            kill_switch=False,
        )


# ── PnL & inventory tracking per pair ────────────────────────────────────────


@dataclass
class PairMetrics:
    pair: str
    inventory: Decimal = _ZERO          # current base position
    realized_pnl_myr: Decimal = _ZERO   # cumulative realized PnL
    daily_pnl_myr: Decimal = _ZERO      # since last daily reset
    fees_paid_myr: Decimal = _ZERO
    fill_count: int = 0
    quote_ticks: int = 0                # ticks where we had active quotes
    total_ticks: int = 0

    def mark_to_market(self, mid: Optional[Decimal], cost_basis: Decimal) -> Decimal:
        """Unrealized PnL = inventory * (mid - avg_cost_basis)."""
        if mid is None or self.inventory == 0:
            return _ZERO
        return self.inventory * (mid - cost_basis)

    @property
    def quote_uptime(self) -> float:
        if self.total_ticks == 0:
            return 0.0
        return self.quote_ticks / self.total_ticks


# ── Risk manager ──────────────────────────────────────────────────────────────


class RiskManager:
    def __init__(
        self,
        pair_configs: dict[str, PairRiskConfig],
        max_daily_loss_myr: float = 500.0,
        max_open_orders_total: int = 20,
        stale_ob_threshold_sec: float = 30.0,
        global_kill_switch: bool = False,
    ) -> None:
        self._pair_cfg: dict[str, PairRiskConfig] = pair_configs
        self._max_daily_loss = Decimal(str(max_daily_loss_myr))
        self._max_open_orders = max_open_orders_total
        self._stale_threshold = stale_ob_threshold_sec
        self._global_kill = global_kill_switch

        # Per-pair state
        self._metrics: dict[str, PairMetrics] = {
            p: PairMetrics(pair=p) for p in pair_configs
        }
        # Average cost basis per pair (for MTM)
        self._cost_basis: dict[str, Decimal] = {p: _ZERO for p in pair_configs}

        # Dynamic inventory limits (portfolio-based)
        self._portfolio_value_myr: Decimal = _ZERO
        self._total_inv_value_myr: Decimal = _ZERO   # sum(inventory[p] * mid[p])
        self._dynamic_max: dict[str, Decimal] = {
            p: (cfg.hard_cap_units if cfg.hard_cap_units > _ZERO else cfg.max_inventory_base)
            for p, cfg in pair_configs.items()
        }

        # Daily reset bookkeeping
        self._daily_reset_day: int = self._today()

    # ── Kill switch control ───────────────────────────────────────────────

    def set_global_kill(self, val: bool) -> None:
        self._global_kill = val
        if val:
            log.warning('"GLOBAL KILL SWITCH ACTIVATED"')

    def set_pair_kill(self, pair: str, val: bool) -> None:
        if pair in self._pair_cfg:
            self._pair_cfg[pair].kill_switch = val
            log.warning('"pair kill switch"', extra={"pair": pair, "kill": val})

    def add_pair(self, cfg: PairRiskConfig) -> None:
        """Register a new pair at runtime (e.g. from auto-discovery). No-op if already registered."""
        if cfg.pair in self._pair_cfg:
            return
        self._pair_cfg[cfg.pair] = cfg
        self._metrics[cfg.pair] = PairMetrics(pair=cfg.pair)
        self._cost_basis[cfg.pair] = _ZERO
        self._dynamic_max[cfg.pair] = (
            cfg.hard_cap_units if cfg.hard_cap_units > _ZERO else cfg.max_inventory_base
        )

    # ── Dynamic inventory limits ──────────────────────────────────────────

    def update_portfolio_value(self, value_myr: Decimal) -> None:
        self._portfolio_value_myr = value_myr

    def update_total_inventory_value(self, value_myr: Decimal) -> None:
        """Track sum(inventory[p] * mid[p]) across all pairs. Used for portfolio cap."""
        self._total_inv_value_myr = value_myr

    def update_dynamic_max(self, pair: str, mid: Decimal) -> None:
        """Recompute max inventory for pair based on current portfolio value and mid."""
        cfg = self._pair_cfg.get(pair)
        if cfg is None or cfg.allocation_pct == 0.0 or mid <= _ZERO:
            return
        dynamic = (self._portfolio_value_myr * Decimal(str(cfg.allocation_pct))) / mid
        cap = cfg.hard_cap_units if cfg.hard_cap_units > _ZERO else cfg.max_inventory_base
        self._dynamic_max[pair] = min(dynamic, cap)

    def effective_max_inventory(self, pair: str) -> Decimal:
        """Current effective max inventory for pair (dynamic or static fallback)."""
        return self._dynamic_max.get(
            pair,
            self._pair_cfg[pair].max_inventory_base if pair in self._pair_cfg else _ZERO,
        )

    @property
    def portfolio_value_myr(self) -> Decimal:
        return self._portfolio_value_myr

    # ── Pre-quote checks ──────────────────────────────────────────────────

    def can_quote(
        self,
        pair: str,
        orderbook: OrderBook,
        open_order_count: int,
    ) -> tuple[bool, str]:
        """
        Return (True, "") if quoting is allowed for the pair, or
        (False, reason) otherwise.
        """
        self._maybe_daily_reset()

        if self._global_kill:
            return False, "global_kill_switch"

        cfg = self._pair_cfg.get(pair)
        if cfg is None:
            return False, "pair_not_configured"
        if not cfg.enabled:
            return False, "pair_disabled"
        if cfg.kill_switch:
            return False, "pair_kill_switch"

        if open_order_count >= self._max_open_orders:
            return False, f"max_open_orders ({self._max_open_orders})"

        if not orderbook.is_valid(self._stale_threshold):
            age = orderbook.last_update_age_sec()
            return False, f"stale_orderbook ({age:.1f}s)"

        m = self._metrics[pair]
        if m.daily_pnl_myr < -self._max_daily_loss:
            return False, f"daily_loss_limit ({m.daily_pnl_myr:.2f} MYR)"

        return True, ""

    def can_place_bid(self, pair: str, price: Decimal, size: Decimal) -> tuple[bool, str]:
        """Extra check before placing a BID order."""
        cfg = self._pair_cfg.get(pair)
        if cfg is None:
            return False, "pair_not_configured"
        m = self._metrics[pair]
        eff_max = self.effective_max_inventory(pair)
        new_inventory = m.inventory + size
        if new_inventory > eff_max:
            return False, f"max_inventory ({new_inventory:.6f} > {eff_max:.6f})"
        notional = (m.inventory + size) * price
        if notional > cfg.max_notional_myr:
            return False, f"max_notional ({notional:.2f} > {cfg.max_notional_myr})"
        # Portfolio-level cap: total inventory value must not exceed portfolio value.
        # Guards against the case where per-pair allocations sum to >100% of capital.
        if self._portfolio_value_myr > _ZERO:
            prospective_total = self._total_inv_value_myr + size * price
            if prospective_total > self._portfolio_value_myr:
                return False, (
                    f"portfolio_cap ({prospective_total:.2f} > "
                    f"{self._portfolio_value_myr:.2f} MYR)"
                )
        return True, ""

    def can_place_ask(self, pair: str, size: Decimal) -> tuple[bool, str]:
        """Extra check before placing an ASK order. Can't sell what we don't have."""
        m = self._metrics[pair]
        if m.inventory < size:
            return False, f"insufficient_inventory ({m.inventory} < {size})"
        return True, ""

    # ── Fill processing ───────────────────────────────────────────────────

    def process_fill(self, fill: FillRecord) -> None:
        """Update inventory and PnL from a confirmed fill."""
        pair = fill.pair
        m = self._metrics.get(pair)
        if m is None:
            return

        m.fill_count += 1

        if fill.side == "BID":
            # We bought base currency
            old_inv = m.inventory
            new_inv = m.inventory + fill.volume
            # Update cost basis (weighted average)
            if new_inv > 0:
                self._cost_basis[pair] = (
                    old_inv * self._cost_basis[pair] + fill.volume * fill.price
                ) / new_inv
            m.inventory = new_inv

        else:  # ASK
            # We sold base currency; realise PnL against cost basis
            pnl = fill.volume * (fill.price - self._cost_basis[pair])
            m.realized_pnl_myr += pnl
            m.daily_pnl_myr += pnl
            m.inventory = max(_ZERO, m.inventory - fill.volume)

        m.fees_paid_myr += fill.fee

        log.info(
            '"fill processed"',
            extra={
                "pair": pair,
                "side": fill.side,
                "price": str(fill.price),
                "size": str(fill.volume),
                "inventory": str(m.inventory),
                "daily_pnl": str(m.daily_pnl_myr),
            },
        )

    # ── Inventory / balance sync (from REST balance refresh) ─────────────

    def sync_inventory(self, pair: str, base_available: Decimal) -> None:
        """
        Update our tracked inventory from the exchange balance.
        Called after REST balance refresh.
        """
        m = self._metrics.get(pair)
        if m:
            m.inventory = base_available

    # ── Tick counters ─────────────────────────────────────────────────────

    def record_quote_tick(self, pair: str, both_sides_quoted: bool) -> None:
        m = self._metrics.get(pair)
        if m:
            m.total_ticks += 1
            if both_sides_quoted:
                m.quote_ticks += 1

    def mtm_pnl(self, pair: str, mid: Optional[Decimal]) -> Decimal:
        """Unrealized PnL = inventory × (mid − cost_basis)."""
        m = self._metrics.get(pair)
        if m is None or mid is None:
            return _ZERO
        return m.mark_to_market(mid, self._cost_basis.get(pair, _ZERO))

    # ── Accessors ─────────────────────────────────────────────────────────

    def inventory(self, pair: str) -> Decimal:
        return self._metrics[pair].inventory if pair in self._metrics else _ZERO

    def metrics(self, pair: str) -> Optional[PairMetrics]:
        return self._metrics.get(pair)

    def all_metrics(self) -> dict[str, PairMetrics]:
        return dict(self._metrics)

    def daily_pnl_total(self) -> Decimal:
        return sum(m.daily_pnl_myr for m in self._metrics.values())

    def to_state_dict(self) -> dict:
        return {
            pair: {
                "inventory": str(m.inventory),
                "realized_pnl_myr": str(m.realized_pnl_myr),
                "daily_pnl_myr": str(m.daily_pnl_myr),
                "fees_paid_myr": str(m.fees_paid_myr),
                "fill_count": m.fill_count,
                "cost_basis": str(self._cost_basis.get(pair, _ZERO)),
            }
            for pair, m in self._metrics.items()
        }

    def load_state_dict(self, state: dict) -> None:
        for pair, d in state.items():
            m = self._metrics.get(pair)
            if m:
                m.inventory = Decimal(str(d.get("inventory", "0")))
                m.realized_pnl_myr = Decimal(str(d.get("realized_pnl_myr", "0")))
                m.daily_pnl_myr = Decimal(str(d.get("daily_pnl_myr", "0")))
                m.fees_paid_myr = Decimal(str(d.get("fees_paid_myr", "0")))
                m.fill_count = int(d.get("fill_count", 0))
                self._cost_basis[pair] = Decimal(str(d.get("cost_basis", "0")))

    # ── Daily reset ───────────────────────────────────────────────────────

    def _maybe_daily_reset(self) -> None:
        today = self._today()
        if today != self._daily_reset_day:
            log.info('"daily PnL reset"')
            for m in self._metrics.values():
                m.daily_pnl_myr = _ZERO
            self._daily_reset_day = today

    @staticmethod
    def _today() -> int:
        import datetime
        return datetime.datetime.utcnow().toordinal()

    # ── Utility ───────────────────────────────────────────────────────────

    def log_status(self) -> None:
        for pair, m in self._metrics.items():
            log.info(
                '"risk_status"',
                extra={
                    "pair": pair,
                    "inventory": str(m.inventory),
                    "realized_pnl": str(m.realized_pnl_myr),
                    "daily_pnl": str(m.daily_pnl_myr),
                    "fills": m.fill_count,
                    "uptime_pct": f"{m.quote_uptime * 100:.1f}",
                },
            )
