"""
Simulation layer — two modes:

1. DryRunSimulator
   ─────────────────
   Used during a live dry-run session. Receives real market data from the
   WebSocket, but all order placement is intercepted here. Simulates fills
   by checking whether our posted quotes would have traded on the next tick.

   Usage: inject an instance into OrderManager (see main.py --dry-run).

2. ReplaySimulator
   ────────────────
   Reads a JSONL recording produced by --record mode, re-runs the quoting
   engine over each snapshot, and outputs summary statistics.

   Usage: python -m simulation.replay --file data/recordings/XBTMYR.jsonl
           --config config.yaml --pair XBTMYR

Recording file format (one JSON object per line):
  {
    "pair": "XBTMYR",
    "sequence": 12345,
    "timestamp_ms": 1710000000000,
    "status": "ACTIVE",
    "best_bid": "195000.00",
    "best_ask": "195100.00",
    "bid_count": 42,
    "ask_count": 38
  }
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional

# ── Allow running as __main__ from repo root ─────────────────────────────────
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))

from src.quoting.quoting_engine import PairQuoteConfig, Quote, QuotingEngine, _round_price, _round_volume
from src.exchange.luno_ws import OrderBook
from src.signal.signal_adapter import Signal
from src.utils.logger import get_logger

log = get_logger(__name__)

_ZERO = Decimal("0")


# ── Simulated fill record ─────────────────────────────────────────────────────


@dataclass
class SimFill:
    ts_ms: int
    pair: str
    side: str
    price: Decimal
    volume: Decimal
    pnl_myr: Decimal = _ZERO


# ── Dry-run simulator (live session) ─────────────────────────────────────────


class DryRunSimulator:
    """
    Tracks simulated open orders during a dry-run live session.
    On each tick, checks whether any of our posted quotes would have filled.
    """

    def __init__(self, maker_fee_rate: Decimal = Decimal("0.001")) -> None:
        self._maker_fee = maker_fee_rate
        # key → (side, price, volume)
        self._open: dict[str, tuple[str, Decimal, Decimal]] = {}
        self.fills: list[SimFill] = []
        self._inventory: dict[str, Decimal] = {}
        self._cost_basis: dict[str, Decimal] = {}
        self._realized_pnl: dict[str, Decimal] = {}

    def add_order(self, key: str, pair: str, side: str, price: Decimal, volume: Decimal) -> None:
        self._open[key] = (side, price, volume)

    def cancel_order(self, key: str) -> None:
        self._open.pop(key, None)

    def tick(self, pair: str, best_bid: Decimal, best_ask: Decimal, ts_ms: int = 0) -> list[SimFill]:
        """
        Check open orders against new best prices. Fill any that would match.
        Returns newly created fills.
        """
        new_fills: list[SimFill] = []
        to_remove: list[str] = []

        for key, (side, price, volume) in self._open.items():
            if not key.startswith(pair):
                continue

            filled = False
            if side == "BID" and best_ask <= price:
                # Our bid would have been hit (someone crossed our bid)
                filled = True
                fill_price = price  # We're maker
            elif side == "ASK" and best_bid >= price:
                # Our ask was lifted
                filled = True
                fill_price = price

            if filled:
                fee = volume * fill_price * self._maker_fee
                pnl = _ZERO
                inv = self._inventory.get(pair, _ZERO)
                cb = self._cost_basis.get(pair, _ZERO)

                if side == "BID":
                    new_inv = inv + volume
                    if new_inv > 0:
                        self._cost_basis[pair] = (inv * cb + volume * fill_price) / new_inv
                    self._inventory[pair] = new_inv
                else:
                    pnl = volume * (fill_price - cb) - fee
                    self._inventory[pair] = max(_ZERO, inv - volume)
                    self._realized_pnl[pair] = self._realized_pnl.get(pair, _ZERO) + pnl

                sf = SimFill(ts_ms=ts_ms, pair=pair, side=side, price=fill_price, volume=volume, pnl_myr=pnl)
                new_fills.append(sf)
                self.fills.append(sf)
                to_remove.append(key)

        for k in to_remove:
            del self._open[k]

        return new_fills

    def summary(self) -> dict:
        return {
            "total_fills": len(self.fills),
            "realized_pnl": {k: str(v) for k, v in self._realized_pnl.items()},
            "inventory": {k: str(v) for k, v in self._inventory.items()},
        }


# ── Replay simulator (historical) ────────────────────────────────────────────


@dataclass
class ReplayStats:
    pair: str
    total_ticks: int = 0
    quoted_ticks: int = 0
    fills: list[SimFill] = field(default_factory=list)
    realized_pnl: Decimal = _ZERO
    fees_paid: Decimal = _ZERO
    inventory: Decimal = _ZERO
    cost_basis: Decimal = _ZERO

    def bid_fills(self) -> list[SimFill]:
        return [f for f in self.fills if f.side == "BID"]

    def ask_fills(self) -> list[SimFill]:
        return [f for f in self.fills if f.side == "ASK"]

    def quote_uptime(self) -> float:
        return self.quoted_ticks / self.total_ticks if self.total_ticks else 0.0


def run_replay(
    recording_file: str,
    pair_config_dict: dict,
    signal_direction: str = "NEUTRAL",
    maker_fee_rate: float = 0.001,
    verbose: bool = False,
) -> ReplayStats:
    """
    Replay a JSONL recording file through the quoting engine and simulate fills.

    Fill assumption: on each tick, if our posted bid_price >= row.best_ask → BID filled.
                     if our posted ask_price <= row.best_bid → ASK filled.
    (This is optimistic; real fill probability depends on queue position.)
    """
    pair = pair_config_dict["pair"] if "pair" in pair_config_dict else ""
    cfg = PairQuoteConfig.from_dict(pair, pair_config_dict)
    engine = QuotingEngine()
    fee = Decimal(str(maker_fee_rate))
    stats = ReplayStats(pair=pair)

    signal = Signal(
        direction=signal_direction,
        momentum_score=0.0,
        r2=0.8,
        slope_ann_pct=0.0,
        hl_symbol="",
        luno_pair=pair,
        source_age_sec=0.0,
    )

    # Minimal MarketInfo for simulation
    from src.exchange.luno_client import MarketInfo
    market = MarketInfo(
        pair=pair,
        min_volume=Decimal("0.0001"),
        max_volume=Decimal("10"),
        volume_scale=4,
        price_scale=2,
        tick_size=Decimal("0.01"),
        min_price=Decimal("1"),
        max_price=Decimal("99999999"),
        status="ACTIVE",
    )

    # Simulated open orders: side → (price, volume)
    open_bid: Optional[tuple[Decimal, Decimal]] = None
    open_ask: Optional[tuple[Decimal, Decimal]] = None
    inventory = _ZERO
    cost_basis = _ZERO

    with open(recording_file, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            if row.get("pair") != pair and pair:
                continue
            if row.get("status") != "ACTIVE":
                continue

            best_bid = Decimal(str(row["best_bid"]))
            best_ask = Decimal(str(row["best_ask"]))
            ts_ms = int(row.get("timestamp_ms", 0))

            stats.total_ticks += 1

            # ── Simulate fills before repricing ────────────────────────────
            if open_bid and best_ask <= open_bid[0]:
                price, volume = open_bid
                new_inv = inventory + volume
                if new_inv > 0:
                    cost_basis = (inventory * cost_basis + volume * price) / new_inv
                inventory = new_inv
                fill_fee = volume * price * fee
                stats.fees_paid += fill_fee
                sf = SimFill(ts_ms=ts_ms, pair=pair, side="BID", price=price, volume=volume)
                stats.fills.append(sf)
                open_bid = None
                if verbose:
                    print(f"  BID FILL  price={price} vol={volume} inv={inventory:.4f}")

            if open_ask and best_bid >= open_ask[0]:
                price, volume = open_ask
                pnl = volume * (price - cost_basis)
                fill_fee = volume * price * fee
                pnl_net = pnl - fill_fee
                stats.realized_pnl += pnl_net
                stats.fees_paid += fill_fee
                inventory = max(_ZERO, inventory - volume)
                sf = SimFill(ts_ms=ts_ms, pair=pair, side="ASK", price=price, volume=volume, pnl_myr=pnl_net)
                stats.fills.append(sf)
                open_ask = None
                if verbose:
                    print(f"  ASK FILL  price={price} vol={volume} pnl={pnl_net:.4f} MYR")

            # ── Build synthetic orderbook ───────────────────────────────────
            ob = OrderBook(pair=pair)
            ob.bids["_bb"] = (best_bid, Decimal("1"))
            ob.asks["_ba"] = (best_ask, Decimal("1"))
            ob.last_update_ts = 0.0
            ob.status = "ACTIVE"
            import time as _time
            ob.last_update_ts = _time.monotonic()

            # ── Compute new quotes ──────────────────────────────────────────
            quote = engine.compute(ob, signal, inventory, cfg, market)

            if quote and quote.bid_size > 0 and quote.ask_size > 0:
                stats.quoted_ticks += 1

                # Reprice or place
                if open_bid is None:
                    open_bid = (quote.bid_price, quote.bid_size)
                elif abs(open_bid[0] - quote.bid_price) / quote.bid_price * 10000 > cfg.cancel_replace_threshold_bps:
                    open_bid = (quote.bid_price, quote.bid_size)

                if open_ask is None:
                    open_ask = (quote.ask_price, quote.ask_size)
                elif abs(open_ask[0] - quote.ask_price) / quote.ask_price * 10000 > cfg.cancel_replace_threshold_bps:
                    open_ask = (quote.ask_price, quote.ask_size)

    stats.inventory = inventory
    stats.cost_basis = cost_basis
    return stats


# ── CLI entrypoint ────────────────────────────────────────────────────────────


def _print_stats(stats: ReplayStats) -> None:
    print("\n" + "=" * 60)
    print(f"  Replay results — {stats.pair}")
    print("=" * 60)
    print(f"  Ticks processed  : {stats.total_ticks:,}")
    print(f"  Quote uptime     : {stats.quote_uptime() * 100:.1f}%")
    print(f"  Total fills      : {len(stats.fills)}")
    print(f"    BID fills      : {len(stats.bid_fills())}")
    print(f"    ASK fills      : {len(stats.ask_fills())}")
    print(f"  Realized PnL     : {stats.realized_pnl:.4f} MYR")
    print(f"  Fees paid        : {stats.fees_paid:.4f} MYR")
    print(f"  Net PnL          : {stats.realized_pnl:.4f} MYR")
    print(f"  Final inventory  : {stats.inventory}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay recorded order-book snapshots")
    parser.add_argument("--file", required=True, help="JSONL recording file")
    parser.add_argument("--pair", required=True, help="Luno pair, e.g. XBTMYR")
    parser.add_argument("--config", default="config.yaml", help="config.yaml path")
    parser.add_argument("--signal", default="NEUTRAL", choices=["LONG", "SHORT", "NEUTRAL"])
    parser.add_argument("--fee", type=float, default=0.001, help="Maker fee rate (default 0.001)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    import yaml  # type: ignore
    with open(args.config, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    pair_cfg = cfg.get("pairs", {}).get(args.pair)
    if not pair_cfg:
        print(f"Pair {args.pair} not found in config")
        sys.exit(1)
    pair_cfg["pair"] = args.pair

    stats = run_replay(
        recording_file=args.file,
        pair_config_dict=pair_cfg,
        signal_direction=args.signal,
        maker_fee_rate=args.fee,
        verbose=args.verbose,
    )
    _print_stats(stats)
