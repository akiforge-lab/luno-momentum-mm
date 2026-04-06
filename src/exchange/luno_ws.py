"""
Luno WebSocket order-book handler.

Protocol
--------
  wss://ws.luno.com/api/1/stream/{PAIR}

  1. After connecting, send credentials:
       {"api_key_id": "...", "api_key_secret": "..."}

  2. Receive initial snapshot:
       {
         "sequence": "1",
         "asks": [{"id":"BXG1", "price":"195100.00", "volume":"0.5"}],
         "bids": [{"id":"BXG2", "price":"195000.00", "volume":"0.3"}],
         "status": "ACTIVE",
         "timestamp": 1710000000000
       }

  3. Subsequent messages are incremental updates:
       {
         "sequence":      "2",
         "trade_updates": [{"base":"0.1","counter":"19500","maker_order_id":"BXG2","taker_order_id":"BXG3"}],
         "create_update": {"id":"BXG4","price":"194900.00","volume":"0.2","type":"BID"},
         "delete_update": {"id":"BXG1"},
         "status":        "ACTIVE",
         "timestamp":     1710000000500
       }

  Sequence gaps → resubscribe.
  Stale threshold → caller checks `last_update_age_sec`.

Reconnect strategy: exponential back-off (1 s → 30 s).
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from src.utils.logger import get_logger

log = get_logger(__name__)

WS_BASE = "wss://ws.luno.com/api/1/stream"


# ── Order-book snapshot ────────────────────────────────────────────────────


@dataclass
class OrderBook:
    pair: str
    # order_id → (price, volume)
    bids: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)
    asks: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)
    sequence: int = 0
    last_update_ts: float = 0.0          # monotonic clock
    status: str = "UNKNOWN"

    # ── Derived accessors ────────────────────────────────────────────────

    def best_bid(self) -> Optional[Decimal]:
        if not self.bids:
            return None
        return max(p for p, _ in self.bids.values())

    def best_ask(self) -> Optional[Decimal]:
        if not self.asks:
            return None
        return min(p for p, _ in self.asks.values())

    def mid(self) -> Optional[Decimal]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def spread_bps(self) -> Optional[float]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is None or ba is None or bb == 0:
            return None
        return float((ba - bb) / bb * 10000)

    def last_update_age_sec(self) -> float:
        return time.monotonic() - self.last_update_ts

    def is_valid(self, stale_threshold_sec: float = 30.0) -> bool:
        return (
            self.status == "ACTIVE"
            and self.last_update_age_sec() < stale_threshold_sec
            and self.best_bid() is not None
            and self.best_ask() is not None
        )

    # ── Mutators ─────────────────────────────────────────────────────────

    def apply_snapshot(self, msg: dict) -> None:
        # Luno uses "id" in some protocol versions and "order_id" in others
        def _oid(e: dict) -> str:
            return e.get("id") or e.get("order_id") or ""

        self.bids = {
            _oid(e): (Decimal(e["price"]), Decimal(e["volume"]))
            for e in msg.get("bids", []) if _oid(e)
        }
        self.asks = {
            _oid(e): (Decimal(e["price"]), Decimal(e["volume"]))
            for e in msg.get("asks", []) if _oid(e)
        }
        self.sequence = int(msg.get("sequence", 0))
        self.status = msg.get("status", "ACTIVE")
        self.last_update_ts = time.monotonic()

    def apply_update(self, msg: dict) -> bool:
        """Apply incremental update. Returns False if a real sequence gap detected."""
        # Luno sends status-only heartbeat frames with no "sequence" field — skip silently.
        if "sequence" not in msg:
            self.status = msg.get("status", self.status)
            self.last_update_ts = time.monotonic()
            return True

        seq = int(msg["sequence"])
        if seq != self.sequence + 1:
            log.warning(
                '"sequence gap, will resubscribe"',
                extra={"pair": self.pair, "expected": self.sequence + 1, "got": seq},
            )
            return False

        self.sequence = seq
        self.status = msg.get("status", self.status)
        self.last_update_ts = time.monotonic()

        # Trade updates — remove fully-traded orders
        for trade in msg.get("trade_updates", []):
            mid = trade.get("maker_order_id")
            if mid and mid in self.bids:
                del self.bids[mid]
            elif mid and mid in self.asks:
                del self.asks[mid]

        # Delete update
        del_upd = msg.get("delete_update")
        if del_upd:
            oid = del_upd.get("id") or del_upd.get("order_id")
            self.bids.pop(oid, None)
            self.asks.pop(oid, None)

        # Create update
        cre_upd = msg.get("create_update")
        if cre_upd:
            oid = cre_upd.get("id") or cre_upd.get("order_id")
            if oid:
                entry = (Decimal(cre_upd["price"]), Decimal(cre_upd["volume"]))
                if cre_upd.get("type") == "ASK":
                    self.asks[oid] = entry
                else:
                    self.bids[oid] = entry

        return True

    def to_dict(self) -> dict:
        """Serialise for recording."""
        return {
            "pair": self.pair,
            "sequence": self.sequence,
            "timestamp_ms": int(time.time() * 1000),
            "status": self.status,
            "best_bid": str(self.best_bid()),
            "best_ask": str(self.best_ask()),
            "bid_count": len(self.bids),
            "ask_count": len(self.asks),
        }


class _SequenceGap(Exception):
    """Raised when a real sequence gap is detected; triggers clean reconnect."""


# ── WebSocket handler ──────────────────────────────────────────────────────


class LunoWebSocket:
    """
    Maintains a live OrderBook for one trading pair.

    Usage::

        ws = LunoWebSocket("XBTMYR", api_key, api_secret)
        task = asyncio.create_task(ws.run())
        ...
        ob = ws.orderbook   # read from quoting loop
    """

    def __init__(
        self,
        pair: str,
        api_key: str,
        api_secret: str,
        on_trade: Optional[Callable[[str, dict], None]] = None,
        record_path: Optional[str] = None,
    ) -> None:
        self.pair = pair
        self._key = api_key
        self._secret = api_secret
        self._on_trade = on_trade    # callback(pair, trade_update_dict)
        self._record_path = record_path
        self._record_fh = None
        self.orderbook = OrderBook(pair=pair)
        self._connected = False
        self._stop_event = asyncio.Event()

    # ── Control ───────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Main loop ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run forever with reconnect until stop() is called."""
        if self._record_path:
            self._record_fh = open(self._record_path, "a")  # noqa: SIM115
        try:
            await self._run_with_reconnect()
        finally:
            if self._record_fh:
                self._record_fh.close()

    async def _run_with_reconnect(self) -> None:
        backoff = 1.0
        max_backoff = 30.0

        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
                backoff = 1.0   # reset on clean exit
            except _SequenceGap:
                log.warning('"ws sequence gap, reconnecting"', extra={"pair": self.pair})
            except ConnectionClosed as exc:
                log.warning(
                    '"ws disconnected"',
                    extra={"pair": self.pair, "code": exc.rcvd.code if exc.rcvd else None},
                )
            except Exception as exc:
                log.error('"ws error"', extra={"pair": self.pair, "error": str(exc)})
            finally:
                self._connected = False

            if self._stop_event.is_set():
                break
            log.info(
                '"ws reconnecting"', extra={"pair": self.pair, "backoff": backoff}
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def _connect_and_stream(self) -> None:
        url = f"{WS_BASE}/{self.pair}"
        log.info('"ws connecting"', extra={"pair": self.pair, "url": url})

        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            # Send credentials
            await ws.send(
                json.dumps({"api_key_id": self._key, "api_key_secret": self._secret})
            )
            self._connected = True
            log.info('"ws connected"', extra={"pair": self.pair})

            async for raw in ws:
                if self._stop_event.is_set():
                    break
                await self._handle_message(raw)

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Luno occasionally sends non-dict keepalive frames (e.g. "1")
        if not isinstance(msg, dict):
            return

        # Distinguish snapshot (has "asks"/"bids" top-level arrays) from update
        if "asks" in msg or "bids" in msg:
            self.orderbook.apply_snapshot(msg)
            log.info(
                '"ob snapshot"',
                extra={
                    "pair": self.pair,
                    "seq": self.orderbook.sequence,
                    "bid": str(self.orderbook.best_bid()),
                    "ask": str(self.orderbook.best_ask()),
                },
            )
        else:
            ok = self.orderbook.apply_update(msg)
            if not ok:
                # Real sequence gap — force reconnect via dedicated exception
                raise _SequenceGap()

        # Record snapshot to file
        if self._record_fh:
            self._record_fh.write(json.dumps(self.orderbook.to_dict()) + "\n")
            self._record_fh.flush()

        # Fire trade callbacks
        if self._on_trade:
            for trade in msg.get("trade_updates", []):
                self._on_trade(self.pair, trade)
