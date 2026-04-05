"""
Async REST client for the Luno API.

Authentication: HTTP Basic — api_key_id : api_key_secret
Rate limit:     ~300 req/min for authenticated endpoints (≈5 req/s).
                We enforce a conservative token-bucket at 4 req/s.

Key endpoints used:
  GET  /api/1/balance            — wallet balances
  GET  /api/1/marketinfo         — pair metadata (scales, min_volume, status)
  GET  /api/1/feestatus          — maker/taker fee for the pair
  GET  /api/1/ticker             — best bid/ask snapshot
  GET  /api/1/orderbook          — full order book snapshot (REST fallback)
  POST /api/1/postorder          — place a post-only limit order
  DELETE /api/1/orders/{id}      — cancel an order
  GET  /api/1/listorders         — list open orders for a pair
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import aiohttp

from src.utils.logger import get_logger

log = get_logger(__name__)

# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class MarketInfo:
    pair: str
    min_volume: Decimal
    max_volume: Decimal
    volume_scale: int       # decimal places for volume
    price_scale: int        # decimal places for price
    tick_size: Decimal      # 10^-price_scale
    min_price: Decimal
    max_price: Decimal
    status: str             # "ACTIVE" | "POSTONLY" | "DISABLED"


@dataclass
class FeeInfo:
    maker_fee: Decimal
    taker_fee: Decimal
    thirty_day_volume: Decimal = Decimal("0")


@dataclass
class Order:
    order_id: str
    pair: str
    side: str               # "BID" | "ASK"
    price: Decimal
    volume: Decimal         # original volume
    remaining_volume: Decimal = field(default=Decimal("0"))
    state: str = "PENDING"  # PENDING | COMPLETE | CANCELLED
    created_at_ms: int = 0
    client_order_id: str = ""

    def __post_init__(self) -> None:
        if self.remaining_volume == Decimal("0"):
            self.remaining_volume = self.volume


# ── Rate limiter (token bucket) ───────────────────────────────────────────────


class _RateLimiter:
    """Simple async token-bucket rate limiter."""

    def __init__(self, rate: float = 4.0, burst: int = 8) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


# ── Luno REST client ──────────────────────────────────────────────────────────


class LunoClient:
    BASE_URL = "https://api.luno.com"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._rl = _RateLimiter(rate=4.0, burst=8)
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Session management ────────────────────────────────────────────────

    async def _session_(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            auth = aiohttp.BasicAuth(self._api_key, self._api_secret)
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(auth=auth, timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Internal request helper ───────────────────────────────────────────

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        retries: int = 3,
    ) -> dict:
        await self._rl.acquire()
        session = await self._session_()
        url = f"{self.BASE_URL}{endpoint}"
        last_exc: Exception = RuntimeError("No attempts made")
        for attempt in range(retries):
            try:
                async with session.request(
                    method, url, params=params, data=data
                ) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 2.0))
                        log.warning(
                            '"rate limited, sleeping"',
                            extra={"retry_after": retry_after},
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    return body
            except aiohttp.ClientError as exc:
                last_exc = exc
                if attempt < retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
        raise last_exc

    # ── Public API methods ────────────────────────────────────────────────

    async def get_balances(self) -> dict[str, Decimal]:
        """Return {asset: available_balance} for all wallet accounts."""
        data = await self._request("GET", "/api/1/balance")
        result: dict[str, Decimal] = {}
        for b in data.get("balance", []):
            asset = b["asset"]
            available = Decimal(str(b.get("balance", "0"))) - Decimal(
                str(b.get("reserved", "0"))
            )
            result[asset] = available
        return result

    async def get_market_info(self, pair: str) -> MarketInfo:
        # GET /api/exchange/1/markets — public, no auth, returns all pairs.
        data = await self._request("GET", "/api/exchange/1/markets")
        markets = data.get("markets", [])
        info = next((m for m in markets if m.get("market_id") == pair), None)
        if info is None:
            raise ValueError(f"Pair {pair} not found in /api/exchange/1/markets")
        price_scale = int(info.get("price_scale", 2))
        volume_scale = int(info.get("volume_scale", 8))
        tick = Decimal(10) ** -price_scale
        return MarketInfo(
            pair=pair,
            min_volume=Decimal(str(info.get("min_volume", "0"))),
            max_volume=Decimal(str(info.get("max_volume", "0"))),
            volume_scale=volume_scale,
            price_scale=price_scale,
            tick_size=tick,
            min_price=Decimal(str(info.get("min_price", "0"))),
            max_price=Decimal(str(info.get("max_price", "99999999"))),
            status=info.get("trading_status", "ACTIVE"),
        )

    async def get_fee_info(self, pair: str) -> FeeInfo:
        data = await self._request("GET", "/api/1/fee_info", params={"pair": pair})
        return FeeInfo(
            maker_fee=Decimal(str(data.get("maker_fee", "0.001"))),
            taker_fee=Decimal(str(data.get("taker_fee", "0.001"))),
            thirty_day_volume=Decimal(str(data.get("thirty_day_volume", "0"))),
        )

    async def get_ticker(self, pair: str) -> dict[str, Decimal]:
        data = await self._request("GET", "/api/1/ticker", params={"pair": pair})
        return {
            "bid": Decimal(str(data["bid"])),
            "ask": Decimal(str(data["ask"])),
            "last_trade": Decimal(str(data.get("last_trade", data["bid"]))),
            "timestamp": int(data.get("timestamp", 0)),
        }

    async def get_orderbook(self, pair: str) -> dict:
        """REST orderbook snapshot — used as WS fallback."""
        data = await self._request("GET", "/api/1/orderbook", params={"pair": pair})
        return data

    async def place_post_only_order(
        self,
        pair: str,
        side: str,       # "BID" | "ASK"
        price: Decimal,
        volume: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Optional[Order]:
        """
        Place a post-only limit order via POST /api/1/postorder.
        Returns None if Luno rejects the order (e.g. would cross book).
        """
        cid = client_order_id or f"mm-{uuid.uuid4().hex[:12]}"
        payload = {
            "pair": pair,
            "type": side,
            "price": str(price),
            "volume": str(volume),
            "client_order_id": cid,
        }
        try:
            data = await self._request("POST", "/api/1/postorder", data=payload)
        except aiohttp.ClientResponseError as exc:
            # Luno returns 400/422 when the order would immediately match
            log.warning(
                '"post-only order rejected"',
                extra={"pair": pair, "side": side, "price": str(price), "status": exc.status},
            )
            return None
        except Exception as exc:
            log.error(
                '"order placement error"',
                extra={"pair": pair, "side": side, "error": str(exc)},
            )
            return None

        order_id = data.get("order_id")
        if not order_id:
            log.error('"no order_id in response"', extra={"pair": pair, "resp": data})
            return None

        order = Order(
            order_id=order_id,
            pair=pair,
            side=side,
            price=price,
            volume=volume,
            remaining_volume=volume,
            state="PENDING",
            created_at_ms=int(time.time() * 1000),
            client_order_id=cid,
        )
        log.info(
            '"order placed"',
            extra={
                "pair": pair,
                "side": side,
                "price": str(price),
                "size": str(volume),
                "order_id": order_id,
            },
        )
        return order

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order. Returns True on success, False if already gone."""
        try:
            await self._request("DELETE", f"/api/1/orders/{order_id}")
            log.info('"order cancelled"', extra={"order_id": order_id})
            return True
        except aiohttp.ClientResponseError as exc:
            if exc.status in (404, 400):
                # Already filled or cancelled — not an error
                return False
            log.warning(
                '"cancel failed"', extra={"order_id": order_id, "status": exc.status}
            )
            return False
        except Exception as exc:
            log.error('"cancel error"', extra={"order_id": order_id, "error": str(exc)})
            return False

    async def get_open_orders(self, pair: str) -> list[Order]:
        """Return all PENDING orders for a pair."""
        try:
            data = await self._request(
                "GET", "/api/1/listorders", params={"pair": pair, "state": "PENDING"}
            )
        except Exception as exc:
            log.error(
                '"get_open_orders error"', extra={"pair": pair, "error": str(exc)}
            )
            return []
        orders: list[Order] = []
        for o in data.get("orders", []):
            try:
                orders.append(
                    Order(
                        order_id=o["order_id"],
                        pair=pair,
                        side=o["type"],
                        price=Decimal(str(o.get("limit_price", "0"))),
                        volume=Decimal(str(o.get("limit_volume", "0"))),
                        remaining_volume=Decimal(
                            str(o.get("base", o.get("limit_volume", "0")))
                        ),
                        state=o.get("state", "PENDING"),
                        created_at_ms=int(o.get("creation_timestamp", 0)),
                        client_order_id=o.get("client_order_id", ""),
                    )
                )
            except Exception:
                pass
        return orders

    async def get_candles(
        self,
        pair: str,
        since_ms: int,
        duration_sec: int = 86400,
    ) -> list[dict]:
        """
        Fetch OHLCV candles from GET /api/exchange/1/candles.
        Returns a list of dicts with keys: timestamp, open, close, high, low, volume.
        """
        data = await self._request(
            "GET",
            "/api/exchange/1/candles",
            params={"pair": pair, "since": since_ms, "duration": duration_sec},
        )
        return data.get("candles", [])

    async def cancel_all_open_orders(self, pair: str) -> int:
        """Cancel every open order for a pair. Returns count cancelled."""
        orders = await self.get_open_orders(pair)
        tasks = [self.cancel_order(o.order_id) for o in orders]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)
