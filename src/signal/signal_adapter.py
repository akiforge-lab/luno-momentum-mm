"""
Momentum classifier — same algorithm as the Hyperliquid scanner,
applied to Luno daily candle data for each enabled Luno MYR pair.

Algorithm (mirrors hyperliquid-momentum-scanner/src/compute_momentum.py):
  1. Fetch the last ~LOOKBACK_DAYS daily closes from Luno candle API.
  2. Skip classification if fewer than MIN_CLOSES are available → NEUTRAL.
  3. OLS regression on log(close) over the last REGRESSION_WINDOW days:
       slope_ann_pct  = daily_slope × 252 × 100
       r2             = R² of the regression  (= correlation²)
       momentum_score = slope_ann_pct × r2
  4. 100-day simple moving average of close prices.
  5. Signal:
       LONG    slope_ann_pct > 0  AND  r2 ≥ min_r2  AND  close > ma100
       SHORT   slope_ann_pct < 0  AND  r2 ≥ min_r2  AND  close < ma100
       NEUTRAL everything else (weak trend, mixed signals, insufficient data)

Data source: Luno /api/exchange/1/candles  (daily, MYR-denominated).
Candle cache: data/candles/<PAIR>.json — refreshed when older than
              candle_cache_ttl_hours (default 12 h).  Avoids re-fetching
              all history on every signal refresh tick.

Note on MYR pricing:  the regression runs on MYR-denominated closes.
Trend direction is equivalent to USD-denominated for most moves;
MYR/USD is not a volatile pair.  If you prefer USD-normalised signals,
the USD candle source would need to be added separately.
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from src.exchange.luno_client import LunoClient

log = get_logger(__name__)

# ── Reference mapping (documentation / future use) ───────────────────────────
# Maps Hyperliquid coin symbols to Luno pair codes.
# No longer on the hot path — classification now uses Luno pairs directly.
HL_TO_LUNO: dict[str, str] = {
    "BTC":    "XBTMYR",
    "ETH":    "ETHMYR",
    "XRP":    "XRPMYR",
    "SOL":    "SOLMYR",
    "LTC":    "LTCMYR",
    "BCH":    "BCHMYR",
    "ADA":    "ADAMYR",
    "DOT":    "DOTMYR",
    "LINK":   "LINKMYR",
    "UNI":    "UNIMYR",
    "AAVE":   "AAVEMYR",
    "ALGO":   "ALGOMYR",
    "ARB":    "ARBMYR",
    "ATOM":   "ATOMMYR",
    "AVAX":   "AVAXMYR",
    "CRV":    "CRVMYR",
    "GRT":    "GRTMYR",
    "HBAR":   "HBARMYR",
    "JUP":    "JUPMYR",
    "NEAR":   "NEARMYR",
    "ONDO":   "ONDOMYR",
    "POL":    "POLMYR",
    "RENDER": "RENDERMYR",
    "SKY":    "SKYMYR",
    "SNX":    "SNXMYR",
    "SUI":    "SUIMYR",
    "TAO":    "TAOMYR",
    "TON":    "TONMYR",
    "TRX":    "TRXMYR",
    "XLM":    "XLMMYR",
}
LUNO_TO_HL: dict[str, str] = {v: k for k, v in HL_TO_LUNO.items()}


# ── Signal dataclass (public interface — unchanged) ───────────────────────────


@dataclass
class Signal:
    direction: str          # "LONG" | "SHORT" | "NEUTRAL"
    momentum_score: float   # slope_ann_pct × r2
    r2: float               # 0.0–1.0
    slope_ann_pct: float    # annualised % slope
    hl_symbol: str          # e.g. "BTC"  (cosmetic)
    luno_pair: str          # e.g. "XBTMYR"
    source_age_sec: float   # seconds since the closes were last fetched
    n_closes: int = 0       # number of closes used in classification
    ma100: float = 0.0
    latest_price: float = 0.0


# ── Pure-Python regression (identical to scipy.stats.linregress for OLS) ─────


def _linregress_log(closes: list[float]) -> tuple[float, float]:
    """
    OLS regression of log(close) on integer time index.
    Returns (slope_per_day, r2).

    Derivation:
        slope = Σ(x - x̄)(y - ȳ) / Σ(x - x̄)²
        r2    = [Σ(x - x̄)(y - ȳ)]² / [Σ(x - x̄)² · Σ(y - ȳ)²]
    where y = log(close), x = 0, 1, 2, …, n-1.
    """
    n = len(closes)
    if n < 2:
        return 0.0, 0.0

    ys = [math.log(c) for c in closes]
    # For x = 0..n-1, mean = (n-1)/2
    mean_x = (n - 1) / 2.0
    mean_y = sum(ys) / n

    ss_xx = sum((i - mean_x) ** 2 for i in range(n))
    ss_xy = sum((i - mean_x) * (y - mean_y) for i, y in enumerate(ys))
    ss_yy = sum((y - mean_y) ** 2 for y in ys)

    if ss_xx == 0:
        return 0.0, 0.0

    slope = ss_xy / ss_xx
    r2 = (ss_xy ** 2) / (ss_xx * ss_yy) if ss_yy > 0 else 1.0
    return slope, r2


# ── Candle cache ──────────────────────────────────────────────────────────────


def _cache_path(cache_dir: str, pair: str) -> Path:
    safe = pair.replace("/", "_").replace(":", "_")
    return Path(cache_dir) / f"{safe}.json"


def _load_cache(cache_dir: str, pair: str) -> Optional[dict]:
    path = _cache_path(cache_dir, pair)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _save_cache(cache_dir: str, pair: str, closes: list[tuple[int, float]]) -> None:
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    path = _cache_path(cache_dir, pair)
    tmp = path.with_suffix(".tmp")
    payload = {"updated_at": time.time(), "closes": closes}
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, path)
    except Exception as exc:
        log.warning('"candle cache write failed"', extra={"pair": pair, "error": str(exc)})


# ── Signal adapter / classifier ───────────────────────────────────────────────


class SignalAdapter:
    """
    Fetches Luno daily candles and classifies each enabled pair using the
    same momentum algorithm as the Hyperliquid scanner.

    refresh() is async — call with `await self._signals.refresh()`.
    get(luno_pair) returns a Signal synchronously from the last refresh result.
    """

    REGRESSION_WINDOW = 90   # days
    MIN_CLOSES = 100         # minimum history required
    MA_WINDOW = 100          # days for SMA trend filter
    LOOKBACK_DAYS = 220      # how far back to fetch candles

    def __init__(
        self,
        client: "LunoClient",
        pairs: list[str],
        min_r2: float = 0.50,
        candle_cache_ttl_hours: float = 12.0,
        cache_dir: str = "data/candles",
        fallback_direction: str = "NEUTRAL",
    ) -> None:
        self._client = client
        self._pairs = pairs
        self._min_r2 = min_r2
        self._cache_ttl = candle_cache_ttl_hours * 3600
        self._cache_dir = cache_dir
        self._fallback = fallback_direction
        self._signals: dict[str, Signal] = {}
        self._last_refresh: float = 0.0

    # ── Public interface ──────────────────────────────────────────────────

    def set_pairs(self, pairs: list[str]) -> None:
        """Update the pair list after runtime discovery of new pairs."""
        self._pairs = list(pairs)

    def get(self, luno_pair: str) -> Signal:
        return self._signals.get(luno_pair, self._neutral(luno_pair))

    def all_signals(self) -> dict[str, Signal]:
        return dict(self._signals)

    def last_refresh_age_sec(self) -> float:
        return time.time() - self._last_refresh if self._last_refresh else float("inf")

    async def refresh(self) -> None:
        """
        Re-classify all enabled pairs.
        Candle history is read from cache if fresh enough; otherwise fetched
        from Luno and the cache is updated.
        """
        updated: dict[str, Signal] = {}
        for pair in self._pairs:
            try:
                closes, fetch_age = await self._get_closes(pair)
                sig = self._classify(pair, closes, fetch_age)
            except Exception as exc:
                log.error('"classify error"', extra={"pair": pair, "error": str(exc)})
                sig = self._neutral(pair)
            updated[pair] = sig

        self._signals = updated
        self._last_refresh = time.time()

        log.info(
            '"signals refreshed"',
            extra={
                "pairs": self._pairs,
                "longs":   sum(1 for s in updated.values() if s.direction == "LONG"),
                "shorts":  sum(1 for s in updated.values() if s.direction == "SHORT"),
                "neutral": sum(1 for s in updated.values() if s.direction == "NEUTRAL"),
            },
        )
        for pair, sig in updated.items():
            log.info(
                '"signal"',
                extra={
                    "pair":          pair,
                    "direction":     sig.direction,
                    "momentum":      f"{sig.momentum_score:.2f}",
                    "r2":            f"{sig.r2:.3f}",
                    "slope_ann_pct": f"{sig.slope_ann_pct:.1f}",
                    "n_closes":      sig.n_closes,
                    "price_vs_ma100": (
                        "above" if sig.latest_price > sig.ma100 > 0 else
                        "below" if 0 < sig.latest_price < sig.ma100 else "n/a"
                    ),
                },
            )

    # ── Candle management ─────────────────────────────────────────────────

    async def _get_closes(self, pair: str) -> tuple[list[float], float]:
        """
        Return (list_of_close_prices, age_sec_since_last_fetch).
        Uses cache if it exists and is younger than cache_ttl; otherwise fetches.
        """
        cached = _load_cache(self._cache_dir, pair)
        now = time.time()

        if cached and (now - cached["updated_at"]) < self._cache_ttl:
            closes = [c for _, c in cached["closes"]]
            age = now - cached["updated_at"]
            log.debug(
                '"candles from cache"', extra={"pair": pair, "n": len(closes), "age_h": age / 3600}
            )
            return closes, age

        # Fetch from Luno
        since_ms = int((now - self.LOOKBACK_DAYS * 86400) * 1000)
        raw = await self._client.get_candles(pair, since_ms=since_ms, duration_sec=86400)

        if not raw:
            log.warning('"no candles returned"', extra={"pair": pair})
            # Return whatever the cache had (even if stale), or empty
            if cached:
                return [c for _, c in cached["closes"]], now - cached["updated_at"]
            return [], float("inf")

        # Sort by timestamp ascending, deduplicate
        raw_sorted = sorted(raw, key=lambda c: int(c["timestamp"]))
        closes_with_ts = [
            (int(c["timestamp"]), float(c["close"]))
            for c in raw_sorted
            if float(c["close"]) > 0
        ]
        _save_cache(self._cache_dir, pair, closes_with_ts)

        closes = [c for _, c in closes_with_ts]
        log.info('"candles fetched"', extra={"pair": pair, "n": len(closes)})
        return closes, 0.0

    # ── Classification ────────────────────────────────────────────────────

    def _classify(self, pair: str, closes: list[float], fetch_age: float) -> Signal:
        hl_sym = LUNO_TO_HL.get(pair, pair)

        if len(closes) < self.MIN_CLOSES:
            log.warning(
                '"insufficient history"',
                extra={"pair": pair, "n_closes": len(closes), "need": self.MIN_CLOSES},
            )
            return self._neutral(pair, n_closes=len(closes))

        latest_price = closes[-1]

        # MA100 from the last MA_WINDOW closes
        ma_window = min(self.MA_WINDOW, len(closes))
        ma100 = sum(closes[-ma_window:]) / ma_window

        # Regression on last REGRESSION_WINDOW closes (log-transformed)
        reg_window = min(self.REGRESSION_WINDOW, len(closes))
        reg_closes = closes[-reg_window:]
        slope, r2 = _linregress_log(reg_closes)

        slope_ann_pct = slope * 252 * 100
        momentum_score = slope_ann_pct * r2

        # Classification (same rules as HL scanner)
        if slope_ann_pct > 0 and r2 >= self._min_r2 and latest_price > ma100:
            direction = "LONG"
        elif slope_ann_pct < 0 and r2 >= self._min_r2 and latest_price < ma100:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"

        return Signal(
            direction=direction,
            momentum_score=momentum_score,
            r2=r2,
            slope_ann_pct=slope_ann_pct,
            hl_symbol=hl_sym,
            luno_pair=pair,
            source_age_sec=fetch_age,
            n_closes=len(closes),
            ma100=ma100,
            latest_price=latest_price,
        )

    def _neutral(self, luno_pair: str, n_closes: int = 0) -> Signal:
        hl_sym = LUNO_TO_HL.get(luno_pair, luno_pair)
        return Signal(
            direction=self._fallback,
            momentum_score=0.0,
            r2=0.0,
            slope_ann_pct=0.0,
            hl_symbol=hl_sym,
            luno_pair=luno_pair,
            source_age_sec=float("inf"),
            n_closes=n_closes,
        )
