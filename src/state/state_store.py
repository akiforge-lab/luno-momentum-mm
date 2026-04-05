"""
State store — persists bot state to disk so restarts are safe.

File format: JSON, written atomically (write to .tmp then rename).
Location:    data/state/bot_state.json  (configurable).

Saved fields:
  • Risk metrics (inventory, PnL, cost basis) per pair
  • Fill records (capped at last 1 000)
  • Session start time
  • Last save timestamp
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

from src.utils.logger import get_logger

log = get_logger(__name__)

_DEFAULT_PATH = Path("data/state/bot_state.json")


class StateStore:
    def __init__(self, path: Optional[str] = None) -> None:
        self._path = Path(path) if path else _DEFAULT_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict[str, Any] = {}
        self._dirty = False

    # ── Load / save ───────────────────────────────────────────────────────

    def load(self) -> dict:
        if not self._path.exists():
            log.info('"state file not found, starting fresh"', extra={"path": str(self._path)})
            return {}
        try:
            with open(self._path, encoding="utf-8") as fh:
                self._state = json.load(fh)
            log.info('"state loaded"', extra={"path": str(self._path)})
            return self._state
        except Exception as exc:
            log.error('"state load error"', extra={"error": str(exc)})
            return {}

    def save(self, risk_state: dict, fills: list[dict]) -> None:
        """Atomic save (write tmp → rename)."""
        payload = {
            "saved_at": time.time(),
            "saved_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "risk": risk_state,
            "fills": fills[-1000:],   # keep last 1 000 fills
        }
        tmp = self._path.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, default=str)
            os.replace(tmp, self._path)
            self._dirty = False
        except Exception as exc:
            log.error('"state save error"', extra={"error": str(exc)})

    # ── Helpers ───────────────────────────────────────────────────────────

    def get_risk_state(self) -> dict:
        return self._state.get("risk", {})

    def get_fills(self) -> list[dict]:
        return self._state.get("fills", [])

    def fills_to_dicts(self, fills: list) -> list[dict]:
        """Convert FillRecord objects to plain dicts for serialisation."""
        result = []
        for f in fills:
            result.append(
                {
                    "order_id": f.order_id,
                    "pair": f.pair,
                    "side": f.side,
                    "price": str(f.price),
                    "volume": str(f.volume),
                    "fee": str(f.fee),
                    "ts": f.ts,
                }
            )
        return result
