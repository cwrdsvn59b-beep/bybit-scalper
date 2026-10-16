from __future__ import annotations

from decimal import Decimal

from bybit_scalper.decimals import D


class OrderBook:
    """Bybit V5 snapshot/delta. Update IDs increase but need not be consecutive.

    Transport loss/overflow must reset the book and resubscribe for a new snapshot.
    Neither u nor seq documents a +1 continuity guarantee.
    """

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_u: int | None = None
        self.valid = False
        self.ts = 0
        self.reset_needed = False

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_u = None
        self.valid = False
        self.reset_needed = True

    def apply(self, message: dict) -> None:
        data = message.get("data") or {}
        kind = message.get("type")
        try:
            update_id = int(data.get("u"))
        except (TypeError, ValueError):
            self.valid = False
            self.reset_needed = True
            return
        if kind == "snapshot":
            self.bids.clear()
            self.asks.clear()
            self._apply_levels(data, delete_zero=False)
            self.last_u = update_id
            self.valid = self._ordered()
            self.reset_needed = not self.valid
        elif kind == "delta":
            if self.last_u is None or not self.valid:
                self.valid = False
                self.reset_needed = True
                return
            if update_id <= self.last_u:
                return
            self._apply_levels(data, delete_zero=True)
            self.last_u = update_id
            self.valid = self._ordered()
            self.reset_needed = not self.valid
        else:
            self.valid = False
            self.reset_needed = True
            return
        try:
            self.ts = int(message.get("ts") or 0)
        except (TypeError, ValueError):
            self.ts = 0

    def _apply_levels(self, data: dict, delete_zero: bool) -> None:
        for raw in data.get("b") or []:
            self._put(self.bids, raw, delete_zero)
        for raw in data.get("a") or []:
            self._put(self.asks, raw, delete_zero)

    @staticmethod
    def _put(side: dict[Decimal, Decimal], raw: list, delete_zero: bool) -> None:
        price = D(raw[0])
        size = D(raw[1])
        if delete_zero and size == 0:
            side.pop(price, None)
        elif size > 0:
            side[price] = size
        elif size == 0:
            side.pop(price, None)

    def _ordered(self) -> bool:
        bids = sorted(self.bids)
        asks = sorted(self.asks)
        if not bids or not asks:
            return False
        if bids != sorted(bids) or asks != sorted(asks):
            return False
        if bids[-1] >= asks[0]:
            return False
        return True

    def top(self) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
        if not self.valid or not self.bids or not self.asks:
            return None
        bid = max(self.bids)
        ask = min(self.asks)
        return bid, ask, self.bids[bid], self.asks[ask]

    def depth_qty(self, bps: Decimal) -> tuple[Decimal, Decimal]:
        top = self.top()
        if top is None:
            return Decimal("0"), Decimal("0")
        bid, ask, _, _ = top
        mid = (bid + ask) / D(2)
        band = mid * bps / D(10_000)
        bid_qty = sum((q for p, q in self.bids.items() if p >= mid - band), Decimal("0"))
        ask_qty = sum((q for p, q in self.asks.items() if p <= mid + band), Decimal("0"))
        return bid_qty, ask_qty
