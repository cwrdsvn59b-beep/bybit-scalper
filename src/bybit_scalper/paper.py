from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import uuid4

from bybit_scalper.decimals import D
from bybit_scalper.types import Exec, Fees, Instrument, OrderStatus, PlaceResult, Position, Side, StopResult


@dataclass
class Resting:
    link_id: str
    side: Side
    price: Decimal
    qty: Decimal
    filled: Decimal
    queue_ahead: Decimal
    status: OrderStatus
    reduce_only: bool
    execs: list[Exec] = field(default_factory=list)


class PaperExchange:
    """Исполнение лимита только лентой сделок, не касанием high/low свечи.

    Очередь перед нашей заявкой должна быть съедена объёмом ленты.
    Отмена сначала применяет сделку, уже находящуюся в пути.
    """

    def __init__(self, instrument: Instrument, fees: Fees, slippage_bps: Decimal, queue_ahead: Decimal) -> None:
        self.instrument = instrument
        self.fees = fees
        self.slippage_bps = slippage_bps
        self.queue_ahead_default = queue_ahead
        self.orders: dict[str, Resting] = {}
        self.position = Position()
        self.fail_stop = False
        self.place_timeout = False
        self.timeout_still_rests = False
        self.http_calls: list[str] = []
        self.market_closes: list[str] = []
        self.stop_calls: list[dict] = []
        self.inflight_on_cancel: tuple[Decimal, Decimal] | None = None
        self.realized = Decimal("0")
        self.fees_paid = Decimal("0")
        self.slippage_paid = Decimal("0")
        self._exec_n = 0
        self._session_id = uuid4().hex
        self.server_ms = 0

    def place_limit(
        self,
        link_id: str,
        side: Side,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> PlaceResult:
        self.http_calls.append("place")
        if self.place_timeout:
            self.place_timeout = False
            if self.timeout_still_rests:
                self.timeout_still_rests = False
                self.orders[link_id] = Resting(
                    link_id=link_id,
                    side=side,
                    price=price,
                    qty=qty,
                    filled=Decimal("0"),
                    queue_ahead=self.queue_ahead_default,
                    status=OrderStatus.SUBMITTED,
                    reduce_only=reduce_only,
                )
            raise TimeoutError("timeout after the request may have reached the exchange")
        order = Resting(
            link_id=link_id,
            side=side,
            price=price,
            qty=qty,
            filled=Decimal("0"),
            queue_ahead=self.queue_ahead_default,
            status=OrderStatus.SUBMITTED,
            reduce_only=reduce_only,
        )
        self.orders[link_id] = order
        return PlaceResult(True, link_id, "accepted", order_id=f"oid-{link_id}")

    def cancel(self, link_id: str) -> None:
        self.http_calls.append("cancel")
        if self.inflight_on_cancel is not None:
            price, qty = self.inflight_on_cancel
            self.inflight_on_cancel = None
            self.match_trade(price, qty)
        order = self.orders.get(link_id)
        if order and order.filled < order.qty and order.status != OrderStatus.FILLED:
            order.status = OrderStatus.CANCELLED

    def match_trade(self, price: Decimal, qty: Decimal, taker_side: str | None = None) -> list[Exec]:
        """Сделка ленты. Касание фитилём сюда не передаётся."""
        produced: list[Exec] = []
        left = qty
        for order in list(self.orders.values()):
            if left <= 0:
                break
            if order.status not in {OrderStatus.SUBMITTED, OrderStatus.PARTIAL}:
                continue
            if taker_side and taker_side != ("Sell" if order.side is Side.LONG else "Buy"):
                continue
            crosses = price <= order.price if order.side is Side.LONG else price >= order.price
            if not crosses:
                continue
            if order.queue_ahead > 0:
                eaten = min(order.queue_ahead, left)
                order.queue_ahead -= eaten
                left -= eaten
            if left <= 0 or order.queue_ahead > 0:
                continue
            take = min(order.qty - order.filled, left)
            if take <= 0:
                continue
            produced.append(self._fill(order, take, order.price, maker=True))
            left -= take
        return produced

    def executions(self, link_id: str) -> list[Exec]:
        order = self.orders.get(link_id)
        return [] if order is None else list(order.execs)

    def set_stop(self, stop: Decimal, target: Decimal) -> StopResult:
        self.http_calls.append("set_trading_stop")
        self.stop_calls.append({"stop": stop, "target": target, "qty": self.position.qty})
        if self.fail_stop or not self.position.is_open():
            return StopResult(False, "stop rejected")
        self.position.stop = stop
        self.position.target = target
        self.position.stop_confirmed = True
        return StopResult(True, "stop set")

    def clear_stop(self) -> None:
        if self.position.is_open():
            return
        self.position.stop = Decimal("0")
        self.position.target = Decimal("0")
        self.position.stop_confirmed = False

    def close_market(self, qty: Decimal, side: Side, mark: Decimal, ts: int, *, link_id: str = "emergency") -> Exec:
        if not self.position.is_open():
            raise RuntimeError("no position to close")
        self.http_calls.append("market_reduce")
        self.market_closes.append(side.value)
        slip = mark * self.slippage_bps / D(10_000)
        if side is Side.LONG:
            price = mark - slip
        else:
            price = mark + slip
        self.slippage_paid += slip * qty
        exec_ = self._fill_position(side, qty, price, ts, reduce_only=True, maker=False, link_id=link_id)
        self.orders[link_id] = Resting(link_id, side, price, qty, qty, D('0'),
                                     OrderStatus.FILLED, True, [exec_])
        return exec_

    def _fill(self, order: Resting, qty: Decimal, price: Decimal, maker: bool) -> Exec:
        self._exec_n += 1
        fee_rate = self.fees.maker if maker else self.fees.taker
        fee = qty * price * fee_rate
        self.fees_paid += fee
        order.filled += qty
        order.status = OrderStatus.FILLED if order.filled >= order.qty else OrderStatus.PARTIAL
        exec_ = Exec(
            exec_id=f"paper-{self._session_id}-{self._exec_n}",
            link_id=order.link_id,
            side="Buy" if order.side is Side.LONG else "Sell",
            qty=qty,
            price=price,
            fee=fee,
            ts=self.server_ms,
            reduce_only=order.reduce_only,
        )
        order.execs.append(exec_)
        self._apply_position(order.side, qty, price, order.reduce_only)
        return exec_

    def _fill_position(self, side: Side, qty: Decimal, price: Decimal, ts: int, reduce_only: bool, maker: bool, link_id: str) -> Exec:
        self._exec_n += 1
        fee_rate = self.fees.maker if maker else self.fees.taker
        fee = qty * price * fee_rate
        self.fees_paid += fee
        exec_ = Exec(
            exec_id=f"paper-{self._session_id}-{self._exec_n}",
            link_id=link_id,
            side="Buy" if side is Side.LONG else "Sell",
            qty=qty,
            price=price,
            fee=fee,
            ts=ts,
            reduce_only=reduce_only,
        )
        self._apply_position(side, qty, price, reduce_only)
        if reduce_only:
            exec_.side = "Sell" if side is Side.LONG else "Buy"
        return exec_

    def _apply_position(self, side: Side, qty: Decimal, price: Decimal, reduce_only: bool) -> None:
        if reduce_only or (self.position.is_open() and self.position.side is not side):
            if not self.position.is_open():
                return
            close_qty = min(self.position.qty, qty)
            pnl = (price - self.position.avg) * close_qty
            if self.position.side is Side.SHORT:
                pnl = -pnl
            self.realized += pnl
            self.position.qty -= close_qty
            if self.position.qty == 0:
                self.position.side = None
                self.position.avg = Decimal("0")
                self.position.status = "flat"
            return
        if self.position.side is None:
            self.position.side = side
            self.position.qty = qty
            self.position.avg = price
            self.position.status = "open"
            self.position.stop_confirmed = False
            return
        if self.position.side is side:
            new_qty = self.position.qty + qty
            self.position.avg = (self.position.avg * self.position.qty + price * qty) / new_qty
            self.position.qty = new_qty

    def order_status(self, link_id: str) -> OrderStatus | None:
        order = self.orders.get(link_id)
        if order is None:
            return None
        return order.status

    def position_snapshot(self) -> Position:
        src = self.position
        return Position(
            side=src.side,
            qty=src.qty,
            avg=src.avg,
            stop=src.stop,
            target=src.target,
            opened_ms=src.opened_ms,
            stop_confirmed=src.stop_confirmed,
            status=src.status,
        )

    def unrealized(self, mark: Decimal) -> Decimal:
        if not self.position.is_open():
            return Decimal("0")
        move = (mark - self.position.avg) * self.position.qty
        return move if self.position.side is Side.LONG else -move
