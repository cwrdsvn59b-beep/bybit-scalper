from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


class OrderStatus(str, Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class BlockReason(str, Enum):
    NONE = ""
    STALE = "stale_data"
    QUEUE = "queue_overflow"
    DAILY = "daily_loss"
    PAUSE = "loss_streak_pause"
    STOP = "stop_unconfirmed"
    UNKNOWN_ORDER = "unknown_order"
    UNKNOWN_POSITION = "unknown_position"
    DISCONNECT = "disconnected"


@dataclass(frozen=True)
class Candle:
    ts: int
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    v: Decimal


@dataclass(frozen=True)
class Instrument:
    symbol: str
    tick: Decimal
    qty_step: Decimal
    min_qty: Decimal
    min_notional: Decimal


@dataclass(frozen=True)
class Fees:
    maker: Decimal
    taker: Decimal
    source: str


@dataclass(frozen=True)
class BookTop:
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    ts: int
    valid: bool


@dataclass
class Signal:
    side: Side
    stop_ref: Decimal
    atr: Decimal
    opposite: Decimal
    bar_ts: int


@dataclass
class Decision:
    accepted: bool
    reason: str
    signal: Signal | None = None


@dataclass
class Plan:
    ok: bool
    reason: str
    side: Side | None = None
    entry: Decimal | None = None
    stop: Decimal | None = None
    target: Decimal | None = None
    qty: Decimal | None = None
    link_id: str = ""
    bar_ts: int = 0


@dataclass
class Exec:
    exec_id: str
    link_id: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Decimal
    ts: int
    reduce_only: bool = False


@dataclass
class OrderView:
    link_id: str
    status: OrderStatus
    side: Side
    price: Decimal
    qty: Decimal
    filled: Decimal = Decimal("0")
    stop: Decimal = Decimal("0")
    target: Decimal = Decimal("0")
    opened_ms: int = 0
    bar_ts: int = 0

    @property
    def active(self) -> bool:
        return self.status in {
            OrderStatus.NEW,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIAL,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.UNKNOWN,
        }


@dataclass
class Position:
    side: Side | None = None
    qty: Decimal = Decimal("0")
    avg: Decimal = Decimal("0")
    stop: Decimal = Decimal("0")
    target: Decimal = Decimal("0")
    opened_ms: int = 0
    stop_confirmed: bool = False
    status: str = "flat"

    def is_open(self) -> bool:
        return self.qty > 0 and self.side is not None


@dataclass
class MarketState:
    bars: list[Candle]
    book: BookTop
    now_ms: int
    equity: Decimal
    mark: Decimal
    stale: bool = False
    queue_overflow: bool = False
    funding_rate: Decimal = Decimal("0")
    next_funding_ms: int = 0
    connected: bool = True


@dataclass
class PlaceResult:
    accepted: bool
    link_id: str
    message: str
    order_id: str = ""


@dataclass
class StopResult:
    ok: bool
    message: str


@dataclass
class ClosedTrade:
    ts: int
    side: str
    qty: Decimal
    entry: Decimal
    exit: Decimal
    pnl: Decimal
    fees: Decimal
    slippage: Decimal
    reason: str


@dataclass
class Report:
    label: str
    net_pnl: Decimal
    expectancy: Decimal
    profit_factor: str
    max_drawdown: Decimal
    trades: int
    submitted: int
    filled_entries: int
    fill_rate: str
    fees: Decimal
    slippage: Decimal
    preliminary: bool
    notes: list[str] = field(default_factory=list)
