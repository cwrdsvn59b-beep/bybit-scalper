from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.risk import build_plan
from bybit_scalper.signals import evaluate
from bybit_scalper.state import StateStore
from bybit_scalper.types import (
    BlockReason,
    ClosedTrade,
    Exec,
    Fees,
    Instrument,
    MarketState,
    OrderStatus,
    OrderView,
    Plan,
    Position,
    Side,
)

log = logging.getLogger("bybit_scalper")

_HARD = {
    BlockReason.STOP,
    BlockReason.DAILY,
    BlockReason.UNKNOWN_ORDER,
    BlockReason.UNKNOWN_POSITION,
}


def utc_day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


class Engine:
    """Состояния заявки и позиции. Принятие HTTP не считается исполнением."""

    def __init__(self, cfg: Settings, store: StateStore, exchange, fees: Fees, instrument: Instrument) -> None:
        self.cfg = cfg
        self.store = store
        self.exchange = exchange
        self.fees = fees
        self.instrument = instrument
        self.order: OrderView | None = None
        self.position = Position()
        self.block = BlockReason.NONE
        self.block_note = ""
        self.realized = Decimal("0")
        self.start_equity = Decimal("0")
        self.day = ""
        self.consec_losses = 0
        self.pause_until = 0
        self.submitted = 0
        self.filled_entries = 0
        self.signal_count = 0
        self.connected = True
        self.now_ms = 0
        self.mark = Decimal("0")
        self.clock_skew_ms = 0
        self.rate_limited = False
        self.last_attempt_bar = 0
        self.exit_reason = ""
        self.open_fees = Decimal("0")
        self.round_pnl = Decimal("0")
        self.round_fees = Decimal("0")
        self.round_slip = Decimal("0")
        self.exit_notional = Decimal("0")
        self.closed_qty = Decimal("0")
        self.audit: list[tuple[int, str]] = []
        self._reqs: deque[int] = deque()
        self._flattening = False

    def recover(self, now_ms: int, equity: Decimal) -> None:
        self.now_ms = now_ms
        self.position = self.store.load_position()
        self.order = self.store.load_order()
        self.submitted = int(self.store.get_kv("submitted", "0") or "0")
        self.filled_entries = int(self.store.get_kv("filled_entries", "0") or "0")
        self.consec_losses = int(self.store.get_kv("consec_losses", "0") or "0")
        self.pause_until = int(self.store.get_kv("pause_until", "0") or "0")
        saved = self.store.get_kv("block", "")
        self.block = BlockReason(saved) if saved else BlockReason.NONE
        self.block_note = self.store.get_kv("block_note", "")
        self._load_round()
        self._ensure_day(now_ms, equity)
        self._refresh_pause(now_ms)
        if self.position.is_open() and not self.position.stop_confirmed:
            if self.mark <= 0:
                self.mark = self.position.avg
            self._protect()

    def on_disconnect(self) -> None:
        self.connected = False
        self._block(BlockReason.DISCONNECT, "ws")

    def on_reconnect(self) -> None:
        self.connected = True
        self.reconcile()
        remote = self.exchange.position_snapshot()
        if self.position.qty == remote.qty and self.block == BlockReason.DISCONNECT:
            self._clear_block()

    def note_clock(self, local_ms: int, server_ms: int) -> None:
        self.clock_skew_ms = abs(local_ms - server_ms)

    def note_request(self, now_ms: int) -> None:
        self._reqs.append(now_ms)
        while self._reqs and now_ms - self._reqs[0] >= 1000:
            self._reqs.popleft()
        self.rate_limited = len(self._reqs) > self.cfg.max_requests_per_sec

    def on_market(self, market: MarketState) -> None:
        self.now_ms = market.now_ms
        self.mark = market.mark
        self._ensure_day(market.now_ms, market.equity)
        self._refresh_pause(market.now_ms)
        if not market.connected or not self.connected:
            self.connected = market.connected and self.connected
            self._block(BlockReason.DISCONNECT, "disconnected")
        elif market.queue_overflow:
            self._block(BlockReason.QUEUE, "queue")
        elif market.stale or self.clock_skew_ms > self.cfg.max_clock_skew_ms or self.rate_limited:
            note = "stale"
            if self.clock_skew_ms > self.cfg.max_clock_skew_ms:
                note = "clock_skew"
            elif self.rate_limited:
                note = "rate_limit"
            self._block(BlockReason.STALE, note)
        elif self.block in {BlockReason.STALE, BlockReason.QUEUE, BlockReason.DISCONNECT}:
            self.connected = True
            self._clear_block()
        self._expire_entry(market.now_ms)
        hold_ms = self.cfg.max_hold_sec * 1000
        if (
            self.position.is_open()
            and self.position.opened_ms
            and market.now_ms - self.position.opened_ms >= hold_ms
        ):
            self.exit_reason = "max_hold"
            self._flatten(market.mark)
        self._check_daily(market.mark)
        self._maybe_enter(market)

    def on_trade_print(self, price: Decimal, qty: Decimal, ts: int, simulate_fill: bool) -> None:
        """Лента сделок. Фитиль свечи сюда передавать нельзя."""
        self.mark = price
        self.now_ms = ts or self.now_ms
        if simulate_fill and hasattr(self.exchange, "match_trade"):
            for ex in self.exchange.match_trade(price, qty):
                self.on_exec(ex)
            self._paper_protection(price, ts)

    def on_exec(self, ex: Exec) -> bool:
        if not self.store.add_exec(ex.exec_id, ex.link_id, ex.qty, ex.price, ex.fee, ex.side, ex.ts):
            return False
        self._apply_fill(ex)
        return True

    def submit(self, plan: Plan) -> None:
        if not plan.ok or plan.side is None or plan.entry is None or plan.qty is None:
            return
        if self.block != BlockReason.NONE:
            return
        if self.position.is_open() or (self.order is not None and self.order.active):
            return
        self.order = OrderView(
            link_id=plan.link_id,
            status=OrderStatus.NEW,
            side=plan.side,
            price=plan.entry,
            qty=plan.qty,
            stop=plan.stop or Decimal("0"),
            target=plan.target or Decimal("0"),
            opened_ms=self.now_ms,
            bar_ts=plan.bar_ts,
        )
        self.store.save_order(self.order)
        try:
            result = self.exchange.place_limit(plan.link_id, plan.side, plan.qty, plan.entry, False)
        except TimeoutError:
            self.order.status = OrderStatus.UNKNOWN
            self._block(BlockReason.UNKNOWN_ORDER, "place timeout")
            self.store.save_order(self.order)
            log.warning("таймаут place_order, заявка %s в неизвестном состоянии", plan.link_id)
            return
        if not result.accepted:
            self.order.status = OrderStatus.REJECTED
            self.store.save_order(self.order)
            self.order = None
            log.info("заявка отклонена: %s", result.message)
            return
        self.order.status = OrderStatus.SUBMITTED
        self.submitted += 1
        self.audit.append((plan.bar_ts, "submit"))
        self.store.set_kv("submitted", str(self.submitted))
        self.store.save_order(self.order)
        log.info("заявка принята, ещё не исполнена %s %s %s", plan.side.value, plan.qty, plan.entry)

    def reconcile(self) -> None:
        if self.order is not None:
            for ex in self.exchange.executions(self.order.link_id):
                self.on_exec(ex)
            status_fn = getattr(self.exchange, "order_status", None)
            if self.order.status == OrderStatus.UNKNOWN and callable(status_fn):
                remote = status_fn(self.order.link_id)
                if remote is not None and remote != OrderStatus.UNKNOWN:
                    self.order.status = remote
                    self.store.save_order(self.order)
                    if self.block == BlockReason.UNKNOWN_ORDER:
                        self._clear_block()
                elif remote is None:
                    self._block(BlockReason.UNKNOWN_ORDER, "order not found")
        snap = getattr(self.exchange, "position_snapshot", None)
        if callable(snap):
            remote_pos = snap()
            if remote_pos.qty != self.position.qty:
                self._block(BlockReason.UNKNOWN_POSITION, "position mismatch")

    def note_remote_status(self, link_id: str, status: OrderStatus) -> None:
        if self.order is None or self.order.link_id != link_id:
            return
        for ex in self.exchange.executions(link_id):
            self.on_exec(ex)
        if status in {OrderStatus.CANCELLED, OrderStatus.REJECTED}:
            if self.order.filled >= self.order.qty and self.order.qty > 0:
                self.order.status = OrderStatus.FILLED
            else:
                self.order.status = status
            self.store.save_order(self.order)

    def _maybe_enter(self, market: MarketState) -> None:
        if self.block != BlockReason.NONE or not self.connected:
            return
        if not self.cfg.symbol_allowed():
            return
        if self.position.is_open() or (self.order is not None and self.order.active):
            return
        if not market.book.valid or market.stale or market.queue_overflow or not market.connected:
            return
        if self.clock_skew_ms > self.cfg.max_clock_skew_ms or self.rate_limited:
            return
        if not market.bars:
            return
        bar_ts = market.bars[-1].ts
        if bar_ts == self.last_attempt_bar or len(market.bars) < self.cfg.min_1m_bars:
            return
        self.last_attempt_bar = bar_ts
        decision = evaluate(market.bars, self.cfg)
        if not decision.accepted or decision.signal is None:
            return
        self.signal_count += 1
        self.audit.append((bar_ts, "signal"))
        plan = build_plan(
            decision.signal,
            market.book.bid,
            market.book.ask,
            market.book.bid_qty,
            market.book.ask_qty,
            market.book.valid,
            market.equity,
            self.fees,
            self.instrument,
            self.cfg,
            market.funding_rate,
            market.now_ms,
            market.next_funding_ms,
        )
        if not plan.ok:
            log.info("пропуск: %s", plan.reason)
            return
        self.submit(plan)

    def _expire_entry(self, now_ms: int) -> None:
        order = self.order
        if order is None or order.status not in {OrderStatus.SUBMITTED, OrderStatus.PARTIAL}:
            return
        if now_ms - order.opened_ms < self.cfg.entry_timeout_sec * 1000:
            return
        order.status = OrderStatus.CANCEL_PENDING
        self.store.save_order(order)
        for ex in self.exchange.executions(order.link_id):
            self.on_exec(ex)
        try:
            self.exchange.cancel(order.link_id)
        except Exception as exc:
            order.status = OrderStatus.UNKNOWN
            self._block(BlockReason.UNKNOWN_ORDER, f"cancel failed: {exc}")
            self.store.save_order(order)
            return
        for ex in self.exchange.executions(order.link_id):
            self.on_exec(ex)
        if self.order is None:
            return
        if self.order.filled >= self.order.qty and self.order.qty > 0:
            self.order.status = OrderStatus.FILLED
        else:
            self.order.status = OrderStatus.CANCELLED
        self.store.save_order(self.order)

    def _apply_fill(self, ex: Exec) -> None:
        order_side = Side.LONG if ex.side.lower() == "buy" else Side.SHORT
        reduce = ex.reduce_only or (self.position.is_open() and self.position.side is not order_side)
        if reduce:
            self._reduce(ex)
            return
        if self.position.is_open() and (self.order is None or ex.link_id != self.order.link_id):
            self._block(BlockReason.UNKNOWN_POSITION, "averaging forbidden")
            return
        if not self.position.is_open():
            self.position = Position(
                side=order_side,
                qty=ex.qty,
                avg=ex.price,
                stop=self.order.stop if self.order else Decimal("0"),
                target=self.order.target if self.order else Decimal("0"),
                opened_ms=ex.ts or self.now_ms,
                stop_confirmed=False,
                status="open",
            )
            self.open_fees = ex.fee
            self.round_pnl = Decimal("0")
            self.round_fees = Decimal("0")
            self.round_slip = Decimal("0")
            self.exit_notional = Decimal("0")
            self.closed_qty = Decimal("0")
        else:
            new_qty = self.position.qty + ex.qty
            self.position.avg = (self.position.avg * self.position.qty + ex.price * ex.qty) / new_qty
            self.position.qty = new_qty
            self.open_fees += ex.fee
        if self.order is not None and ex.link_id == self.order.link_id:
            before = self.order.filled
            self.order.filled += ex.qty
            self.order.status = (
                OrderStatus.FILLED if self.order.filled >= self.order.qty else OrderStatus.PARTIAL
            )
            if before == 0 and self.order.filled > 0:
                self.filled_entries += 1
                self.audit.append((self.order.bar_ts, "fill"))
                self.store.set_kv("filled_entries", str(self.filled_entries))
            self.store.save_order(self.order)
        self._save_position()
        if self.position.is_open() and not self.position.stop_confirmed:
            self._protect()

    def _reduce(self, ex: Exec) -> None:
        if not self.position.is_open():
            self._block(BlockReason.UNKNOWN_POSITION, "reduce without position")
            return
        if ex.qty > self.position.qty:
            self._block(BlockReason.UNKNOWN_POSITION, "reduce larger than position")
        qty = min(self.position.qty, ex.qty)
        gross = (ex.price - self.position.avg) * qty
        if self.position.side is Side.SHORT:
            gross = -gross
        frac = qty / self.position.qty
        fee_part = self.open_fees * frac
        self.open_fees -= fee_part
        net = gross - fee_part - ex.fee
        slip = abs(self.mark - ex.price) * qty if self.mark > 0 else Decimal("0")
        self.round_pnl += net
        self.round_fees += fee_part + ex.fee
        self.round_slip += slip
        self.exit_notional += ex.price * qty
        self.closed_qty += qty
        self.realized += net
        side_name = self.position.side.value if self.position.side else ""
        entry = self.position.avg
        self.position.qty -= qty
        if self.position.qty <= 0:
            exit_px = self.exit_notional / self.closed_qty if self.closed_qty else ex.price
            trade = ClosedTrade(
                ts=ex.ts or self.now_ms,
                side=side_name,
                qty=self.closed_qty,
                entry=entry,
                exit=exit_px,
                pnl=self.round_pnl,
                fees=self.round_fees,
                slippage=self.round_slip,
                reason=self.exit_reason or "reduce",
            )
            self.store.add_trade(trade)
            if self.round_pnl < 0:
                self.consec_losses += 1
                if self.consec_losses >= self.cfg.max_consecutive_losses:
                    self.pause_until = (ex.ts or self.now_ms) + self.cfg.pause_after_losses_sec * 1000
            else:
                self.consec_losses = 0
            self.position = Position()
            self.open_fees = Decimal("0")
            self.round_pnl = Decimal("0")
            self.round_fees = Decimal("0")
            self.round_slip = Decimal("0")
            self.exit_notional = Decimal("0")
            self.closed_qty = Decimal("0")
            self.exit_reason = ""
            self._save_position()
            self._persist_risk()
            try:
                self.exchange.clear_stop()
            except Exception:
                log.warning("не удалось снять стоп после закрытия")
            self._check_daily(self.mark)
        else:
            self._save_position()
            self._persist_risk()

    def _protect(self) -> None:
        if not self.position.is_open() or self._flattening:
            return
        if self.position.stop <= 0 or self.position.target <= 0:
            self._block(BlockReason.STOP, "no stop price")
            self.exit_reason = "stop_missing"
            self._flatten(self.mark if self.mark > 0 else self.position.avg)
            return
        result = self.exchange.set_stop(self.position.stop, self.position.target)
        if result.ok:
            self.position.stop_confirmed = True
            self._save_position()
            return
        self.position.stop_confirmed = False
        self._save_position()
        self._block(BlockReason.STOP, result.message)
        self.exit_reason = "stop_failed"
        self._flatten(self.mark if self.mark > 0 else self.position.avg)

    def _flatten(self, mark: Decimal) -> None:
        if not self.position.is_open() or self._flattening:
            return
        self._flattening = True
        try:
            ex = self.exchange.close_market(self.position.qty, self.position.side, mark, self.now_ms)
        except Exception as exc:
            self._block(BlockReason.UNKNOWN_POSITION, f"flatten failed: {exc}")
            log.error("аварийное закрытие не подтверждено")
            return
        finally:
            self._flattening = False
        self.on_exec(ex)

    def _paper_protection(self, price: Decimal, ts: int) -> None:
        if not self.position.is_open() or not self.position.stop_confirmed or self.position.side is None:
            return
        if self.position.side is Side.LONG:
            hit_stop = price <= self.position.stop
            hit_target = price >= self.position.target
        else:
            hit_stop = price >= self.position.stop
            hit_target = price <= self.position.target
        if not hit_stop and not hit_target:
            return
        self.now_ms = ts or self.now_ms
        self.mark = price
        self.exit_reason = "stop" if hit_stop else "target"
        self._flatten(price)

    def _check_daily(self, mark: Decimal) -> None:
        if self.start_equity <= 0:
            return
        limit = -self.start_equity * self.cfg.d("daily_loss_pct")
        if self._equity_pnl(mark) <= limit:
            self._block(BlockReason.DAILY, "daily_loss")
            if self.position.is_open():
                self.exit_reason = "daily_loss"
                self._flatten(mark)

    def _equity_pnl(self, mark: Decimal) -> Decimal:
        if not self.position.is_open():
            return self.realized
        gross = (mark - self.position.avg) * self.position.qty
        if self.position.side is Side.SHORT:
            gross = -gross
        exit_fee = self.position.qty * mark * self.fees.taker
        return self.realized + gross - exit_fee

    def _ensure_day(self, now_ms: int, equity: Decimal) -> None:
        day = utc_day(now_ms)
        if day == self.day:
            return
        info = self.store.ensure_day(day, equity)
        new_day = self.day != "" and day != self.day
        self.day = day
        self.start_equity = info["start_equity"]
        self.realized = info["realized"]
        if new_day and self.block == BlockReason.DAILY:
            self._clear_block()

    def _refresh_pause(self, now_ms: int) -> None:
        if self.consec_losses >= self.cfg.max_consecutive_losses and now_ms < self.pause_until:
            self._block(BlockReason.PAUSE, "loss_streak")
        elif self.block == BlockReason.PAUSE and now_ms >= self.pause_until:
            self._clear_block()

    def _persist_risk(self) -> None:
        if not self.day:
            return
        self.store.update_day(self.day, self.realized, self.consec_losses, self.pause_until)
        self.store.set_kv("consec_losses", str(self.consec_losses))
        self.store.set_kv("pause_until", str(self.pause_until))
        self._save_round()

    def _save_position(self) -> None:
        self.store.save_position(self.position)
        self._save_round()

    def _save_round(self) -> None:
        payload = {
            "open_fees": str(self.open_fees),
            "round_pnl": str(self.round_pnl),
            "round_fees": str(self.round_fees),
            "round_slip": str(self.round_slip),
            "exit_notional": str(self.exit_notional),
            "closed_qty": str(self.closed_qty),
        }
        self.store.set_kv("round", json.dumps(payload))

    def _load_round(self) -> None:
        raw = self.store.get_kv("round", "")
        if not raw:
            return
        payload = json.loads(raw)
        self.open_fees = D(payload.get("open_fees", "0"))
        self.round_pnl = D(payload.get("round_pnl", "0"))
        self.round_fees = D(payload.get("round_fees", "0"))
        self.round_slip = D(payload.get("round_slip", "0"))
        self.exit_notional = D(payload.get("exit_notional", "0"))
        self.closed_qty = D(payload.get("closed_qty", "0"))

    def _block(self, reason: BlockReason, note: str = "") -> None:
        if self.block in _HARD and reason not in _HARD:
            return
        self.block = reason
        self.block_note = note
        self.store.set_kv("block", reason.value)
        self.store.set_kv("block_note", note)

    def _clear_block(self) -> None:
        self.block = BlockReason.NONE
        self.block_note = ""
        self.store.set_kv("block", "")
        self.store.set_kv("block_note", "")
