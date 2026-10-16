from __future__ import annotations

import secrets
import time
from decimal import Decimal

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D, ceil_tick, floor_step, floor_tick
from bybit_scalper.types import Fees, Instrument, Plan, Side, Signal


def entry_fee_rate(cfg: Settings, fees: Fees) -> Decimal:
    if cfg.entry_fee_assumption == "maker":
        return fees.maker
    return fees.taker


def funding_crosses(now_ms: int, hold_ms: int, next_funding_ms: int) -> bool:
    if next_funding_ms <= 0:
        return False
    return now_ms < next_funding_ms <= now_ms + hold_ms


def unit_loss(
    entry: Decimal,
    stop: Decimal,
    fee_in: Decimal,
    fee_out: Decimal,
    slip: Decimal,
    funding_per_unit: Decimal,
) -> Decimal:
    return abs(entry - stop) + entry * fee_in + stop * fee_out + stop * slip + funding_per_unit


def min_target(
    side: Side,
    entry: Decimal,
    loss_per_unit: Decimal,
    multiple: Decimal,
    fee_in: Decimal,
    fee_out: Decimal,
    slip: Decimal,
    funding_per_unit: Decimal,
) -> Decimal:
    """Минимальная цена цели: чистая прибыль >= multiple * плановый убыток."""
    need = multiple * loss_per_unit + entry * fee_in + funding_per_unit
    if side is Side.LONG:
        denom = Decimal("1") - fee_out - slip
        if denom <= 0:
            raise ValueError("комиссия и проскальзывание съедают цель")
        return (need + entry) / denom
    denom = Decimal("1") + fee_out + slip
    return (entry - need) / denom


def economic_net(
    side: Side,
    entry: Decimal,
    target: Decimal,
    qty: Decimal,
    fee_in: Decimal,
    fee_out: Decimal,
    slip: Decimal,
    funding_per_unit: Decimal,
) -> Decimal:
    gross = (target - entry) if side is Side.LONG else (entry - target)
    costs = entry * fee_in + target * fee_out + target * slip + funding_per_unit
    return qty * (gross - costs)


def build_plan(
    signal: Signal,
    bid: Decimal,
    ask: Decimal,
    bid_qty: Decimal,
    ask_qty: Decimal,
    book_valid: bool,
    equity: Decimal,
    fees: Fees,
    instrument: Instrument,
    cfg: Settings,
    funding_rate: Decimal,
    now_ms: int,
    next_funding_ms: int,
    link_id: str | None = None,
) -> Plan:
    if not book_valid or bid <= 0 or ask <= 0 or ask <= bid:
        return Plan(False, "стакан недействителен")
    mid = (bid + ask) / D(2)
    spread_bps = (ask - bid) / mid * D(10_000)
    if spread_bps > cfg.d("max_spread_bps"):
        return Plan(False, "спред шире порога")
    if bid_qty < cfg.d("min_depth_qty") or ask_qty < cfg.d("min_depth_qty"):
        return Plan(False, "недостаточная глубина стакана")
    if equity <= 0:
        return Plan(False, "нет капитала")

    entry = floor_tick(bid, instrument.tick) if signal.side is Side.LONG else ceil_tick(ask, instrument.tick)
    if signal.side is Side.LONG and entry >= ask:
        return Plan(False, "post-only buy пересёк бы ask")
    if signal.side is Side.SHORT and entry <= bid:
        return Plan(False, "post-only sell пересёк бы bid")

    buffer = signal.atr * cfg.d("stop_atr_buffer")
    if signal.side is Side.LONG:
        stop = floor_tick(signal.stop_ref - buffer, instrument.tick)
        if stop >= entry:
            return Plan(False, "стоп не ниже входа")
    else:
        stop = ceil_tick(signal.stop_ref + buffer, instrument.tick)
        if stop <= entry:
            return Plan(False, "стоп не выше входа")
    if stop <= 0:
        return Plan(False, "цена стопа должна быть положительной")

    fee_in = entry_fee_rate(cfg, fees)
    fee_out = fees.taker
    slip = cfg.d("slippage_bps") / D(10_000)
    funding_per_unit = Decimal("0")
    if funding_crosses(now_ms, cfg.max_hold_sec * 1000, next_funding_ms):
        funding_per_unit = abs(funding_rate) * entry
    loss_1 = unit_loss(entry, stop, fee_in, fee_out, slip, funding_per_unit)
    if loss_1 <= 0:
        return Plan(False, "нулевой риск")
    try:
        raw_target = min_target(
            signal.side,
            entry,
            loss_1,
            cfg.d("reward_multiple"),
            fee_in,
            fee_out,
            slip,
            funding_per_unit,
        )
    except ValueError as exc:
        return Plan(False, str(exc))
    if signal.side is Side.LONG:
        target = ceil_tick(raw_target, instrument.tick)
        if signal.opposite < target:
            return Plan(False, "противоположный уровень ближе минимальной цели")
    else:
        target = floor_tick(raw_target, instrument.tick)
        if target <= 0:
            return Plan(False, "цена цели должна быть положительной")
        if signal.opposite > target:
            return Plan(False, "противоположный уровень ближе минимальной цели")

    budget = equity * cfg.d("risk_pct")
    qty = floor_step(budget / loss_1, instrument.qty_step)
    max_notional = equity * cfg.d("max_leverage")
    capped = floor_step(max_notional / entry, instrument.qty_step)
    if qty > capped:
        qty = capped
    if instrument.max_qty > 0:
        qty = min(qty, floor_step(instrument.max_qty, instrument.qty_step))
    if qty < instrument.min_qty or qty * entry < instrument.min_notional:
        min_loss = instrument.min_qty * loss_1
        return Plan(False, "минимальный ордер превышает допустимый риск" if min_loss > budget else "объём ниже минимума")
    if qty * loss_1 > budget * D("1.0000001"):
        return Plan(False, "округление превысило риск")

    link = link_id or new_link_id()
    return Plan(
        True,
        "план принят",
        side=signal.side,
        entry=entry,
        stop=stop,
        target=target,
        qty=qty,
        link_id=link,
        bar_ts=signal.bar_ts,
    )


def new_link_id() -> str:
    raw = f"sc{int(time.time() * 1000)}{secrets.token_hex(3)}"
    return raw[:36]
