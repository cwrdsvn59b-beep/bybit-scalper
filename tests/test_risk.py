from decimal import Decimal

from bybit_scalper.decimals import D, ceil_tick, floor_step, floor_tick
from bybit_scalper.risk import build_plan, economic_net, entry_fee_rate, min_target, unit_loss
from bybit_scalper.signals import evaluate
from bybit_scalper.synthetic import build_long_fixture
from bybit_scalper.types import Side, Signal


def _signal():
    return Signal(side=Side.LONG, stop_ref=D("99900"), atr=D("50"), opposite=D("110000"), bar_ts=1)


def test_risk_matches_hand_formula(cfg):
    fees = cfg.fallback_fees()
    instrument = cfg.instrument()
    bid, ask = D("100000"), D("100000.2")
    plan = build_plan(
        _signal(),
        bid,
        ask,
        D("1"),
        D("1"),
        True,
        D("10000"),
        fees,
        instrument,
        cfg,
        D("0"),
        1_700_000_000_000,
        0,
    )
    assert plan.ok
    entry = floor_tick(bid, instrument.tick)
    buffer = D("50") * cfg.d("stop_atr_buffer")
    stop = floor_tick(D("99900") - buffer, instrument.tick)
    fee_in = entry_fee_rate(cfg, fees)
    fee_out = fees.taker
    slip = cfg.d("slippage_bps") / D(10_000)
    loss = unit_loss(entry, stop, fee_in, fee_out, slip, Decimal("0"))
    raw = min_target(Side.LONG, entry, loss, cfg.d("reward_multiple"), fee_in, fee_out, slip, Decimal("0"))
    target = ceil_tick(raw, instrument.tick)
    budget = D("10000") * cfg.d("risk_pct")
    qty = floor_step(budget / loss, instrument.qty_step)
    assert plan.entry == entry
    assert plan.stop == stop
    assert plan.target == target
    assert plan.qty == qty
    net = economic_net(Side.LONG, entry, target, qty, fee_in, fee_out, slip, Decimal("0"))
    assert net >= cfg.d("reward_multiple") * qty * loss


def test_min_order_above_risk_is_skipped(cfg):
    plan = build_plan(
        _signal(),
        D("100000"),
        D("100000.2"),
        D("1"),
        D("1"),
        True,
        D("20"),
        cfg.fallback_fees(),
        cfg.instrument(),
        cfg,
        D("0"),
        1,
        0,
    )
    assert not plan.ok
    assert "минимальный ордер" in plan.reason


def test_funding_widens_target(cfg):
    fees = cfg.fallback_fees()
    instrument = cfg.instrument()
    now = 1_700_000_000_000
    common = dict(
        signal=_signal(),
        bid=D("100000"),
        ask=D("100000.2"),
        bid_qty=D("1"),
        ask_qty=D("1"),
        book_valid=True,
        equity=D("10000"),
        fees=fees,
        instrument=instrument,
        cfg=cfg,
        now_ms=now,
    )
    plain = build_plan(**common, funding_rate=D("0"), next_funding_ms=0)
    funded = build_plan(**common, funding_rate=D("0.0001"), next_funding_ms=now + 60_000)
    assert plain.ok and funded.ok
    assert funded.target > plain.target


def test_near_opposite_level_skips(cfg):
    bars = build_long_fixture(cfg)
    signal = evaluate(bars, cfg).signal
    assert signal is not None
    signal.opposite = signal.stop_ref
    plan = build_plan(
        signal,
        bars[-1].c - D("0.1"),
        bars[-1].c + D("0.1"),
        D("1"),
        D("1"),
        True,
        D("10000"),
        cfg.fallback_fees(),
        cfg.instrument(),
        cfg,
        D("0"),
        bars[-1].ts,
        0,
    )
    assert not plan.ok
    assert "противоположный" in plan.reason
