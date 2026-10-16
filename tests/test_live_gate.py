from decimal import Decimal

from bybit_scalper.decimals import D
from bybit_scalper.live_gate import live_blockers
from bybit_scalper.report import curve_drawdown
from test_execution import _engine, _market, _plan


def _ok(cfg, **overrides):
    facts = dict(
        margin_verified=True,
        margin_mode="cross",
        leverage=D("2"),
        equity=D("1000"),
        available=D("1000"),
        funding_ok=True,
        clock_skew_ms=0,
        position_open=False,
    )
    facts.update(overrides)
    cfg.allocated_capital = "1000"
    return live_blockers(cfg, **facts)


def test_dedicated_subaccount_shape_is_accepted(cfg):
    assert _ok(cfg) == []


def test_live_refuses_a_wallet_larger_than_the_budget(cfg):
    reasons = _ok(cfg, equity=D("10000"), available=D("10000"))
    assert any("больше денег" in reason for reason in reasons)


def test_live_refuses_budget_above_equity_and_unconfirmed_funding(cfg):
    reasons = _ok(cfg, equity=D("500"), available=D("500"), funding_ok=False)
    assert any("больше equity" in reason for reason in reasons)
    assert any("funding" in reason for reason in reasons)


def test_flat_account_with_locked_margin_is_refused(cfg):
    reasons = _ok(cfg, available=D("100"))
    assert any("доступная маржа" in reason for reason in reasons)
    assert _ok(cfg, available=D("100"), position_open=True) == [] or not any(
        "доступная маржа" in reason for reason in _ok(cfg, available=D("100"), position_open=True)
    )


def test_live_without_a_funding_ledger_does_not_arm_entries(cfg, tmp_path):
    cfg.mode = "live"
    engine, _, _ = _engine(cfg, tmp_path)
    assert engine.live_ready is False
    bars, _plan_built = _plan(cfg)
    now = bars[-1].ts + 60_000
    engine.funding_ledger_confirmed = False
    engine.next_funding_ms = now + 86_400_000
    assert not engine._funding_allows(now)
    engine.on_market(_market(bars, now, mark=bars[-1].c, next_funding_ms=engine.next_funding_ms))
    assert engine.submitted == 0
    assert engine.last_attempt_bar == 0


def test_capital_curve_includes_funding_drawdown():
    net, drawdown = curve_drawdown([(10, Decimal("5")), (30, Decimal("1")), (20, Decimal("-4"))])
    assert net == Decimal("2")
    assert drawdown == Decimal("4")
