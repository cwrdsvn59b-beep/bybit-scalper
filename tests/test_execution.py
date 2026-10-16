from decimal import Decimal

from bybit_scalper.decimals import D
from bybit_scalper.engine import Engine
from bybit_scalper.paper import PaperExchange
from bybit_scalper.risk import build_plan
from bybit_scalper.runner import parse_kline, run_execution_probe, run_offline
from bybit_scalper.signals import evaluate
from bybit_scalper.state import StateStore
from bybit_scalper.synthetic import build_long_fixture
from bybit_scalper.types import BlockReason, BookTop, Exec, MarketState, OrderStatus, Position, Side


def _engine(cfg, tmp_path, queue=Decimal("0")):
    store = StateStore(str(tmp_path / "state.db"))
    paper = PaperExchange(cfg.instrument(), cfg.fallback_fees(), cfg.d("slippage_bps"), queue)
    engine = Engine(cfg, store, paper, cfg.fallback_fees(), cfg.instrument())
    engine.recover(1_700_000_100_000, D("10000"))
    engine.mark = D("100000")
    return engine, paper, store


def _plan(cfg):
    bars = build_long_fixture(cfg)
    signal = evaluate(bars, cfg).signal
    assert signal is not None
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
        bars[-1].ts + 60_000,
        0,
    )
    assert plan.ok
    return bars, plan


def _market(bars, now, **kwargs):
    book = BookTop(D("100"), D("100.2"), D("1"), D("1"), now, True)
    base = dict(
        bars=bars,
        book=book,
        now_ms=now,
        equity=D("10000"),
        mark=bars[-1].c if bars else D("100"),
        stale=False,
        queue_overflow=False,
        connected=True,
    )
    base.update(kwargs)
    return MarketState(**base)


def test_accept_is_not_a_fill_and_partial_is_protected(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    bars, plan = _plan(cfg)
    engine.now_ms = bars[-1].ts + 60_000
    engine.mark = plan.entry
    engine.submit(plan)
    assert engine.order is not None
    assert engine.order.status == OrderStatus.SUBMITTED
    assert not engine.position.is_open()
    partial = (plan.qty / 2).quantize(D("0.001"))
    produced = paper.match_trade(plan.entry, partial)
    assert produced
    assert engine.on_exec(produced[0])
    assert engine.position.qty == partial
    assert engine.position.stop_confirmed
    assert paper.stop_calls[-1]["qty"] == partial
    assert "market_reduce" not in paper.http_calls


def test_cancel_fill_race_does_not_go_market(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 1_700_000_100_000
    engine.mark = plan.entry
    engine.submit(plan)
    paper.inflight_on_cancel = (plan.entry, plan.qty)
    engine.order.opened_ms = engine.now_ms - 4_000
    engine._expire_entry(engine.now_ms)
    assert engine.position.qty == plan.qty
    assert "market_reduce" not in paper.http_calls
    assert engine.order.status == OrderStatus.FILLED


def test_timeout_after_accept_reconciles_without_second_order(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    bars, plan = _plan(cfg)
    engine.now_ms = bars[-1].ts + 60_000
    paper.place_timeout = True
    paper.timeout_still_rests = True
    engine.submit(plan)
    assert engine.order.status == OrderStatus.UNKNOWN
    assert engine.block == BlockReason.UNKNOWN_ORDER
    engine.on_market(_market(bars, engine.now_ms + 1_000))
    assert paper.http_calls.count("place") == 1
    engine.reconcile()
    assert engine.order.status == OrderStatus.SUBMITTED
    assert engine.block == BlockReason.NONE
    assert paper.http_calls.count("place") == 1


def test_duplicate_exec_is_ignored(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 1_700_000_100_000
    engine.mark = plan.entry
    engine.submit(plan)
    produced = paper.match_trade(plan.entry, plan.qty)
    assert engine.on_exec(produced[0])
    qty = engine.position.qty
    assert engine.on_exec(produced[0]) is False
    assert engine.position.qty == qty


def test_disconnect_reconnect_applies_missed_fill_once(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    bars, plan = _plan(cfg)
    engine.now_ms = bars[-1].ts + 60_000
    engine.mark = plan.entry
    engine.submit(plan)
    engine.on_disconnect()
    engine.on_market(_market(bars, engine.now_ms + 500))
    assert paper.http_calls.count("place") == 1
    paper.match_trade(plan.entry, plan.qty)
    assert not engine.position.is_open()
    engine.on_reconnect()
    assert engine.position.qty == plan.qty
    assert engine.block == BlockReason.NONE
    assert paper.http_calls.count("place") == 1


def test_restart_with_open_position_does_not_double_enter(cfg, tmp_path):
    engine, _paper, store = _engine(cfg, tmp_path)
    bars, _plan_obj = _plan(cfg)
    engine.position = Position(
        side=Side.LONG,
        qty=D("0.01"),
        avg=D("100000"),
        stop=D("99000"),
        target=D("102000"),
        opened_ms=1_700_000_100_000,
        stop_confirmed=True,
        status="open",
    )
    store.save_position(engine.position)
    second_paper = PaperExchange(cfg.instrument(), cfg.fallback_fees(), cfg.d("slippage_bps"), Decimal("0"))
    second = Engine(cfg, store, second_paper, cfg.fallback_fees(), cfg.instrument())
    second.recover(1_700_000_100_000, D("10000"))
    second.on_market(_market(bars, 1_700_000_160_000))
    assert second.position.qty == D("0.01")
    assert second_paper.http_calls.count("place") == 0


def test_stop_failure_flattens_and_blocks_entries(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    bars, plan = _plan(cfg)
    engine.now_ms = bars[-1].ts + 60_000
    engine.mark = plan.entry
    paper.fail_stop = True
    engine.submit(plan)
    produced = paper.match_trade(plan.entry, plan.qty)
    engine.on_exec(produced[0])
    assert engine.block == BlockReason.STOP
    assert not engine.position.is_open()
    assert paper.market_closes
    engine.on_market(_market(bars, engine.now_ms + 60_000, mark=plan.entry))
    assert paper.http_calls.count("place") == 1


def test_daily_loss_persists_across_restart(cfg, tmp_path):
    engine, _paper, store = _engine(cfg, tmp_path)
    now = 1_700_000_100_000
    engine.now_ms = now
    engine.mark = D("100")
    engine.exit_reason = "test"
    engine.position = Position(
        side=Side.LONG,
        qty=D("1"),
        avg=D("100"),
        stop=D("90"),
        target=D("130"),
        opened_ms=now,
        stop_confirmed=True,
        status="open",
    )
    store.save_position(engine.position)
    assert engine.on_exec(
        Exec("loss-1", "manual", "Sell", D("1"), D("0"), D("0"), now, reduce_only=True)
    )
    assert engine.block == BlockReason.DAILY
    restarted = Engine(cfg, store, PaperExchange(cfg.instrument(), cfg.fallback_fees(), D("1"), D("0")), cfg.fallback_fees(), cfg.instrument())
    restarted.recover(now, D("50000"))
    assert restarted.start_equity == D("10000")
    assert restarted.realized <= D("-100")
    assert restarted.block == BlockReason.DAILY


def test_four_losses_pause_persists(cfg, tmp_path):
    engine, _paper, store = _engine(cfg, tmp_path)
    now = 1_700_000_100_000
    for index in range(4):
        engine.now_ms = now + index
        engine.mark = D("100")
        engine.exit_reason = "test"
        engine.position = Position(
            side=Side.LONG,
            qty=D("1"),
            avg=D("100"),
            stop=D("90"),
            target=D("130"),
            opened_ms=now,
            stop_confirmed=True,
            status="open",
        )
        assert engine.on_exec(
            Exec(f"loss-{index}", "manual", "Sell", D("1"), D("99"), D("0"), now + index, reduce_only=True)
        )
    engine.on_market(_market([], now + 4, mark=D("100"), book=BookTop(D("0"), D("0"), D("0"), D("0"), now, False)))
    assert engine.consec_losses == 4
    assert engine.block == BlockReason.PAUSE
    restarted = Engine(cfg, store, PaperExchange(cfg.instrument(), cfg.fallback_fees(), D("1"), D("0")), cfg.fallback_fees(), cfg.instrument())
    restarted.recover(now + 10, D("10000"))
    assert restarted.block == BlockReason.PAUSE
    assert restarted.pause_until > now


def test_candle_wick_is_not_a_fill(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    bars, plan = _plan(cfg)
    engine.now_ms = bars[-1].ts + 60_000
    engine.mark = plan.entry
    engine.submit(plan)
    wick = bars[-1]
    wicked = type(wick)(wick.ts + 60_000, plan.entry, plan.entry, plan.entry - D("50"), plan.entry + D("1"), D("3"))
    engine.on_market(_market(bars + [wicked], engine.now_ms + 1_000, mark=wicked.c))
    assert engine.order.filled == 0
    assert not engine.position.is_open()
    engine.on_trade_print(plan.entry, plan.qty, engine.now_ms + 1_000, simulate_fill=True)
    assert engine.position.is_open()


def test_unfilled_timeout_cancels_without_market(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 5_000
    engine.mark = plan.entry
    engine.submit(plan)
    engine.order.opened_ms = 0
    engine._expire_entry(5_000)
    assert engine.order.status == OrderStatus.CANCELLED
    assert not engine.position.is_open()
    assert "market_reduce" not in paper.http_calls


def test_kline_message_exposes_confirm_flag():
    candle, confirmed = parse_kline(
        {
            "topic": "kline.1.BTCUSDT",
            "data": [
                {
                    "start": 60_000,
                    "open": "1",
                    "high": "3",
                    "low": "0.5",
                    "close": "2",
                    "volume": "4",
                    "confirm": False,
                }
            ],
        }
    )
    assert candle.l == D("0.5")
    assert confirmed is False


def test_offline_report_and_probe_are_labeled(cfg, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    text = run_offline(cfg)
    assert "Предварительный отчёт: да" in text
    assert "не доказательство прибыли" in text
    probe = run_execution_probe(cfg)
    assert "не доходность" in probe
    assert "Стоп подтверждён до выхода: True" in probe
