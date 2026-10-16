import os
from pathlib import Path

import pytest

from bybit_scalper.config import load_settings
from bybit_scalper.decimals import D
from bybit_scalper.exchange import PybitBroker
from bybit_scalper.ledger import cashflow_from_transaction, funding_from_execution
from bybit_scalper.runner import PrivateQueue, collect_market, replay_market, take_private_then_public
from bybit_scalper.state import StateStore
from bybit_scalper.types import BlockReason, BookTop, Exec, Side
from test_execution import _engine, _market, _plan


def test_funding_execution_and_settlement_are_stored_once(cfg, tmp_path):
    engine, _paper, store = _engine(cfg, tmp_path)
    parsed = funding_from_execution(
        {"execType": "Funding", "execId": "exec-f", "execFee": "1.25", "execTime": engine.now_ms}
    )
    assert parsed is not None
    exec_id, cash, ts, aliases = parsed
    assert cash == D("-1.25")
    assert store.add_cashflow("execution", exec_id, "funding", cash, ts, aliases)
    settlement = cashflow_from_transaction(
        {
            "id": "tx-1",
            "type": "SETTLEMENT",
            "cashFlow": "0",
            "funding": "-1.25",
            "transactionTime": engine.now_ms,
            "tradeId": "exec-f",
        }
    )
    assert settlement is not None
    kind, external, cash2, ts2, aliases2 = settlement
    assert store.add_cashflow("transaction", external, kind, cash2, ts2, aliases2) is False
    assert cashflow_from_transaction(
        {"id": "trade-fee", "type": "TRADE", "cashFlow": "9", "transactionTime": engine.now_ms}
    ) is None
    assert engine._funding_today() == D("-1.25")


def test_transfer_is_not_trading_pnl_and_utc_rollover_keeps_new_funding(cfg, tmp_path):
    engine, _paper, store = _engine(cfg, tmp_path)
    engine.position.side = Side.LONG
    engine.position.qty = D("1")
    engine.position.avg = D("100")
    engine.position.stop = D("90")
    engine.position.target = D("130")
    engine.position.status = "open"
    engine.mark = D("110")
    assert store.add_cashflow("execution", "old-fund", "funding", D("-1"), engine.now_ms, ["old-fund"])
    exit_fee = D("110") * engine.fees.taker
    assert engine._equity_pnl(D("110")) == D("10") - exit_fee + D("-1")
    engine._ensure_day(engine.now_ms + 2 * 86_400_000, D("10000"))
    assert engine._equity_pnl(D("110")) == D("0")
    assert store.add_cashflow(
        "transaction",
        "in-1",
        "transfer_in",
        D("500"),
        engine.now_ms + 2 * 86_400_000,
        ["in-1"],
    )
    assert engine._equity_pnl(D("110")) == D("0")
    assert store.add_cashflow(
        "execution",
        "new-fund",
        "funding",
        D("-0.4"),
        engine.now_ms + 2 * 86_400_000,
        ["new-fund"],
    )
    assert engine._equity_pnl(D("110")) == D("-0.4")


def test_paper_blocks_entries_until_funding_clock_is_known(cfg, tmp_path):
    cfg.mode = "paper"
    engine, _paper, _store = _engine(cfg, tmp_path)
    bars, _plan_unused = _plan(cfg)
    now = bars[-1].ts + 60_000
    book = BookTop(bars[-1].c - D("0.1"), bars[-1].c + D("0.1"), D("1"), D("1"), now, True)
    engine.on_market(_market(bars, now, book=book, mark=bars[-1].c, next_funding_ms=0))
    assert engine.submitted == 0
    assert engine.last_attempt_bar == 0
    engine.on_market(_market(bars, now, book=book, mark=bars[-1].c, next_funding_ms=now + 1_000))
    assert engine.last_attempt_bar == 0
    engine.funding_ledger_confirmed = True
    engine.on_market(_market(bars, now, book=book, mark=bars[-1].c, next_funding_ms=now + 1_000))
    assert engine.last_attempt_bar == bars[-1].ts


def test_zero_available_margin_rejects_plan(cfg, tmp_path):
    engine, _paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.available = D("0")
    assert engine._margin_fits(plan) is False
    engine.margin_verified = False
    assert engine._budget() is None


def test_tape_before_order_open_does_not_fill(cfg, tmp_path):
    engine, _paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 1_700_000_100_000
    engine.submit(plan)
    opened = engine.order.opened_ms
    engine.on_trade_print(plan.entry, plan.qty, opened - 1, True, "Sell")
    assert not engine.position.is_open()
    engine.on_trade_print(plan.entry, plan.qty, opened + 500, True, "Sell")
    assert engine.position.is_open()


def test_second_partial_resets_full_stop(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 1_700_000_100_000
    engine.submit(plan)
    half = (plan.qty / 2).quantize(D("0.001"))
    assert half > 0
    engine.on_exec(paper.match_trade(plan.entry, half)[0])
    assert paper.http_calls.count("set_trading_stop") == 1
    engine.on_exec(paper.match_trade(plan.entry, plan.qty - half)[0])
    assert paper.http_calls.count("set_trading_stop") >= 2
    assert engine.position.stop_confirmed


def test_late_entry_fill_during_exit_does_not_close_twice(cfg, tmp_path):
    engine, paper, _store = _engine(cfg, tmp_path)
    _bars, plan = _plan(cfg)
    engine.now_ms = 1_700_000_100_000
    engine.mark = plan.entry
    engine.submit(plan)
    half = (plan.qty / 2).quantize(D("0.001"))
    engine.on_exec(paper.match_trade(plan.entry, half)[0])
    calls = []

    def delayed(*_args, **kwargs):
        calls.append(kwargs.get("link_id"))
        raise TimeoutError("close result unknown")

    paper.close_market = delayed
    engine._flatten(plan.entry)
    assert len(calls) == 1
    assert engine.pending_exit
    late = Exec("late-entry", plan.link_id, "Buy", half, plan.entry, D("0"), engine.now_ms)
    engine.on_exec(late)
    assert len(calls) == 1
    assert engine.block == BlockReason.UNKNOWN_POSITION


def test_private_queue_is_drained_before_quotes_and_not_evicted():
    import queue

    private = PrivateQueue(2)
    public = queue.Queue()
    private.put_nowait("exec-1")
    private.put_nowait("exec-2")
    private.put_nowait("exec-dropped")
    public.put("quote")
    batch, quote = take_private_then_public(private, public, 0.1)
    assert batch == ["exec-1", "exec-2"]
    assert quote == "quote"
    assert private.dropped == 1
    assert private.qsize() == 0


def test_live_database_cannot_be_the_paper_default():
    root = Path(__file__).resolve().parents[1]
    live = load_settings(root / "config" / "live.yaml")
    assert live.mode == "live"
    assert live.state_db == "data/state-live.db"
    assert live.d("allocated_capital") == D("1000")
    with pytest.raises(ValueError, match="data/state.db"):
        load_settings(root / "config" / "default.yaml", mode="live")


def test_collect_error_does_not_invent_tape_and_replay_says_so(cfg, tmp_path, monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(
        "bybit_scalper.runner._public_http",
        lambda _cfg: type("S", (), {"get_kline": staticmethod(boom), "get_public_trade_history": staticmethod(boom)})(),
    )
    with pytest.raises(SystemExit, match="network down"):
        collect_market(cfg, str(tmp_path / "empty"))
    assert not (tmp_path / "empty").exists()
    text = replay_market(cfg, str(tmp_path / "missing"))
    assert "не выдумана" in text


def test_account_reader_rejects_portfolio_and_unknown_margin(cfg):
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg

    def caller(margin_mode, trade_mode):
        def _call(method, **_kwargs):
            if method == "get_wallet_balance":
                return {"result": {"list": [{"totalEquity": "2500", "totalAvailableBalance": "800"}]}}
            if method == "get_account_info":
                return {"result": {"marginMode": margin_mode}}
            if method == "get_positions":
                return {"result": {"list": [{
                    "positionIdx": 0,
                    "tradeMode": trade_mode,
                    "leverage": "2",
                    "symbol": "BTCUSDT",
                }]}}
            raise AssertionError(method)

        return _call

    broker._call = caller("PORTFOLIO_MARGIN", 0)
    portfolio = broker.read_account()
    assert portfolio.verified is False
    assert portfolio.margin_mode == "portfolio"
    broker._call = caller("REGULAR_MARGIN", 0)
    isolated = broker.read_account()
    assert isolated.verified is True
    assert isolated.margin_mode == "cross"
    assert isolated.available == D("800")
    broker._call = caller("REGULAR_MARGIN", 3)
    assert broker.read_account().verified is False


def test_cashflow_sync_paginates_without_double_count_and_fails_closed(cfg, tmp_path):
    store = StateStore(str(tmp_path / "sync.db"))
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg
    broker.server_time_ms = lambda: 1700000101000
    state = {"exec": 0}

    def _call(method, **kwargs):
        if method == "get_executions":
            state["exec"] += 1
            if state["exec"] == 1:
                return {"result": {"list": [{
                    "execType": "Funding",
                    "execId": "exec-f",
                    "execFee": "1.25",
                    "execTime": "1700000100000",
                    "symbol": "BTCUSDT",
                }], "nextPageCursor": "c1"}}
            return {"result": {"list": [], "nextPageCursor": ""}}
        if method == "get_transaction_log":
            return {"result": {"list": [
                {
                    "id": "tx-1",
                    "type": "SETTLEMENT",
                    "cashFlow": "0",
                    "funding": "-1.25",
                    "transactionTime": "1700000100000",
                    "tradeId": "exec-f",
                    "currency": "USDT",
                    "symbol": "BTCUSDT",
                },
                {
                    "id": "tx-2",
                    "type": "TRANSFER_IN",
                    "cashFlow": "50",
                    "transactionTime": "1700000100000",
                    "currency": "USDT",
                },
                {
                    "id": "tx-3",
                    "type": "TRADE",
                    "cashFlow": "3",
                    "transactionTime": "1700000100000",
                    "currency": "USDT",
                },
            ], "nextPageCursor": ""}}
        raise AssertionError(method)

    broker._call = _call
    assert broker.sync_cashflows(store) is True
    assert store.sum_cashflow("funding", 0, 10**15) == D("-1.25")
    assert store.sum_cashflow("transfer_in", 0, 10**15) == D("50")

    def repeated(method, **_kwargs):
        return {"result": {"list": [], "nextPageCursor": "same"}}

    broker._call = repeated
    assert broker.sync_cashflows(store) is False
    store.close()


@pytest.mark.skipif(os.environ.get("BYBIT_INTEGRATION") != "1", reason="BYBIT_INTEGRATION=1 is required")
def test_testnet_public_time_is_opt_in():
    from pybit.unified_trading import HTTP

    session = HTTP(testnet=True, timeout=8, max_retries=1, force_retry=False)
    payload = session.get_server_time()
    assert payload.get("retCode") == 0
