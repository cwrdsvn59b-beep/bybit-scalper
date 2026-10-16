from decimal import Decimal

import pytest

from bybit_scalper.decimals import D
from bybit_scalper.exchange import PybitBroker
from bybit_scalper.ledger import cashflow_from_transaction
from bybit_scalper.state import StateStore
from test_execution import _engine, _plan


def test_v5_settlement_uses_funding_not_cashflow():
    parsed = cashflow_from_transaction(dict(
        id="settlement",
        type="SETTLEMENT",
        funding="-1.25",
        cashFlow="0",
        transactionTime="1700000100000",
        tradeId="trade",
    ))
    assert parsed[2] == D("-1.25")


def test_settlement_missing_funding_is_not_silently_zero():
    with pytest.raises(ValueError):
        cashflow_from_transaction(dict(id="bad", type="SETTLEMENT", cashFlow="0", transactionTime="1"))


def test_past_funding_clock_cannot_authorize_new_entry(cfg, tmp_path):
    engine, _, _ = _engine(cfg, tmp_path)
    cfg.mode = "testnet"
    engine.next_funding_ms = engine.now_ms - 1
    engine.funding_ledger_confirmed = True
    assert not engine._funding_allows(engine.now_ms)


def test_margin_reserve_cannot_exceed_allocated_budget(cfg, tmp_path):
    engine, _, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    cfg.allocated_capital = "100"
    engine.available = D("100000")
    engine.account_leverage = D("1")
    assert not engine._margin_fits(plan)


def test_unknown_account_margin_mode_is_unverified(cfg):
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg

    def call(method, **kwargs):
        if method == "get_account_info":
            return {"result": {"marginMode": "UNKNOWN"}}
        if method == "get_wallet_balance":
            return {"result": {"list": [{"totalEquity": "1000", "totalAvailableBalance": "500"}]}}
        if method == "get_positions":
            return {"result": {"list": [{"positionIdx": 0, "tradeMode": 0, "leverage": "2", "symbol": "BTCUSDT"}]}}
        raise AssertionError(method)

    broker._call = call
    assert not broker.read_account().verified


def test_funding_sync_uses_one_canonical_source_and_repairs_old_rows(cfg, tmp_path):
    store = StateStore(str(tmp_path / "ledger.db"))
    now = 1700000100000
    store.add_cashflow("execution", "old-id", "funding", D("-1"), now, ["old-id"])
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg
    broker.server_time_ms = lambda: now + 1000
    calls = []

    def call(method, **kwargs):
        calls.append((method, kwargs))
        if method == "get_executions":
            return {"result": {"list": [{
                "execType": "Funding",
                "execId": "different-id",
                "execFee": "1",
                "execTime": str(now),
                "symbol": cfg.symbol,
            }]}}
        if method == "get_transaction_log":
            return {"result": {"list": [{
                "id": "canonical-id",
                "tradeId": "unrelated-id",
                "type": "SETTLEMENT",
                "symbol": cfg.symbol,
                "currency": "USDT",
                "funding": "-1",
                "cashFlow": "0",
                "transactionTime": str(now),
            }]}}
        raise AssertionError(method)

    broker._call = call
    assert broker.sync_cashflows(store)
    assert store.sum_cashflow("funding", 0, 10**15) == D("-1")
    assert broker.sync_cashflows(store)
    assert store.sum_cashflow("funding", 0, 10**15) == D("-1")
    assert all(
        "startTime" in params and "endTime" in params
        for method, params in calls
        if method == "get_transaction_log"
    )
    store.close()


def test_failed_later_ledger_page_keeps_previous_snapshot(cfg, tmp_path):
    store = StateStore(str(tmp_path / "failed-ledger.db"))
    store.add_cashflow("transaction", "existing", "funding", D("-2"), 1700000100000, ["existing"])
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg
    broker.server_time_ms = lambda: 1700000101000

    def call(method, **kwargs):
        if method == "get_executions":
            return {"result": {"list": []}}
        if kwargs.get("cursor"):
            raise RuntimeError("second page unavailable")
        return {"result": {
            "list": [{
                "id": "new",
                "type": "SETTLEMENT",
                "symbol": cfg.symbol,
                "funding": "-3",
                "cashFlow": "0",
                "currency": "USDT",
                "transactionTime": "1700000100000",
            }],
            "nextPageCursor": "second",
        }}

    broker._call = call
    assert not broker.sync_cashflows(store)
    assert store.sum_cashflow("funding", 0, 10**15) == D("-2")
    store.close()


def test_account_lock_rejects_second_database_for_same_uid(tmp_path, monkeypatch):
    from bybit_scalper.state import account_lease

    monkeypatch.setenv("BYBIT_LOCK_DIR", str(tmp_path / "locks"))
    with account_lease("testnet", "12345"):
        with pytest.raises(RuntimeError, match="другой процесс"):
            with account_lease("testnet", "12345"):
                raise AssertionError("second account lease was allowed")
        with account_lease("mainnet", "12345"):
            pass
        with account_lease("testnet", "67890"):
            pass
    with account_lease("testnet", "12345"):
        pass


def test_broker_identity_is_authenticated_uid_not_key_id(cfg):
    broker = object.__new__(PybitBroker)
    broker.cfg = cfg
    broker._call = lambda *args, **kwargs: {"result": {"userID": 12345, "id": "different-key-id"}}
    assert broker.account_identity() == "12345"
    broker._call = lambda *args, **kwargs: {"result": {"id": "key-without-uid"}}
    with pytest.raises(RuntimeError, match="UID"):
        broker.account_identity()


def test_report_does_not_return_offline_cache_and_includes_funding(cfg, tmp_path, monkeypatch):
    from bybit_scalper.runner import format_saved_report

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "last_report.txt").write_text("unrelated synthetic result")
    cfg.mode = "paper"
    cfg.state_db = str(tmp_path / "state.db")
    writer = StateStore(cfg.state_db)
    writer.add_cashflow("transaction", "funding", "funding", D("-2"), 1, ["funding"])
    text = format_saved_report(cfg)
    assert "unrelated synthetic" not in text
    assert "Учтённый funding: -2" in text
    assert "Сумма торгового PnL и funding: -2" in text
    writer.close()


def test_replay_does_not_invent_book_from_candle_close(cfg, tmp_path):
    import json

    from bybit_scalper.runner import replay_market

    (tmp_path / "BTCUSDT_klines.json").write_text(json.dumps([
        {"ts": 60000, "o": "100", "h": "101", "l": "99", "c": "100", "v": "1"},
    ]))
    (tmp_path / "BTCUSDT_trades.json").write_text(json.dumps([
        {"T": 61000, "p": "100", "v": "1", "S": "Sell"},
    ]))
    text = replay_market(cfg, str(tmp_path))
    assert "Чистый PnL" not in text
    assert "не рассчитывается" in text
