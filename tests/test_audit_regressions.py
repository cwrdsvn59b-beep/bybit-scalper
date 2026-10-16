from decimal import Decimal
from dataclasses import replace

import pytest

from bybit_scalper.book import OrderBook
from bybit_scalper.decimals import D
from bybit_scalper.engine import Engine
from bybit_scalper.exchange import PybitBroker
from bybit_scalper.paper import PaperExchange
from bybit_scalper.state import StateStore
from bybit_scalper.types import BlockReason, Exec, OrderStatus, Position, Side
from test_execution import _engine, _plan


def test_cancel_ack_does_not_prove_cancellation(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    paper.cancel = lambda link: None  # API accepted the request; order still rests.
    engine._expire_entry(engine.now_ms + 4000)
    assert engine.order.active
    assert engine.order.status in {OrderStatus.CANCEL_PENDING, OrderStatus.UNKNOWN}


def test_flatten_cancels_remaining_entry_before_exit(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty / 2)[0])
    engine._flatten(plan.entry)
    assert paper.orders[plan.link_id].status == OrderStatus.CANCELLED
    assert not paper.match_trade(plan.entry, plan.qty)
    assert not engine.position.is_open()


def test_pending_exit_is_not_submitted_twice(cfg, tmp_path):
    engine, paper, store = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    calls = []
    def delayed(*args, **kwargs):
        calls.append(kwargs.get('link_id'))
        raise TimeoutError('accepted but fill not visible yet')
    paper.close_market = delayed
    engine._flatten(plan.entry)
    engine._flatten(plan.entry)
    assert len(calls) == 1
    restarted = Engine(cfg, store, paper, cfg.fallback_fees(), cfg.instrument())
    restarted.recover(engine.now_ms, D('10000'))
    restarted._flatten(plan.entry)
    assert len(calls) == 1


def test_daily_loss_includes_fees_already_paid(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    exit_fee = plan.entry * plan.qty * engine.fees.taker
    assert engine._equity_pnl(plan.entry) == -engine.open_fees - exit_fee


def test_reconcile_checks_side_not_only_size(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    paper.position.side = Side.SHORT
    engine.reconcile()
    assert engine.block == BlockReason.UNKNOWN_POSITION


def test_reconcile_rechecks_exchange_stop(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    paper.position.stop_confirmed = False
    paper.position.stop = D('0')
    before = len(paper.stop_calls)
    engine.reconcile()
    assert len(paper.stop_calls) > before
    assert paper.position.stop_confirmed


def test_actual_bybit_ids_need_not_be_consecutive():
    book = OrderBook()
    book.apply({'type': 'snapshot', 'ts': 10, 'data': {'u': 100, 'seq': 500, 'b': [['100', '1']], 'a': [['101', '1']]}})
    book.apply({'type': 'delta', 'ts': 11, 'data': {'u': 107, 'seq': 520, 'b': [['100', '2']], 'a': []}})
    assert book.valid
    assert book.top()[2] == D('2')


def test_private_execution_filters_other_symbols(cfg):
    broker = object.__new__(PybitBroker)
    broker.cfg, broker._execs = cfg, {}
    row = dict(execType='Trade', execId='foreign', symbol='ETHUSDT', category='linear',
               execQty='1', execPrice='100', execFee='0', side='Buy', orderLinkId='other')
    assert broker.ingest_execution_message({'data': [row]}) == []


def test_market_close_returns_request_not_one_of_many_fills(cfg):
    class Session:
        def place_order(self, **kwargs):
            return {'retCode': 0, 'result': {'orderId': 'exit-1'}}
        def get_executions(self, **kwargs):
            return {'result': {'list': []}}
    broker = object.__new__(PybitBroker)
    broker.cfg, broker.session = cfg, Session()
    broker._invalid, broker._failed = ValueError, RuntimeError
    broker._execs, broker._status = {}, {}
    result = broker.close_market(D('1'), Side.LONG, D('100'), 1, link_id='durable-exit')
    assert result.accepted
    assert result.link_id == 'durable-exit'


def test_paper_execution_ids_survive_restart_uniquely(cfg):
    first = PaperExchange(cfg.instrument(), cfg.fallback_fees(), D('1'), D('0'))
    second = PaperExchange(cfg.instrument(), cfg.fallback_fees(), D('1'), D('0'))
    for paper in [first, second]:
        paper.place_limit('entry', Side.LONG, D('1'), D('100'))
    assert first.match_trade(D('100'), D('1'))[0].exec_id != second.match_trade(D('100'), D('1'))[0].exec_id


def test_exec_marker_and_accounting_rollback_together(cfg, tmp_path, monkeypatch):
    engine, paper, store = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    execution = paper.match_trade(plan.entry, plan.qty)[0]
    original = store.save_position
    monkeypatch.setattr(store, 'save_position', lambda pos: (_ for _ in ()).throw(RuntimeError('disk failure')))
    with pytest.raises(RuntimeError, match='disk failure'):
        engine.on_exec(execution)
    assert not engine.position.is_open()
    assert store.conn.execute('SELECT count(*) FROM execs').fetchone()[0] == 0
    monkeypatch.setattr(store, 'save_position', original)
    assert engine.on_exec(execution)
    assert engine.position.qty == plan.qty
    assert store.load_position().qty == plan.qty


def test_all_exit_fills_are_applied(cfg, tmp_path):
    from bybit_scalper.paper import Resting
    from bybit_scalper.types import PlaceResult
    engine, paper, store = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    def split_close(qty, side, mark, ts, *, link_id):
        first = paper._fill_position(side, qty / 2, mark, ts, True, False, link_id)
        second = paper._fill_position(side, qty / 2, mark, ts + 1, True, False, link_id)
        paper.orders[link_id] = Resting(link_id, side, mark, qty, qty, D('0'), OrderStatus.FILLED, True, [first, second])
        return PlaceResult(True, link_id, 'accepted')
    paper.close_market = split_close
    engine._flatten(plan.entry)
    assert not engine.position.is_open()
    assert not engine.pending_exit
    assert store.trades()[0].qty == plan.qty
    assert store.conn.execute('SELECT count(*) FROM execs').fetchone()[0] == 3


def test_reverse_chronological_rest_pages_are_sorted(cfg):
    def row(index):
        return dict(symbol=cfg.symbol, execType='Trade', execId=f'ex-{index}', orderLinkId='entry',
                    execQty='0.01', execPrice='100', execFee='0', side='Buy', execTime=str(index))
    class Session:
        def get_executions(self, **kwargs):
            if kwargs.get('cursor'):
                return {'result': {'list': [row(1)], 'nextPageCursor': ''}}
            return {'result': {'list': [row(3), row(2)], 'nextPageCursor': 'next'}}
    broker = object.__new__(PybitBroker)
    broker.cfg, broker.session = cfg, Session()
    broker._execs = {}
    assert [ex.ts for ex in broker.executions('entry')] == [1, 2, 3]


def test_ws_fill_seen_by_rest_is_still_delivered_to_engine(cfg):
    broker = object.__new__(PybitBroker)
    broker.cfg, broker._execs = cfg, {}
    row = dict(symbol=cfg.symbol, execType='Trade', execId='seen-rest', orderLinkId='entry',
               execQty='1', execPrice='100', execFee='0', side='Buy')
    broker._execs['entry'] = [broker._parse_exec(row)]
    assert len(broker.ingest_execution_message({'data': [row]})) == 1


def test_paper_taker_direction_and_execution_side(cfg):
    paper = PaperExchange(cfg.instrument(), cfg.fallback_fees(), D('0'), D('0'))
    paper.place_limit('entry', Side.LONG, D('1'), D('100'))
    assert not paper.match_trade(D('100'), D('1'), taker_side='Buy')
    assert paper.match_trade(D('100'), D('1'), taker_side='Sell')
    assert paper.close_market(D('1'), Side.LONG, D('100'), 1).side == 'Sell'


def test_expired_paper_order_cannot_fill_on_late_tape(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_trade_print(plan.entry, plan.qty, engine.now_ms + 4000, True, 'Sell')
    assert not engine.position.is_open()
    assert engine.order.status == OrderStatus.CANCELLED


def test_db_cannot_be_reused_between_paper_and_live(tmp_path):
    store = StateStore(str(tmp_path / 'bound.db'))
    store.bind_identity('paper', 'BTCUSDT', 'paper')
    with pytest.raises(RuntimeError, match='другому режиму'):
        store.bind_identity('live', 'BTCUSDT', 'account')
    store.close()


def test_daily_latch_cannot_be_cleared_by_reconnect(cfg, tmp_path):
    engine, paper, store = _engine(cfg, tmp_path)
    store.set_kv('daily_stop_day', engine.day)
    engine.block = BlockReason.DISCONNECT
    engine.on_reconnect()
    assert engine.block == BlockReason.DAILY


def test_paper_book_mark_is_current_not_last_closed_candle(cfg, tmp_path, monkeypatch):
    from bybit_scalper.runner import _on_market
    from types import SimpleNamespace
    engine, paper, _ = _engine(cfg, tmp_path)
    bars, _ = _plan(cfg)
    book = OrderBook()
    now = bars[-1].ts + 60_001
    monkeypatch.setattr('bybit_scalper.runner.time.time', lambda: now / 1000)
    book.apply({'type': 'snapshot', 'ts': now, 'data': {'u': 1, 'b': [['90000', '1']], 'a': [['90001', '1']]}})
    _on_market(engine, bars, book, D('10000'), D('90000.5'), [False], D('0'), 0, SimpleNamespace(connected=True))
    assert engine.mark == D('90000.5')


def test_five_minute_bar_requires_five_distinct_minutes(cfg):
    from bybit_scalper.indicators import closed_5m
    from bybit_scalper.types import Candle
    duplicate = Candle(0, D('1'), D('1'), D('1'), D('1'), D('1'))
    assert closed_5m([duplicate] * 5) == []


def test_reconciliation_replays_entry_and_stop_before_protecting(cfg, tmp_path):
    engine, paper, store = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    entry = paper.match_trade(plan.entry, plan.qty)[0]
    exit_ = paper.close_market(plan.qty, plan.side, plan.entry, engine.now_ms + 1, link_id='stop')
    paper.executions_since = lambda start: [entry, exit_]
    engine.reconcile()
    assert not engine.position.is_open()
    assert not paper.stop_calls
    assert len(store.trades()) == 1


def test_private_recover_never_trusts_stored_stop_before_reconciliation(cfg, tmp_path):
    engine, paper, store = _engine(cfg, tmp_path)
    store.save_position(Position(Side.LONG, D('1'), D('100'), D('90'), D('120'), 10, False, 'open'))
    cfg.mode = 'testnet'
    restarted = Engine(cfg, store, paper, cfg.fallback_fees(), cfg.instrument())
    restarted.recover(engine.now_ms, D('10000'))
    assert not paper.stop_calls and not paper.market_closes
    assert restarted.block == BlockReason.DISCONNECT


def test_second_process_cannot_open_same_state_database(tmp_path):
    path = str(tmp_path / 'single-process.db')
    first = StateStore(path)
    with pytest.raises(RuntimeError, match='другой процесс'):
        StateStore(path)
    first.close()
    second = StateStore(path)
    second.close()


def test_exchange_stop_also_cancels_unfilled_entry_remainder(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty / 2)[0])
    exchange_stop = paper.close_market(engine.position.qty, plan.side, plan.stop, engine.now_ms)
    engine.on_exec(exchange_stop)
    assert not engine.position.is_open()
    assert not engine.order.active
    assert not paper.match_trade(plan.entry, plan.qty)


def test_pending_cancel_does_not_poll_on_every_market_frame(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    paper.cancel = lambda link: None
    engine._expire_entry(engine.now_ms + 4000)
    paper.executions = lambda link: (_ for _ in ()).throw(AssertionError('HTTP storm'))
    engine._expire_entry(engine.now_ms + 4001)


def test_restart_without_market_price_does_not_liquidate_at_zero(cfg, tmp_path):
    engine, paper, _ = _engine(cfg, tmp_path)
    _, plan = _plan(cfg)
    engine.submit(plan)
    engine.on_exec(paper.match_trade(plan.entry, plan.qty)[0])
    engine.mark = D('0')
    engine.reconcile()
    assert not paper.market_closes
    assert engine.position.is_open()
    assert engine.block == BlockReason.STALE
