import json
from types import SimpleNamespace

from bybit_scalper.decimals import D
from bybit_scalper.engine import Engine
from bybit_scalper.paper import PaperExchange
from bybit_scalper.runner import _stream_loop, _fetch_public_klines, PublicStream
from bybit_scalper.synthetic import build_long_fixture


def test_private_reconciliation_runs_while_market_queue_never_empties(cfg, tmp_path, monkeypatch):
    from bybit_scalper import runner, exchange
    bars = build_long_fixture(cfg)
    clock = [bars[-1].ts / 1000 + 61]
    cfg.mode, cfg.testnet, cfg.state_db = 'testnet', True, str(tmp_path / 'runtime.db')
    monkeypatch.setenv('BYBIT_API_KEY', 'not-a-real-key')
    monkeypatch.setenv('BYBIT_API_SECRET', 'not-a-real-secret')
    monkeypatch.setattr(runner.time, 'time', lambda: clock[0])
    monkeypatch.setattr(runner, '_fetch_public_klines', lambda *args: list(bars))
    monkeypatch.setattr(runner, '_public_http', lambda *args: object())
    monkeypatch.setattr(runner, '_refresh_public', lambda *args: (D('0'), 0))
    class Broker(PaperExchange):
        def __init__(self, cfg, key, secret, instrument):
            super().__init__(instrument, cfg.fallback_fees(), D('0'), D('0'))
        def load_context(self):
            return self.instrument, self.fees, D('10000'), int(clock[0] * 1000)
        def open_entry_orders(self):
            return []
        def account_equity(self):
            return D('10000')
    class BusyQueue:
        def __init__(self, **kwargs):
            self.count = 0
        def get(self, **kwargs):
            self.count += 1
            clock[0] += 1
            if self.count > 20:
                raise KeyboardInterrupt
            return json.dumps({'topic': 'orderbook.50.BTCUSDT', '_generation': 1,
                               'ts': int(clock[0] * 1000),
                               'type': 'snapshot' if self.count == 1 else 'delta',
                               'data': {'u': self.count * 7, 'b': [['100', '1']], 'a': [['101', '1']]}})
    class Stream:
        connected, generation = True, 1
        def __init__(self, *args):
            pass
        def start(self):
            pass
        def close(self):
            pass
        def request_snapshot(self):
            raise AssertionError('valid delta must not force a new snapshot')
    ws = SimpleNamespace(ws=object(), auth=True, is_connected=lambda: True, exit=lambda: None)
    monkeypatch.setattr(exchange, 'PybitBroker', Broker)
    monkeypatch.setattr(runner.queue, 'Queue', BusyQueue)
    monkeypatch.setattr(runner, 'PublicStream', Stream)
    monkeypatch.setattr(runner, '_start_private', lambda *args: ws)
    calls = []
    original = Engine.reconcile
    def tracked(self):
        calls.append(clock[0])
        return original(self)
    monkeypatch.setattr(Engine, 'reconcile', tracked)
    _stream_loop(cfg, private=True)
    assert len(calls) >= 5  # Startup, public recovery, and at least three periodic checks.


def test_history_paginates_back_to_utc_midnight(cfg, monkeypatch):
    from bybit_scalper import runner
    now = 86_400_000 + 23 * 3_600_000
    monkeypatch.setattr(runner.time, 'time', lambda: now / 1000)
    class Session:
        def __init__(self):
            self.calls = []
        def get_kline(self, **kwargs):
            self.calls.append(kwargs['end'])
            newest = kwargs['end'] // 60_000 * 60_000
            return {'result': {'list': [[ts, '1', '1', '1', '1', '1']
                                       for ts in range(newest, newest - 1000 * 60_000, -60_000)]}}
    session = Session()
    bars = _fetch_public_klines(session, cfg)
    assert len(session.calls) == 2
    assert bars[0].ts <= now // 86_400_000 * 86_400_000
    assert len({bar.ts for bar in bars}) == len(bars)


def test_public_tape_subscription_does_not_depend_on_signal_filter(cfg, monkeypatch):
    import queue
    import websocket
    sent = []
    messages = queue.Queue()
    stream = PublicStream(cfg, messages, [False])
    class Socket:
        def send(self, message):
            sent.append(json.loads(message))
        def settimeout(self, value):
            pass
        def recv(self):
            stream.stop.set()
            return ''
        def close(self):
            pass
    monkeypatch.setattr(websocket, 'create_connection', lambda *args, **kwargs: Socket())
    cfg.trade_flow_enabled = False
    stream._run()
    assert f'publicTrade.{cfg.symbol}' in sent[0]['args']
