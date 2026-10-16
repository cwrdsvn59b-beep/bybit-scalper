from __future__ import annotations

import json
import logging
import queue
import threading
import time
import hashlib
from collections import deque
from decimal import Decimal
from pathlib import Path

from bybit_scalper.book import OrderBook
from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.engine import Engine
from bybit_scalper.paper import PaperExchange
from bybit_scalper.report import format_report, summarize
from bybit_scalper.state import StateStore, account_lease
from bybit_scalper.synthetic import build_long_fixture, candles_from_csv, candles_to_csv
from bybit_scalper.types import BlockReason, BookTop, Candle, MarketState, OrderStatus

log = logging.getLogger("bybit_scalper")


def book_from_close(close: Decimal, cfg: Settings) -> BookTop:
    tick = cfg.d("tick_size")
    bid = close - tick
    ask = close + tick
    if bid <= 0:
        bid = tick
        ask = bid + tick
    depth = cfg.d("min_depth_qty")
    return BookTop(bid=bid, ask=ask, bid_qty=depth, ask_qty=depth, ts=0, valid=True)


def parse_kline(message: dict) -> tuple[Candle, bool] | None:
    data = message.get("data") or []
    if isinstance(data, dict):
        data = [data]
    if not data:
        return None
    row = data[-1]
    candle = Candle(
        ts=int(row["start"]),
        o=D(row["open"]),
        h=D(row["high"]),
        l=D(row["low"]),
        c=D(row["close"]),
        v=D(row["volume"]),
    )
    return candle, bool(row.get("confirm"))


def parse_trades(message: dict) -> list[tuple[Decimal, Decimal, int, str]]:
    prints = []
    for row in message.get("data") or []:
        prints.append((D(row["p"]), D(row["v"]), int(row["T"]), str(row["S"])))
    return prints


class PrivateQueue:
    """Private executions are not evicted to make room for quotes.

    A full private buffer drops the new event and counts it. The loop then blocks entries.
    """

    def __init__(self, maxsize: int) -> None:
        self.maxsize = maxsize
        self.dropped = 0
        self._items: deque[str] = deque()

    def put_nowait(self, item: str) -> None:
        if len(self._items) >= self.maxsize:
            self.dropped += 1
            return
        self._items.append(item)

    def get_nowait(self) -> str:
        if not self._items:
            raise queue.Empty
        return self._items.popleft()

    def qsize(self) -> int:
        return len(self._items)


def take_private_then_public(private_q: PrivateQueue, public_q: queue.Queue, timeout: float):
    """Drain every queued private event, then take at most one public message."""
    private_batch: list[str] = []
    while True:
        try:
            private_batch.append(private_q.get_nowait())
        except queue.Empty:
            break
    try:
        public_item = public_q.get(timeout=timeout)
    except queue.Empty:
        public_item = None
    return private_batch, public_item


def upsert_candle(bars: list[Candle], candle: Candle) -> None:
    if bars and bars[-1].ts == candle.ts:
        bars[-1] = candle
        return
    if bars and candle.ts < bars[-1].ts:
        return
    bars.append(candle)


class PublicStream:
    """Сырой публичный поток V5.

    pybit 5.17.0 перед callback переписывает orderbook delta в snapshot
    (`_process_normal_message`), поэтому разрыв update id в его callback не виден.
    Здесь в очередь кладётся исходное сообщение. HTTP и приватный поток идут через pybit.
    """

    def __init__(self, cfg: Settings, messages: queue.Queue, overflow: list[bool]) -> None:
        self.cfg = cfg
        self.messages = messages
        self.overflow = overflow
        self.stop = threading.Event()
        self.connected = False
        self.needs_reconcile = False
        self.last_error = ""
        self._thread: threading.Thread | None = None
        self.generation = 0
        self.resubscribe = threading.Event()
        self._ws = None

    def request_snapshot(self) -> None:
        self.resubscribe.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def url(self) -> str:
        if self.cfg.demo and self.cfg.testnet:
            host = "stream-demo-testnet.bybit.com"
        elif self.cfg.demo:
            host = "stream-demo.bybit.com"
        elif self.cfg.testnet:
            host = "stream-testnet.bybit.com"
        else:
            host = "stream.bybit.com"
        return f"wss://{host}/v5/public/linear"

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="bybit-public", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.stop.set()
        self.request_snapshot()

    def _run(self) -> None:
        import websocket

        while not self.stop.is_set():
            self.connected = False
            ws = None
            try:
                ws = websocket.create_connection(self.url(), timeout=10)
                self._ws = ws
                self.resubscribe.clear()
                self.generation += 1
                generation = self.generation
                args = [f"kline.1.{self.cfg.symbol}", f"orderbook.50.{self.cfg.symbol}",
                        f"publicTrade.{self.cfg.symbol}"]
                ws.send(json.dumps({"op": "subscribe", "args": args}))
                self.connected = True
                self.needs_reconcile = True
                ws.settimeout(5)
                last_ping = time.time()
                last_received = time.monotonic()
                while not self.stop.is_set() and not self.resubscribe.is_set():
                    if time.time() - last_ping >= 20:
                        ws.send(json.dumps({"op": "ping"}))
                        last_ping = time.time()
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        if time.monotonic() - last_received > 30:
                            raise TimeoutError("public stream heartbeat expired")
                        continue
                    if not raw:
                        break
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    last_received = time.monotonic()
                    parsed = json.loads(raw)
                    if parsed.get("success") is False:
                        raise RuntimeError("public subscription rejected")
                    parsed["_generation"] = generation
                    try:
                        self.messages.put_nowait(json.dumps(parsed))
                    except queue.Full:
                        self.overflow[0] = True
                        self.resubscribe.set()
            except Exception as exc:
                self.last_error = str(exc)
                self.connected = False
                time.sleep(1)
            finally:
                self.connected = False
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass


def run_offline(cfg: Settings) -> str:
    path = Path("data/btcusdt_1m_sample.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        bars = candles_from_csv(path.read_text(encoding="utf-8"))
    else:
        bars = build_long_fixture(cfg)
        path.write_text(candles_to_csv(bars), encoding="utf-8")
    db_path = Path("data/offline.db")
    if db_path.exists():
        db_path.unlink()
    for extra in (Path("data/offline.db-wal"), Path("data/offline.db-shm")):
        if extra.exists():
            extra.unlink()
    store = StateStore(str(db_path))
    paper = PaperExchange(cfg.instrument(), cfg.fallback_fees(), cfg.d("slippage_bps"), Decimal("1"))
    engine = Engine(cfg, store, paper, cfg.fallback_fees(), cfg.instrument())
    equity = cfg.d("offline_equity")
    engine.recover(bars[0].ts, equity)
    window: list[Candle] = []
    for bar in bars:
        window.append(bar)
        market = MarketState(
            bars=list(window),
            book=book_from_close(bar.c, cfg),
            now_ms=bar.ts + 60_000,
            equity=equity,
            mark=bar.c,
            stale=False,
            queue_overflow=False,
            funding_rate=Decimal("0"),
            next_funding_ms=0,
            connected=True,
        )
        engine.on_market(market)
    cut = min(len(bars) - 1, max(1, int(len(bars) * float(cfg.d("train_ratio")))))
    border = bars[cut].ts
    trades = store.trades()
    text = "\n\n".join(
        [
            format_report(
                _split_report(engine, trades, "настройка, первые 70% хронологии", 0, border, preliminary=True)
            ),
            format_report(
                _split_report(
                    engine,
                    trades,
                    "независимая проверка, последние 30%",
                    border,
                    bars[-1].ts + 60_000,
                    preliminary=True,
                )
            ),
            _notes(cfg, len(bars)),
            run_execution_probe(cfg),
        ]
    )
    Path("data/last_report.txt").write_text(text + "\n", encoding="utf-8")
    store.close()
    return text


def _split_report(engine: Engine, trades, label: str, start: int, end: int, preliminary: bool):
    chosen = [trade for trade in trades if start <= trade.ts < end]
    submitted = sum(1 for ts, kind in engine.audit if kind == "submit" and start <= ts < end)
    filled = sum(1 for ts, kind in engine.audit if kind == "fill" and start <= ts < end)
    signals = sum(1 for ts, kind in engine.audit if kind == "signal" and start <= ts < end)
    notes = [f"Сигналов на отрезке: {signals}. Параметры между отрезками не менялись."]
    return summarize(chosen, submitted, filled, label, preliminary, notes)


def _notes(cfg: Settings, count: int) -> str:
    return "\n".join(
        [
            "Ограничения симуляции:",
            f"- Свечей в ряду: {count}. Это синтетический пример, не рыночная история.",
            "- Лимит PostOnly живёт 3 секунды. Минутная свеча не содержит ленту внутри этих 3 секунд.",
            "- Касание high/low не считается исполнением, поэтому доля исполнений на ряде свечей равна нулю.",
            "- Это не доказательство прибыли и не доказательство, что стратегия не торгует на ленте.",
            "- Комиссии offline: maker 0.020%, taker 0.055%, пока API не подменит их.",
            "- В риск входа заложена taker-комиссия, даже если фактический отдых в стакане был бы maker.",
            f"- Поток сделок в конфиге: {'включён' if cfg.trade_flow_enabled else 'выключен'}.",
            f"- Выделенный капитал: {cfg.d('allocated_capital')}. Это не весь UNIFIED equity.",
            "- Offline не имеет биржевых часов funding, поэтому next_funding=0 вход не блокирует.",
            "- Paper, testnet и live без подтверждённого времени funding новые входы не ставят.",
        ]
    )


def run_execution_probe(cfg: Settings) -> str:
    """Один синтетический проход ленты. Это проверка контура, не отчёт доходности."""
    from bybit_scalper.signals import evaluate

    bars = build_long_fixture(cfg)
    decision = evaluate(bars, cfg)
    if not decision.accepted or decision.signal is None:
        return f"Проверка контура не собрала сигнал: {decision.reason}"
    db = Path("data/probe.db")
    if db.exists():
        db.unlink()
    store = StateStore(str(db))
    paper = PaperExchange(cfg.instrument(), cfg.fallback_fees(), cfg.d("slippage_bps"), Decimal("0.5"))
    engine = Engine(cfg, store, paper, cfg.fallback_fees(), cfg.instrument())
    equity = cfg.d("offline_equity")
    now = bars[-1].ts + 60_000
    engine.recover(bars[0].ts, equity)
    market = MarketState(
        bars=bars,
        book=book_from_close(bars[-1].c, cfg),
        now_ms=now,
        equity=equity,
        mark=bars[-1].c,
        connected=True,
    )
    engine.on_market(market)
    if engine.order is None or engine.order.status.value != "submitted":
        store.close()
        return "Проверка контура: сигнал был, заявка не встала. Это не сделка и не доходность."
    entry = engine.order.price
    engine.on_trade_print(entry, Decimal("2"), now + 1000, simulate_fill=True)
    protected = engine.position.is_open() and engine.position.stop_confirmed
    if protected and engine.position.target is not None:
        engine.on_trade_print(engine.position.target, engine.position.qty, now + 2000, simulate_fill=True)
    trades = store.trades()
    net = trades[-1].pnl if trades else Decimal("0")
    lines = [
        "=== Проверка контура исполнения (не доходность) ===",
        "Сигнал фикстуры принят тем же модулем, что и offline.",
        f"PostOnly принят биржей-симулятором и ещё не был сделкой: да.",
        f"Лента по цене лимита после очереди дала позицию: {engine.filled_entries > 0 or bool(trades)}.",
        f"Стоп подтверждён до выхода: {protected}.",
        f"Сделок контура: {len(trades)}. Чистый PnL этой проверки: {net}.",
        "Цифры контура получены на нарисованных свечах и одной ленте. На рынок их переносить нельзя.",
    ]
    store.close()
    return "\n".join(lines)


def run_paper(cfg: Settings) -> None:
    _stream_loop(cfg, private=False)


def run_testnet(cfg: Settings) -> None:
    cfg.testnet = True
    cfg.demo = False
    _stream_loop(cfg, private=True)


def run_live(cfg: Settings) -> None:
    cfg.testnet = False
    cfg.demo = False
    _stream_loop(cfg, private=True)


def _stream_loop(cfg: Settings, private: bool) -> None:
    if not private:
        return _stream_loop_impl(cfg, private=False)
    import os

    from bybit_scalper.exchange import PybitBroker

    key = os.environ.get("BYBIT_API_KEY", "")
    secret = os.environ.get("BYBIT_API_SECRET", "")
    if not key or not secret:
        raise SystemExit("Нет BYBIT_API_KEY или BYBIT_API_SECRET.")
    broker = PybitBroker(cfg, key, secret, cfg.instrument())
    uid = broker.account_identity()
    environment = "testnet" if cfg.testnet else ("demo" if cfg.demo else "mainnet")
    with account_lease(environment, uid):
        return _stream_loop_impl(cfg, private=True, prepared_broker=broker)


def _stream_loop_impl(cfg: Settings, private: bool, prepared_broker=None) -> None:
    if not cfg.symbol_allowed():
        raise SystemExit(
            "Символ не разрешён. ETHUSDT включается только после отдельной проверки, eth_enabled: true."
        )
    import os

    from bybit_scalper.exchange import PybitBroker

    key = os.environ.get("BYBIT_API_KEY", "")
    secret = os.environ.get("BYBIT_API_SECRET", "")
    if private and (not key or not secret):
        raise SystemExit("Нет BYBIT_API_KEY или BYBIT_API_SECRET.")
    market_http = _public_http(cfg)
    try:
        history = _fetch_public_klines(market_http, cfg)
    except Exception as exc:
        raise SystemExit(
            "Не удалось получить публичные свечи Bybit с этой машины. "
            f"Paper/testnet не запущены и не подменены заглушкой. Причина: {exc}"
        ) from exc
    if history and history[-1].ts + 60_000 > int(time.time() * 1000):
        history.pop()
    if len(history) < cfg.min_1m_bars:
        raise SystemExit(f"Мало минутных свечей для старта: {len(history)}")
    if not candles_contiguous(history):
        raise SystemExit("История свечей содержит пропуски или дубликаты")
    instrument = cfg.instrument()
    fees = cfg.fallback_fees()
    equity = cfg.d("offline_equity")
    broker: PaperExchange | PybitBroker
    if private:
        broker = prepared_broker or PybitBroker(cfg, key, secret, instrument)
        instrument, fees, equity, server_ms = broker.load_context()
        log.info("комиссии API maker=%s taker=%s источник=%s", fees.maker, fees.taker, fees.source)
        log.info("капитал UNIFIED %s, сервер %s", equity, server_ms)
    else:
        meta_resp = market_http.get_instruments_info(category=cfg.category, symbol=cfg.symbol)
        meta = meta_resp["result"]["list"][0]
        lot = meta["lotSizeFilter"]
        from bybit_scalper.types import Instrument
        instrument = Instrument(cfg.symbol, D(meta["priceFilter"]["tickSize"]), D(lot["qtyStep"]),
                                D(lot["minOrderQty"]), D(lot.get("minNotionalValue") or "0"),
                                min(D(lot["maxOrderQty"]), D(lot["maxMktOrderQty"])))
        broker = PaperExchange(instrument, fees, cfg.d("slippage_bps"), Decimal("1"))
        log.info("paper: ключи не используются, комиссии из конфигурации %s", fees.source)
        try:
            server_ms = int(market_http.get_server_time()["result"]["timeNano"]) // 1_000_000
        except Exception:
            server_ms = int(time.time() * 1000)
    store = StateStore(cfg.state_db)
    engine = Engine(cfg, store, broker, fees, instrument)
    fingerprint = hashlib.sha256(key.encode()).hexdigest()[:16] if private else "paper"
    store.bind_identity(cfg.mode, cfg.symbol, fingerprint)
    if not private:
        broker.position = store.load_position()
    engine.recover(int(time.time() * 1000), equity)
    if private and hasattr(broker, "read_account"):
        try:
            engine.apply_account(broker.read_account())
        except Exception as exc:
            engine.margin_verified = False
            log.warning("маржа не подтверждена, новые входы закрыты: %s", exc)
    if private and hasattr(broker, "sync_cashflows"):
        engine.funding_ledger_confirmed = bool(broker.sync_cashflows(store))
    if not private:
        trades = store.trades()
        broker.realized = sum((t.pnl + t.fees for t in trades), D('0')) + engine.round_pnl + engine.round_fees
        broker.fees_paid = sum((t.fees for t in trades), D('0')) + engine.round_fees + engine.open_fees
        if engine.order and engine.order.active:
            # A simulator has no remote order after restart. Resume its known remaining
            # intent so expiry can cancel it, rather than silently inventing a terminal status.
            order = engine.order
            broker.place_limit(order.link_id, order.side, order.qty, order.price)
            broker.orders[order.link_id].filled = order.filled
    if private:
        allowed_link = engine.order.link_id if engine.order else None
        if any(row.get("orderLinkId") != allowed_link for row in broker.open_entry_orders()):
            store.close()
            raise SystemExit("Обнаружены неизвестные входные заявки. Нужна ручная сверка субсчёта.")
        engine.on_reconnect()
    engine.note_clock(int(time.time() * 1000), server_ms)
    messages: queue.Queue = queue.Queue(maxsize=cfg.queue_max)
    private_messages = PrivateQueue(cfg.queue_max)
    overflow = [False]
    stream = PublicStream(cfg, messages, overflow)
    stream.start()
    private_ws = None
    if private:
        try:
            private_ws = _start_private(cfg, key, secret, private_messages, overflow)
        except BaseException:
            stream.close()
            store.close()
            raise
    book = OrderBook()
    bars = history
    last_reconcile = time.time()
    last_aux = 0.0
    public_generation = 0
    private_generation = id(private_ws.ws) if private_ws else None
    recovering_stream = True
    funding_rate = Decimal("0")
    next_funding = 0
    log.info("цикл запущен, режим %s, символ %s. Остановка: Ctrl+C", cfg.mode, cfg.symbol)
    try:
        while True:
            private_ok = not private or (private_ws.is_connected() and private_ws.auth)
            generation = id(private_ws.ws) if private_ws else None
            if (overflow[0] or stream.generation != public_generation or generation != private_generation
                    or not stream.connected or not private_ok):
                if not recovering_stream or stream.generation != public_generation or overflow[0]:
                    book.reset()
                    engine.on_disconnect()
                    recovering_stream = True
                if overflow[0]:
                    stream.request_snapshot()
                    overflow[0] = False
                public_generation = stream.generation
                private_generation = generation
            if recovering_stream and stream.connected and private_ok and book.valid:
                # Backfill closed candles before enabling entries after any stream interruption.
                refreshed = _fetch_public_klines(market_http, cfg)
                now_ms = int(time.time() * 1000)
                bars = [bar for bar in refreshed if bar.ts + 60_000 <= now_ms]
                if not candles_contiguous(bars):
                    raise RuntimeError("Пропуски в свечах после восстановления")
                engine.on_reconnect()
                recovering_stream = False
            now_wall = time.time()
            if private and now_wall - last_reconcile >= 5:
                engine.reconcile()
                last_reconcile = time.time()
            if now_wall - last_aux > 30:
                last_aux = now_wall
                funding_rate, next_funding = _refresh_public(market_http, cfg, engine, funding_rate, next_funding)
                if private and hasattr(broker, "account_equity"):
                    try:
                        equity = broker.account_equity()
                    except Exception:
                        engine.margin_verified = False
                if private and hasattr(broker, "read_account"):
                    try:
                        engine.apply_account(broker.read_account())
                    except Exception:
                        engine.margin_verified = False
                if private and hasattr(broker, "sync_cashflows"):
                    engine.funding_ledger_confirmed = bool(broker.sync_cashflows(store))
            batch, raw = take_private_then_public(private_messages, messages, 0.5)
            for item in batch:
                try:
                    private_message = json.loads(item)
                except json.JSONDecodeError:
                    continue
                _dispatch_private(private_message, engine, broker if private else None, cfg)
            if private_messages.dropped:
                engine._block(BlockReason.UNKNOWN_POSITION, "private queue overflow")
            if raw is None:
                _mark_link(engine, stream, book, bars, equity, overflow, funding_rate, next_funding)
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            topic = str(message.get("topic") or "")
            if "_generation" in message and message["_generation"] != public_generation:
                continue
            if message.get("op") in {"pong", "ping"} or message.get("success") is True:
                continue
            if topic.startswith("kline"):
                parsed = parse_kline(message)
                if parsed is None:
                    continue
                candle, confirmed = parsed
                if confirmed:
                    if bars and candle.ts > bars[-1].ts + 60_000:
                        engine.on_disconnect()
                        recovering_stream = True
                        book.reset()
                        stream.request_snapshot()
                        continue
                    upsert_candle(bars, candle)
                    _on_market(engine, bars, book, equity, candle.c, overflow, funding_rate, next_funding, stream)
                else:
                    engine.mark = candle.c
            elif topic.startswith("orderbook"):
                book.apply(message)
                if book.reset_needed:
                    engine.on_disconnect()
                    recovering_stream = True
                    stream.request_snapshot()
                if bars:
                    top = book.top()
                    mark = (top[0] + top[1]) / D(2) if top else engine.mark
                    _on_market(engine, bars, book, equity, mark, overflow, funding_rate, next_funding, stream)
            elif topic.startswith("publicTrade"):
                latest_price = None
                for price, qty, ts, taker_side in parse_trades(message):
                    if int(time.time() * 1000) - ts > cfg.stale_book_ms:
                        continue
                    engine.on_trade_print(price, qty, ts, simulate_fill=not private, taker_side=taker_side)
                    latest_price = price
                if latest_price is not None:
                    _on_market(engine, bars, book, equity, latest_price, overflow, funding_rate, next_funding, stream)
            elif topic == "execution" and private:
                for ex in broker.ingest_execution_message(message):  # type: ignore[union-attr]
                    engine.on_exec(ex)
            elif topic == "order" and private:
                for row in message.get("data") or []:
                    from bybit_scalper.exchange import map_order_status

                    if row.get("symbol") != cfg.symbol:
                        continue
                    link = str(row.get("orderLinkId") or "")
                    if link:
                        engine.note_remote_status(link, map_order_status(row.get("orderStatus")))
            elif topic == "position" and private:
                pass  # REST reconciliation has its own periodic schedule, even on a busy stream.
    except KeyboardInterrupt:
        log.info("остановка по Ctrl+C")
    finally:
        try:
            _shutdown(engine)
        except Exception:
            log.exception("Сверьте состояние позиции и заявок на бирже после ошибки остановки")
        stream.close()
        if private_ws is not None:
            try:
                private_ws.exit()
            except Exception:
                pass
        store.close()


def _on_market(engine, bars, book: OrderBook, equity, mark, overflow, funding_rate, next_funding, stream: PublicStream) -> None:
    now = int(time.time() * 1000)
    top = book.top()
    stale_book = (not book.valid) or book.ts == 0 or now - book.ts > engine.cfg.stale_book_ms
    last_close = bars[-1].ts + 60_000
    stale_candle = now - last_close > engine.cfg.stale_candle_ms
    if top is None:
        view = BookTop(Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), book.ts, False)
    else:
        bid, ask, _bid_qty, _ask_qty = top
        bid_depth, ask_depth = book.depth_qty(engine.cfg.d("depth_bps"))
        view = BookTop(bid, ask, bid_depth, ask_depth, book.ts, book.valid and not book.reset_needed)
    if isinstance(engine.exchange, PaperExchange):
        paper = engine.exchange
        equity = (engine.cfg.d("offline_equity") + paper.realized - paper.fees_paid
                  + paper.unrealized(mark))
    market = MarketState(
        bars=list(bars),
        book=view,
        now_ms=now,
        equity=equity,
        mark=mark,
        stale=stale_book or stale_candle or not stream.connected or not candles_contiguous(bars),
        queue_overflow=overflow[0],
        funding_rate=funding_rate,
        next_funding_ms=next_funding,
        connected=stream.connected and engine.connected,
    )
    engine.on_market(market)
    overflow[0] = False


def _mark_link(engine, stream, book, bars, equity, overflow, funding_rate, next_funding) -> None:
    if not bars:
        return
    if not stream.connected:
        engine.on_disconnect()
    _on_market(engine, bars, book, equity, engine.mark or bars[-1].c, overflow, funding_rate, next_funding, stream)


def _shutdown(engine: Engine) -> None:
    if engine.order is not None and engine.order.active and engine.order.status != OrderStatus.UNKNOWN:
        try:
            engine._expire_entry(engine.now_ms + engine.cfg.entry_timeout_sec * 1000 + 1)
        except Exception:
            log.warning("не удалось снять рабочую заявку при остановке")
    if engine.position.is_open() and not engine.position.stop_confirmed:
        engine._protect()
    if engine.position.is_open():
        log.info("позиция остаётся. Стоп снимается только после подтверждения закрытия.")


def _refresh_public(session, cfg: Settings, engine: Engine, funding_rate: Decimal, next_funding: int):
    try:
        engine.note_request(int(time.time() * 1000))
        server = session.get_server_time()
        nano = (server.get("result") or {}).get("timeNano")
        server_ms = int(nano) // 1_000_000 if nano else int(time.time() * 1000)
        engine.note_clock(int(time.time() * 1000), server_ms)
        tick = session.get_tickers(category=cfg.category, symbol=cfg.symbol)
        row = ((tick.get("result") or {}).get("list") or [{}])[0]
        return D(row.get("fundingRate") or "0"), int(row.get("nextFundingTime") or 0)
    except Exception:
        log.info("время или funding не обновились; при рассинхроне часов новые входы блокируются")
        return funding_rate, next_funding


def _public_http(cfg: Settings):
    import logging as logging_mod

    from pybit.unified_trading import HTTP

    return HTTP(testnet=cfg.testnet, demo=cfg.demo, log_requests=False, logging_level=logging_mod.WARNING)


def _fetch_public_klines(session, cfg: Settings) -> list[Candle]:
    now = int(time.time() * 1000)
    start = min(now - cfg.min_1m_bars * 60_000 - 300_000, now // 86_400_000 * 86_400_000)
    rows = []
    end = now
    while True:
        resp = session.get_kline(category=cfg.category, symbol=cfg.symbol, interval="1", limit=1000, end=end)
        page = (resp.get("result") or {}).get("list") or []
        if not page:
            break
        rows.extend(page)
        oldest = min(int(item[0]) for item in page)
        if oldest <= start:
            break
        if oldest >= end:
            raise RuntimeError("kline pagination did not advance")
        end = oldest - 1
    rows = sorted({int(item[0]): item for item in rows}.values(), key=lambda item: int(item[0]))
    candles = [
        Candle(ts=int(item[0]), o=D(item[1]), h=D(item[2]), l=D(item[3]), c=D(item[4]), v=D(item[5]))
        for item in rows
    ]
    return candles


def candles_contiguous(bars: list[Candle]) -> bool:
    return bool(bars) and all(right.ts - left.ts == 60_000 for left, right in zip(bars, bars[1:]))


def _dispatch_private(message: dict, engine: Engine, broker, cfg: Settings) -> None:
    topic = str(message.get("topic") or "")
    if topic == "execution" and broker is not None and hasattr(broker, "ingest_execution_message"):
        for ex in broker.ingest_execution_message(message):
            engine.on_exec(ex)
        return
    if topic == "order":
        from bybit_scalper.exchange import map_order_status

        for row in message.get("data") or []:
            if row.get("symbol") != cfg.symbol:
                continue
            link = str(row.get("orderLinkId") or "")
            if link:
                engine.note_remote_status(link, map_order_status(row.get("orderStatus")))


def _start_private(cfg: Settings, key: str, secret: str, messages, overflow: list[bool]):
    from pybit.unified_trading import WebSocket

    def callback(message) -> None:
        payload = json.dumps(message) if not isinstance(message, str) else message
        try:
            messages.put_nowait(payload)
        except queue.Full:
            # queue.Queue only. PrivateQueue records its own drop and does not raise.
            overflow[0] = True

    ws = WebSocket(
        channel_type="private",
        testnet=cfg.testnet,
        demo=cfg.demo,
        api_key=key,
        api_secret=secret,
    )
    ws.order_stream(callback)
    ws.execution_stream(callback)
    ws.position_stream(callback)
    return ws


def collect_market(cfg: Settings, directory: str = "data/replay") -> str:
    """Public klines and public trades. A network error is fatal: no synthetic tape is written."""
    session = _public_http(cfg)
    try:
        bars = _fetch_public_klines(session, cfg)
        trades = _fetch_public_trades(session, cfg)
    except Exception as exc:
        raise SystemExit(
            "Сбор рыночных данных не выполнен, заглушка не создана. "
            f"Причина: {exc}"
        ) from exc
    if not bars:
        raise SystemExit("Публичные свечи не получены. Файл не создан.")
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    kline_path = out / f"{cfg.symbol}_klines.json"
    trade_path = out / f"{cfg.symbol}_trades.json"
    kline_path.write_text(
        json.dumps(
            [
                {"ts": bar.ts, "o": str(bar.o), "h": str(bar.h), "l": str(bar.l), "c": str(bar.c), "v": str(bar.v)}
                for bar in bars
            ]
        ),
        encoding="utf-8",
    )
    trade_path.write_text(json.dumps(trades), encoding="utf-8")
    return (
        f"Собрано свечей: {len(bars)}, публичных сделок: {len(trades)}. "
        f"Файлы: {kline_path}, {trade_path}. Это не отчёт доходности."
    )


def _fetch_public_trades(session, cfg: Settings) -> list[dict]:
    resp = session.get_public_trade_history(category=cfg.category, symbol=cfg.symbol, limit=1000)
    rows = (resp.get("result") or {}).get("list") or []
    trades = []
    for row in rows:
        trades.append(
            {
                "p": str(row.get("price") or row.get("p") or ""),
                "v": str(row.get("size") or row.get("v") or ""),
                "T": int(row.get("time") or row.get("T") or 0),
                "S": str(row.get("side") or row.get("S") or ""),
            }
        )
    return trades


def replay_market(cfg: Settings, directory: str = "data/replay") -> str:
    directory_path = Path(directory)
    kline_path = directory_path / f"{cfg.symbol}_klines.json"
    trade_path = directory_path / f"{cfg.symbol}_trades.json"
    if not kline_path.exists() or not trade_path.exists():
        return (
            "Лента не собрана. Статистика не выдумана. "
            "Сначала команда collect на машине с доступом к публичному API Bybit."
        )
    raw_bars = json.loads(kline_path.read_text(encoding="utf-8"))
    raw_trades = json.loads(trade_path.read_text(encoding="utf-8"))
    bars = [
        Candle(ts=int(row["ts"]), o=D(row["o"]), h=D(row["h"]), l=D(row["l"]), c=D(row["c"]), v=D(row["v"]))
        for row in raw_bars
    ]
    if not bars:
        return "Файл свечей пуст. Статистика не выдумана."
    return (
        f"Прочитано свечей: {len(bars)}, сделок ленты: {len(raw_trades)}. "
        "Доходность не рассчитывается: запись содержит только свечи и последние сделки, "
        "без истории bid/ask, очереди и времени funding. Стакан из close свечи не создаётся. "
        "Нужна синхронная запись стакана, ленты, закрытых свечей и funding для полноценного replay."
    )


def format_saved_report(cfg: Settings) -> str:
    path = Path("data/last_report.txt")
    if cfg.mode == "offline" and path.exists():
        return path.read_text(encoding="utf-8")
    if not Path(cfg.state_db).exists():
        return "Журнал выбранного режима пока не создан. Синтетический offline-отчёт вместо него не показывается."
    store = StateStore(cfg.state_db, readonly=True)
    try:
        trades = store.trades()
        submitted = int(store.get_kv("submitted", "0"))
        filled = int(store.get_kv("filled_entries", "0"))
        funding = store.sum_cashflow("funding", 0, 10**15)
        synced = store.get_kv("cashflow_synced_ms", "не подтверждено")
    finally:
        store.close()
    report = summarize(
        trades,
        submitted,
        filled,
        "журнал sqlite: " + cfg.mode,
        True,
        [
            "Profit factor, ожидание и просадка относятся к закрытым сделкам без распределения funding.",
            "Открытый PnL не включён; это не полная кривая капитала счёта.",
        ],
    )
    return (
        format_report(report).replace("Чистый PnL:", "Торговый PnL без funding:", 1)
        + f"\nУчтённый funding: {funding}"
        + f"\nСумма торгового PnL и funding: {report.net_pnl + funding}"
        + f"\nПоследняя синхронизация журнала, Unix ms: {synced}"
    )
