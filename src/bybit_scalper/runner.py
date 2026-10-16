from __future__ import annotations

import json
import logging
import queue
import threading
import time
from decimal import Decimal
from pathlib import Path

from bybit_scalper.book import OrderBook
from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.engine import Engine
from bybit_scalper.paper import PaperExchange
from bybit_scalper.report import format_report, summarize
from bybit_scalper.state import StateStore
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


def parse_trades(message: dict) -> list[tuple[Decimal, Decimal, int]]:
    prints = []
    for row in message.get("data") or []:
        prints.append((D(row["p"]), D(row["v"]), int(row["T"])))
    return prints


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

    def _run(self) -> None:
        import websocket

        while not self.stop.is_set():
            self.connected = False
            ws = None
            try:
                ws = websocket.create_connection(self.url(), timeout=10)
                args = [f"kline.1.{self.cfg.symbol}", f"orderbook.50.{self.cfg.symbol}"]
                if self.cfg.trade_flow_enabled:
                    args.append(f"publicTrade.{self.cfg.symbol}")
                ws.send(json.dumps({"op": "subscribe", "args": args}))
                self.connected = True
                self.needs_reconcile = True
                ws.settimeout(5)
                last_ping = time.time()
                while not self.stop.is_set():
                    if time.time() - last_ping >= 20:
                        ws.send(json.dumps({"op": "ping"}))
                        last_ping = time.time()
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not raw:
                        break
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    try:
                        self.messages.put_nowait(raw)
                    except queue.Full:
                        self.overflow[0] = True
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
    instrument = cfg.instrument()
    fees = cfg.fallback_fees()
    equity = cfg.d("offline_equity")
    broker: PaperExchange | PybitBroker
    if private:
        broker = PybitBroker(cfg, key, secret, instrument)
        instrument, fees, equity, server_ms = broker.load_context()
        log.info("комиссии API maker=%s taker=%s источник=%s", fees.maker, fees.taker, fees.source)
        log.info("капитал UNIFIED %s, сервер %s", equity, server_ms)
    else:
        broker = PaperExchange(instrument, fees, cfg.d("slippage_bps"), Decimal("1"))
        log.info("paper: ключи не используются, комиссии из конфигурации %s", fees.source)
        try:
            server_ms = int(market_http.get_server_time()["result"]["timeNano"]) // 1_000_000
        except Exception:
            server_ms = int(time.time() * 1000)
    store = StateStore(cfg.state_db)
    engine = Engine(cfg, store, broker, fees, instrument)
    engine.recover(history[-1].ts, equity)
    if private:
        engine.reconcile()
    engine.note_clock(int(time.time() * 1000), server_ms)
    messages: queue.Queue = queue.Queue(maxsize=cfg.queue_max)
    overflow = [False]
    stream = PublicStream(cfg, messages, overflow)
    stream.start()
    private_ws = None
    if private:
        private_ws = _start_private(cfg, key, secret, messages, overflow)
    book = OrderBook()
    bars = history
    last_reconcile = time.time()
    last_aux = 0.0
    funding_rate = Decimal("0")
    next_funding = 0
    log.info("цикл запущен, режим %s, символ %s. Остановка: Ctrl+C", cfg.mode, cfg.symbol)
    try:
        while True:
            if stream.needs_reconcile and engine.block == BlockReason.DISCONNECT:
                stream.needs_reconcile = False
                engine.on_reconnect()
            now_wall = time.time()
            if now_wall - last_aux > 30:
                last_aux = now_wall
                funding_rate, next_funding = _refresh_public(market_http, cfg, engine, funding_rate, next_funding)
            try:
                raw = messages.get(timeout=0.5)
            except queue.Empty:
                _mark_link(engine, stream, book, bars, equity, overflow, funding_rate, next_funding)
                if private and time.time() - last_reconcile > 5 and engine.order and engine.order.active:
                    engine.note_request(int(time.time() * 1000))
                    engine.reconcile()
                    last_reconcile = time.time()
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            topic = str(message.get("topic") or "")
            if message.get("op") in {"pong", "ping"} or message.get("success") is True:
                continue
            if topic.startswith("kline"):
                parsed = parse_kline(message)
                if parsed is None:
                    continue
                candle, confirmed = parsed
                if confirmed:
                    upsert_candle(bars, candle)
                    _on_market(engine, bars, book, equity, candle.c, overflow, funding_rate, next_funding, stream)
                else:
                    engine.mark = candle.c
            elif topic.startswith("orderbook"):
                book.apply(message)
                if bars:
                    _on_market(engine, bars, book, equity, bars[-1].c, overflow, funding_rate, next_funding, stream)
            elif topic.startswith("publicTrade"):
                for price, qty, ts in parse_trades(message):
                    engine.on_trade_print(price, qty, ts, simulate_fill=not private)
            elif topic == "execution" and private:
                for ex in broker.ingest_execution_message(message):  # type: ignore[union-attr]
                    engine.on_exec(ex)
            elif topic == "order" and private:
                for row in message.get("data") or []:
                    from bybit_scalper.exchange import map_order_status

                    link = str(row.get("orderLinkId") or "")
                    if link:
                        engine.note_remote_status(link, map_order_status(row.get("orderStatus")))
            elif topic == "position" and private:
                engine.reconcile()
    except KeyboardInterrupt:
        log.info("остановка по Ctrl+C")
        _shutdown(engine)
    finally:
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
    market = MarketState(
        bars=list(bars),
        book=view,
        now_ms=now,
        equity=equity,
        mark=mark,
        stale=stale_book or stale_candle or not stream.connected,
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
    resp = session.get_kline(category=cfg.category, symbol=cfg.symbol, interval="1", limit=1000)
    rows = list(reversed((resp.get("result") or {}).get("list") or []))
    candles = [
        Candle(ts=int(item[0]), o=D(item[1]), h=D(item[2]), l=D(item[3]), c=D(item[4]), v=D(item[5]))
        for item in rows
    ]
    return candles


def _start_private(cfg: Settings, key: str, secret: str, messages: queue.Queue, overflow: list[bool]):
    from pybit.unified_trading import WebSocket

    def callback(message) -> None:
        try:
            messages.put_nowait(json.dumps(message) if not isinstance(message, str) else message)
        except queue.Full:
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


def format_saved_report(cfg: Settings) -> str:
    path = Path("data/last_report.txt")
    if path.exists():
        return path.read_text(encoding="utf-8")
    store = StateStore(cfg.state_db)
    trades = store.trades()
    store.close()
    submitted = 0
    filled = sum(1 for trade in trades if trade.qty > 0)
    report = summarize(
        trades,
        submitted,
        filled,
        "журнал sqlite",
        True,
        ["Нет data/last_report.txt. Показаны только уже записанные сделки. Доля исполнений отсюда неизвестна."],
    )
    return format_report(report)
