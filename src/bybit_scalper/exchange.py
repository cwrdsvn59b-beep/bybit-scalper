from __future__ import annotations

import logging
import time
from collections import deque
from decimal import Decimal

import requests

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.risk import new_link_id
from bybit_scalper.types import Exec, Fees, Instrument, OrderStatus, PlaceResult, Position, Side, StopResult

log = logging.getLogger("bybit_scalper")


def map_order_status(value: str | None) -> OrderStatus:
    table = {
        "New": OrderStatus.SUBMITTED,
        "Created": OrderStatus.SUBMITTED,
        "PartiallyFilled": OrderStatus.PARTIAL,
        "Filled": OrderStatus.FILLED,
        "Cancelled": OrderStatus.CANCELLED,
        "PartiallyFilledCanceled": OrderStatus.CANCELLED,
        "Rejected": OrderStatus.REJECTED,
        "Deactivated": OrderStatus.CANCELLED,
    }
    return table.get(value or "", OrderStatus.UNKNOWN)


class PybitBroker:
    """HTTP pybit 5.17.0. Принятие заявки не считается исполнением.

    set_leverage, switch_margin_mode и switch_position_mode сознательно не вызываются.
    """

    def __init__(self, cfg: Settings, api_key: str, api_secret: str, instrument: Instrument) -> None:
        from pybit.exceptions import FailedRequestError, InvalidRequestError
        from pybit.unified_trading import HTTP

        self.cfg = cfg
        self.instrument = instrument
        self._invalid = InvalidRequestError
        self._failed = FailedRequestError
        self.session = HTTP(
            testnet=cfg.testnet,
            demo=cfg.demo,
            api_key=api_key,
            api_secret=api_secret,
            log_requests=False,
            logging_level=logging.WARNING,
            timeout=3,
            max_retries=1,
            force_retry=False,
        )
        self._calls: deque[float] = deque()
        self._execs: dict[str, list[Exec]] = {}
        self._status: dict[str, OrderStatus] = {}

    def _call(self, method: str, **kwargs):
        calls = getattr(self, "_calls", None)
        if calls is None:
            self._calls = calls = deque()
        limit = self.cfg.max_requests_per_sec
        now = time.monotonic()
        while calls and now - calls[0] >= 1:
            calls.popleft()
        if len(calls) >= limit:
            time.sleep(max(0, 1 - (now - calls[0])))
            now = time.monotonic()
            while calls and now - calls[0] >= 1:
                calls.popleft()
        calls.append(time.monotonic())
        return getattr(self.session, method)(**kwargs)

    def place_limit(
        self,
        link_id: str,
        side: Side,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> PlaceResult:
        try:
            resp = self._call("place_order",
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                side="Buy" if side is Side.LONG else "Sell",
                orderType="Limit",
                qty=str(qty),
                price=str(price),
                timeInForce="PostOnly",
                orderLinkId=link_id,
                reduceOnly=reduce_only,
                positionIdx=0,
            )
        except self._invalid as exc:
            return PlaceResult(False, link_id, str(exc.message))
        except (self._failed, requests.RequestException) as exc:
            raise TimeoutError("неясный ответ place_order, нужна сверка") from exc
        if int(resp.get("retCode", 0)) != 0:
            return PlaceResult(False, link_id, str(resp.get("retMsg") or "reject"))
        self._status[link_id] = OrderStatus.SUBMITTED
        order_id = str((resp.get("result") or {}).get("orderId") or "")
        return PlaceResult(True, link_id, "accepted", order_id=order_id)

    def cancel(self, link_id: str) -> None:
        try:
            self._call("cancel_order",
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                orderLinkId=link_id,
            )
        except self._invalid as exc:
            # Even 'not found' can race a fill. Only a terminal status confirms cancellation.
            raise RuntimeError("cancel rejected; reconcile order status") from exc

    def executions(self, link_id: str) -> list[Exec]:
        self._refresh_execs(link_id)
        return sorted(self._execs.get(link_id, []), key=lambda ex: (ex.ts, ex.reduce_only, ex.exec_id))

    def order_status(self, link_id: str) -> OrderStatus | None:
        return self._remote_status(link_id)

    def set_stop(self, stop: Decimal, target: Decimal) -> StopResult:
        try:
            self._call("set_trading_stop",
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                tpslMode="Full",
                positionIdx=0,
                stopLoss=str(stop),
                takeProfit=str(target),
                slTriggerBy="LastPrice",
                tpTriggerBy="LastPrice",
                slOrderType="Market",
                tpOrderType="Market",
            )
        except (self._invalid, self._failed, requests.RequestException) as exc:
            return StopResult(False, str(getattr(exc, "message", exc)))
        if not self._stop_matches(stop):
            return StopResult(False, "стоп не подтверждён позицией")
        return StopResult(True, "stop set")

    def clear_stop(self) -> None:
        if self.position_snapshot().is_open():
            return
        try:
            self._call("set_trading_stop",
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                tpslMode="Full",
                positionIdx=0,
                stopLoss="0",
                takeProfit="0",
            )
        except (self._invalid, self._failed):
            return

    def close_market(self, qty: Decimal, side: Side, mark: Decimal, ts: int, *, link_id: str) -> PlaceResult:
        try:
            resp = self._call("place_order",
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                side="Sell" if side is Side.LONG else "Buy",
                orderType="Market",
                qty=str(qty),
                timeInForce="IOC",
                reduceOnly=True,
                orderLinkId=link_id,
                positionIdx=0,
            )
        except (self._invalid, self._failed, requests.RequestException) as exc:
            raise TimeoutError("аварийное закрытие не принято") from exc
        return PlaceResult(int(resp.get("retCode", 0)) == 0, link_id,
                           str(resp.get("retMsg", "accepted")),
                           str((resp.get("result") or {}).get("orderId", "")))

    def position_snapshot(self) -> Position:
        resp = self._call("get_positions", category=self.cfg.category, symbol=self.cfg.symbol)
        rows = (resp.get("result") or {}).get("list") or []
        for row in rows:
            if int(row.get("positionIdx") or 0) != 0:
                continue
            qty = D(row.get("size") or "0")
            if qty <= 0:
                continue
            side = Side.LONG if str(row.get("side")) == "Buy" else Side.SHORT
            return Position(
                side=side,
                qty=qty,
                avg=D(row.get("avgPrice") or "0"),
                stop=D(row.get("stopLoss") or "0"),
                target=D(row.get("takeProfit") or "0"),
                opened_ms=int(row.get("createdTime") or 0),
                stop_confirmed=D(row.get("stopLoss") or "0") > 0,
                status="open",
            )
        return Position()

    def ingest_execution_message(self, message: dict) -> list[Exec]:
        produced: list[Exec] = []
        for row in message.get("data") or []:
            if row.get("symbol") != self.cfg.symbol or row.get("category", self.cfg.category) != self.cfg.category:
                continue
            ex = self._parse_exec(row)
            if ex is None:
                continue
            bucket = self._execs.setdefault(ex.link_id, [])
            if not any(item.exec_id == ex.exec_id for item in bucket):
                bucket.append(ex)
            # Engine's durable execId journal owns deduplication, including REST-before-WS.
            produced.append(ex)
        return produced

    def server_time_ms(self) -> int:
        resp = self._call("get_server_time")
        nano = (resp.get("result") or {}).get("timeNano")
        if nano:
            return int(nano) // 1_000_000
        return int((resp.get("result") or {}).get("timeSecond") or 0) * 1000

    def load_context(self) -> tuple[Instrument, Fees, Decimal, int]:
        server_ms = self.server_time_ms()
        info = self._call("get_instruments_info", category=self.cfg.category, symbol=self.cfg.symbol)
        row = (info.get("result") or {}).get("list") or []
        if not row:
            raise RuntimeError("instrument not found")
        meta = row[0]
        lot = meta["lotSizeFilter"]
        instrument = Instrument(
            symbol=self.cfg.symbol,
            tick=D(meta["priceFilter"]["tickSize"]),
            qty_step=D(lot["qtyStep"]),
            min_qty=D(lot["minOrderQty"]),
            min_notional=D(lot.get("minNotionalValue") or "0"),
            max_qty=min(D(lot["maxOrderQty"]), D(lot["maxMktOrderQty"])),
        )
        self.instrument = instrument
        fees_resp = self._call("get_fee_rates", category=self.cfg.category, symbol=self.cfg.symbol)
        fee_row = ((fees_resp.get("result") or {}).get("list") or [{}])[0]
        fees = Fees(
            maker=D(fee_row["makerFeeRate"]),
            taker=D(fee_row["takerFeeRate"]),
            source="bybit-api",
        )
        equity = self.account_equity()
        self._assert_account_safe()
        return instrument, fees, equity, server_ms

    def account_equity(self) -> Decimal:
        bal = self._call("get_wallet_balance", accountType="UNIFIED")
        row = ((bal.get("result") or {}).get("list") or [{}])[0]
        equity = D(row.get("totalEquity") or "0")
        if equity <= 0:
            raise RuntimeError("Не удалось подтвердить положительный капитал UNIFIED")
        return equity

    def funding(self) -> tuple[Decimal, int]:
        resp = self._call("get_tickers", category=self.cfg.category, symbol=self.cfg.symbol)
        row = ((resp.get("result") or {}).get("list") or [{}])[0]
        return D(row.get("fundingRate") or "0"), int(row.get("nextFundingTime") or 0)

    def fetch_klines(self, limit: int = 1000) -> list:
        from bybit_scalper.types import Candle

        resp = self._call("get_kline",
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            interval="1",
            limit=limit,
        )
        rows = list(reversed((resp.get("result") or {}).get("list") or []))
        candles = []
        for item in rows:
            candles.append(
                Candle(
                    ts=int(item[0]),
                    o=D(item[1]),
                    h=D(item[2]),
                    l=D(item[3]),
                    c=D(item[4]),
                    v=D(item[5]),
                )
            )
        return candles

    def _assert_account_safe(self) -> None:
        resp = self._call("get_positions", category=self.cfg.category, settleCoin="USDT")
        rows = (resp.get("result") or {}).get("list") or []
        foreign = False
        for row in rows:
            idx = int(row.get("positionIdx") or 0)
            if idx in (1, 2):
                raise SystemExit(
                    "Обнаружен hedge mode (positionIdx 1 или 2). Режим не переключаю, запуск остановлен."
                )
            size = D(row.get("size") or "0")
            leverage = D(row.get("leverage") or "0")
            if size > 0 and row.get("symbol") != self.cfg.symbol:
                foreign = True
            if row.get("symbol") == self.cfg.symbol and leverage > self.cfg.d("max_leverage"):
                raise SystemExit(
                    "Плечо выше 2. Самостоятельно не меняю плечо и маржу, запуск остановлен."
                )
        if foreign:
            raise SystemExit("Нужен отдельный субсчёт: обнаружены посторонние позиции.")
        configured = self._call("get_positions", category=self.cfg.category, symbol=self.cfg.symbol)
        configured_rows = (configured.get("result") or {}).get("list") or []
        if len(configured_rows) != 1 or int(configured_rows[0].get("positionIdx", -1)) != 0:
            raise SystemExit("Не подтверждён One-way mode для инструмента")
        leverage = D(configured_rows[0].get("leverage") or "0")
        if not D('0') < leverage <= self.cfg.d('max_leverage'):
            raise SystemExit("Не подтверждено допустимое плечо для инструмента")

    def executions_since(self, start_ms: int) -> list[Exec]:
        return self._execution_pages(startTime=start_ms)

    def open_entry_orders(self) -> list[dict]:
        result: list[dict] = []
        cursor = ""
        while True:
            params = dict(category=self.cfg.category, settleCoin="USDT", limit=50)
            if cursor:
                params["cursor"] = cursor
            resp = self._call("get_open_orders", **params)
            payload = resp.get("result") or {}
            result.extend(row for row in payload.get("list", []) if not row.get("reduceOnly"))
            cursor = payload.get("nextPageCursor") or ""
            if not cursor:
                return result

    def _refresh_execs(self, link_id: str) -> None:
        self._execution_pages(orderLinkId=link_id)

    def _execution_pages(self, **filters) -> list[Exec]:
        found: dict[str, Exec] = {}
        cursor = ""
        seen: set[str] = set()
        while True:
            params = dict(category=self.cfg.category, symbol=self.cfg.symbol, limit=100, **filters)
            if cursor:
                params["cursor"] = cursor
            resp = self._call("get_executions", **params)
            payload = resp.get("result") or {}
            for row in payload.get("list") or []:
                if row.get("symbol", self.cfg.symbol) != self.cfg.symbol:
                    continue
                ex = self._parse_exec(row)
                if ex is not None:
                    found[ex.exec_id] = ex
            cursor = payload.get("nextPageCursor") or ""
            if not cursor:
                break
            if cursor in seen:
                raise RuntimeError("execution pagination cursor repeated")
            seen.add(cursor)
        for ex in found.values():
            bucket = self._execs.setdefault(ex.link_id, [])
            if any(item.exec_id == ex.exec_id for item in bucket):
                continue
            bucket.append(ex)
        return sorted(found.values(), key=lambda ex: (ex.ts, ex.reduce_only, ex.exec_id))

    def _remote_status(self, link_id: str) -> OrderStatus | None:
        open_resp = self._call("get_open_orders",
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            orderLinkId=link_id,
        )
        rows = (open_resp.get("result") or {}).get("list") or []
        if rows:
            status = map_order_status(rows[0].get("orderStatus"))
            self._status[link_id] = status
            return status
        hist = self._call("get_order_history",
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            orderLinkId=link_id,
            limit=1,
        )
        rows = (hist.get("result") or {}).get("list") or []
        if not rows:
            return None
        status = map_order_status(rows[0].get("orderStatus"))
        self._status[link_id] = status
        return status

    def _stop_matches(self, stop: Decimal) -> bool:
        remote = self.position_snapshot()
        if not remote.is_open():
            return False
        return remote.stop == stop

    @staticmethod
    def _parse_exec(row: dict) -> Exec | None:
        if row.get("execType") not in (None, "", "Trade"):
            return None
        exec_id = str(row.get("execId") or "")
        qty = D(row.get("execQty") or "0")
        if not exec_id or qty <= 0:
            return None
        closed = D(row.get("closedSize") or "0")
        return Exec(
            exec_id=exec_id,
            link_id=str(row.get("orderLinkId") or ""),
            side=str(row.get("side") or ""),
            qty=qty,
            price=D(row.get("execPrice") or "0"),
            fee=D(row.get("execFee") or "0"),
            ts=int(row.get("execTime") or 0),
            reduce_only=closed > 0,
        )
