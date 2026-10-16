from __future__ import annotations

import logging
from decimal import Decimal

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
        )
        self._execs: dict[str, list[Exec]] = {}
        self._status: dict[str, OrderStatus] = {}

    def place_limit(
        self,
        link_id: str,
        side: Side,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> PlaceResult:
        try:
            resp = self.session.place_order(
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
        except self._failed as exc:
            raise TimeoutError("неясный ответ place_order, нужна сверка") from exc
        if int(resp.get("retCode", 0)) != 0:
            return PlaceResult(False, link_id, str(resp.get("retMsg") or "reject"))
        self._status[link_id] = OrderStatus.SUBMITTED
        order_id = str((resp.get("result") or {}).get("orderId") or "")
        return PlaceResult(True, link_id, "accepted", order_id=order_id)

    def cancel(self, link_id: str) -> None:
        try:
            self.session.cancel_order(
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                orderLinkId=link_id,
            )
        except self._invalid as exc:
            log.info("cancel %s: %s", link_id, exc.message)
        self._refresh_execs(link_id)

    def executions(self, link_id: str) -> list[Exec]:
        self._refresh_execs(link_id)
        return list(self._execs.get(link_id, []))

    def order_status(self, link_id: str) -> OrderStatus | None:
        if link_id in self._status and self._status[link_id] != OrderStatus.UNKNOWN:
            cached = self._remote_status(link_id)
            if cached is not None:
                return cached
            return self._status.get(link_id)
        return self._remote_status(link_id)

    def set_stop(self, stop: Decimal, target: Decimal) -> StopResult:
        try:
            self.session.set_trading_stop(
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
        except (self._invalid, self._failed) as exc:
            return StopResult(False, str(getattr(exc, "message", exc)))
        if not self._stop_matches(stop):
            return StopResult(False, "стоп не подтверждён позицией")
        return StopResult(True, "stop set")

    def clear_stop(self) -> None:
        if self.position_snapshot().is_open():
            return
        try:
            self.session.set_trading_stop(
                category=self.cfg.category,
                symbol=self.cfg.symbol,
                tpslMode="Full",
                positionIdx=0,
                stopLoss="0",
                takeProfit="0",
            )
        except (self._invalid, self._failed):
            return

    def close_market(self, qty: Decimal, side: Side, mark: Decimal, ts: int) -> Exec:
        link_id = new_link_id()
        try:
            self.session.place_order(
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
        except (self._invalid, self._failed) as exc:
            raise TimeoutError("аварийное закрытие не принято") from exc
        self._refresh_execs(link_id)
        found = self._execs.get(link_id) or []
        if not found:
            raise TimeoutError("аварийное закрытие без исполнения")
        return found[-1]

    def position_snapshot(self) -> Position:
        resp = self.session.get_positions(category=self.cfg.category, symbol=self.cfg.symbol)
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
            ex = self._parse_exec(row)
            if ex is None:
                continue
            bucket = self._execs.setdefault(ex.link_id, [])
            if any(item.exec_id == ex.exec_id for item in bucket):
                continue
            bucket.append(ex)
            produced.append(ex)
        return produced

    def server_time_ms(self) -> int:
        resp = self.session.get_server_time()
        nano = (resp.get("result") or {}).get("timeNano")
        if nano:
            return int(nano) // 1_000_000
        return int((resp.get("result") or {}).get("timeSecond") or 0) * 1000

    def load_context(self) -> tuple[Instrument, Fees, Decimal, int]:
        server_ms = self.server_time_ms()
        info = self.session.get_instruments_info(category=self.cfg.category, symbol=self.cfg.symbol)
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
        )
        self.instrument = instrument
        fees_resp = self.session.get_fee_rates(category=self.cfg.category, symbol=self.cfg.symbol)
        fee_row = ((fees_resp.get("result") or {}).get("list") or [{}])[0]
        fees = Fees(
            maker=D(fee_row["makerFeeRate"]),
            taker=D(fee_row["takerFeeRate"]),
            source="bybit-api",
        )
        bal = self.session.get_wallet_balance(accountType="UNIFIED")
        equity = D(((bal.get("result") or {}).get("list") or [{}])[0].get("totalEquity") or "0")
        self._assert_account_safe()
        return instrument, fees, equity, server_ms

    def funding(self) -> tuple[Decimal, int]:
        resp = self.session.get_tickers(category=self.cfg.category, symbol=self.cfg.symbol)
        row = ((resp.get("result") or {}).get("list") or [{}])[0]
        return D(row.get("fundingRate") or "0"), int(row.get("nextFundingTime") or 0)

    def fetch_klines(self, limit: int = 1000) -> list:
        from bybit_scalper.types import Candle

        resp = self.session.get_kline(
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
        resp = self.session.get_positions(category=self.cfg.category, settleCoin="USDT")
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
            log.warning("На счёте есть чужие позиции. Плечо, маржу и режим не меняю.")

    def _refresh_execs(self, link_id: str) -> None:
        resp = self.session.get_executions(
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            orderLinkId=link_id,
            limit=50,
        )
        for row in (resp.get("result") or {}).get("list") or []:
            ex = self._parse_exec(row)
            if ex is None:
                continue
            bucket = self._execs.setdefault(ex.link_id or link_id, [])
            if any(item.exec_id == ex.exec_id for item in bucket):
                continue
            bucket.append(ex)

    def _remote_status(self, link_id: str) -> OrderStatus | None:
        open_resp = self.session.get_open_orders(
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            orderLinkId=link_id,
        )
        rows = (open_resp.get("result") or {}).get("list") or []
        if rows:
            status = map_order_status(rows[0].get("orderStatus"))
            self._status[link_id] = status
            return status
        hist = self.session.get_order_history(
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
        return abs(remote.stop - stop) <= self.instrument.tick

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
