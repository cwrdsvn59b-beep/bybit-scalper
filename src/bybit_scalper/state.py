from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from bybit_scalper.decimals import D
from bybit_scalper.types import ClosedTrade, OrderStatus, OrderView, Position, Side


class StateStore:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init()

    def _init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS execs (
                exec_id TEXT PRIMARY KEY,
                link_id TEXT NOT NULL,
                qty TEXT NOT NULL,
                price TEXT NOT NULL,
                fee TEXT NOT NULL,
                side TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                link_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS position (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily (
                day TEXT PRIMARY KEY,
                start_equity TEXT NOT NULL,
                realized TEXT NOT NULL,
                consec_losses INTEGER NOT NULL,
                pause_until INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry TEXT NOT NULL,
                exit TEXT NOT NULL,
                pnl TEXT NOT NULL,
                fees TEXT NOT NULL,
                slippage TEXT NOT NULL,
                reason TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def add_exec(self, exec_id: str, link_id: str, qty: Decimal, price: Decimal, fee: Decimal, side: str, ts: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO execs (exec_id, link_id, qty, price, fee, side, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (exec_id, link_id, str(qty), str(price), str(fee), side, ts),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def save_order(self, order: OrderView | None) -> None:
        self.conn.execute("DELETE FROM orders")
        if order is not None:
            payload = json.dumps(
                {
                    "link_id": order.link_id,
                    "status": order.status.value,
                    "side": order.side.value,
                    "price": str(order.price),
                    "qty": str(order.qty),
                    "filled": str(order.filled),
                    "stop": str(order.stop),
                    "target": str(order.target),
                    "opened_ms": order.opened_ms,
                    "bar_ts": order.bar_ts,
                }
            )
            self.conn.execute("INSERT INTO orders (link_id, payload) VALUES (?, ?)", (order.link_id, payload))
        self.conn.commit()

    def load_order(self) -> OrderView | None:
        row = self.conn.execute("SELECT payload FROM orders").fetchone()
        if not row:
            return None
        raw = json.loads(row[0])
        return OrderView(
            link_id=raw["link_id"],
            status=OrderStatus(raw["status"]),
            side=Side(raw["side"]),
            price=D(raw["price"]),
            qty=D(raw["qty"]),
            filled=D(raw["filled"]),
            stop=D(raw["stop"]),
            target=D(raw["target"]),
            opened_ms=int(raw["opened_ms"]),
            bar_ts=int(raw["bar_ts"]),
        )

    def save_position(self, position: Position) -> None:
        payload = json.dumps(
            {
                "side": None if position.side is None else position.side.value,
                "qty": str(position.qty),
                "avg": str(position.avg),
                "stop": str(position.stop),
                "target": str(position.target),
                "opened_ms": position.opened_ms,
                "stop_confirmed": position.stop_confirmed,
                "status": position.status,
            }
        )
        self.conn.execute(
            "INSERT INTO position (id, payload) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
            (payload,),
        )
        self.conn.commit()

    def load_position(self) -> Position:
        row = self.conn.execute("SELECT payload FROM position WHERE id = 1").fetchone()
        if not row:
            return Position()
        raw = json.loads(row[0])
        return Position(
            side=None if raw["side"] is None else Side(raw["side"]),
            qty=D(raw["qty"]),
            avg=D(raw["avg"]),
            stop=D(raw["stop"]),
            target=D(raw["target"]),
            opened_ms=int(raw["opened_ms"]),
            stop_confirmed=bool(raw["stop_confirmed"]),
            status=raw["status"],
        )

    def ensure_day(self, day: str, equity: Decimal) -> dict:
        row = self.conn.execute(
            "SELECT start_equity, realized, consec_losses, pause_until FROM daily WHERE day = ?",
            (day,),
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO daily (day, start_equity, realized, consec_losses, pause_until) VALUES (?, ?, '0', 0, 0)",
                (day, str(equity)),
            )
            self.conn.commit()
            return {"start_equity": equity, "realized": Decimal("0"), "consec_losses": 0, "pause_until": 0}
        return {
            "start_equity": D(row[0]),
            "realized": D(row[1]),
            "consec_losses": int(row[2]),
            "pause_until": int(row[3]),
        }

    def update_day(self, day: str, realized: Decimal, consec_losses: int, pause_until: int) -> None:
        self.conn.execute(
            "UPDATE daily SET realized = ?, consec_losses = ?, pause_until = ? WHERE day = ?",
            (str(realized), consec_losses, pause_until, day),
        )
        self.conn.commit()

    def add_trade(self, trade: ClosedTrade) -> None:
        self.conn.execute(
            "INSERT INTO trades (ts, side, qty, entry, exit, pnl, fees, slippage, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                trade.ts,
                trade.side,
                str(trade.qty),
                str(trade.entry),
                str(trade.exit),
                str(trade.pnl),
                str(trade.fees),
                str(trade.slippage),
                trade.reason,
            ),
        )
        self.conn.commit()

    def trades(self) -> list[ClosedTrade]:
        rows = self.conn.execute(
            "SELECT ts, side, qty, entry, exit, pnl, fees, slippage, reason FROM trades ORDER BY id"
        ).fetchall()
        return [
            ClosedTrade(
                ts=int(r[0]),
                side=r[1],
                qty=D(r[2]),
                entry=D(r[3]),
                exit=D(r[4]),
                pnl=D(r[5]),
                fees=D(r[6]),
                slippage=D(r[7]),
                reason=r[8],
            )
            for r in rows
        ]

    def set_kv(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, value),
        )
        self.conn.commit()

    def get_kv(self, key: str, default: str = "") -> str:
        row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return default if row is None else str(row[0])
