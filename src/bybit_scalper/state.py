from __future__ import annotations

import json
import sqlite3
import os
import hashlib
from decimal import Decimal
from pathlib import Path
from contextlib import contextmanager

from bybit_scalper.decimals import D
from bybit_scalper.types import ClosedTrade, OrderStatus, OrderView, Position, Side


@contextmanager
def account_lease(environment: str, user_id: str):
    """One process per authenticated UID/environment on this host, across state paths/keys.

    This is a local OS lock, not a distributed lease across different machines/users.
    """
    root = Path(os.environ.get("BYBIT_LOCK_DIR", str(Path.home() / ".bybit_scalper" / "leases")))
    scope = hashlib.sha256((environment + ":" + user_id).encode()).hexdigest()
    lease = StateStore(str(root / (scope + ".db")))
    try:
        yield
    finally:
        lease.close()


class StateStore:
    def __init__(self, path: str, *, readonly: bool = False) -> None:
        if readonly:
            self._lock_fd = None
            self._transaction_depth = 0
            self.conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
            return
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock_fd = None
        if path != ':memory:':
            self._lock_fd = os.open(path + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
            try:
                if os.name == 'nt':
                    import msvcrt
                    if os.fstat(self._lock_fd).st_size == 0:
                        os.write(self._lock_fd, b'0')
                    os.lseek(self._lock_fd, 0, os.SEEK_SET)
                    msvcrt.locking(self._lock_fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                os.close(self._lock_fd)
                self._lock_fd = None
                raise RuntimeError('Эту базу уже использует другой процесс бота') from exc
        try:
            self.conn = sqlite3.connect(path)
        except BaseException:
            if self._lock_fd is not None:
                os.close(self._lock_fd)
                self._lock_fd = None
            raise
        self._transaction_depth = 0
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
            CREATE TABLE IF NOT EXISTS cashflows (
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                cash TEXT NOT NULL,
                ts INTEGER NOT NULL,
                PRIMARY KEY (source, external_id)
            );
            CREATE TABLE IF NOT EXISTS cashflow_aliases (
                alias TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL
            );
            """
        )
        self._commit()

    def close(self) -> None:
        self.conn.close()
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _commit(self) -> None:
        if self._transaction_depth == 0:
            self.conn.commit()

    @contextmanager
    def transaction(self):
        outer = self._transaction_depth == 0
        if outer:
            self.conn.execute("BEGIN IMMEDIATE")
        self._transaction_depth += 1
        try:
            yield
        except BaseException:
            if outer:
                self.conn.rollback()
            raise
        else:
            if outer:
                self.conn.commit()
        finally:
            self._transaction_depth -= 1

    def bind_identity(self, mode: str, symbol: str, account_fingerprint: str) -> None:
        identity = json.dumps([mode, symbol, account_fingerprint])
        saved = self.get_kv("identity")
        if saved and saved != identity:
            raise RuntimeError("База состояния относится к другому режиму, инструменту или аккаунту.")
        if not saved and (self.load_position().is_open() or self.load_order() is not None):
            raise RuntimeError("Старая база без идентификатора: нужна ручная сверка перед использованием.")
        self.set_kv("identity", identity)

    def add_exec(self, exec_id: str, link_id: str, qty: Decimal, price: Decimal, fee: Decimal, side: str, ts: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO execs (exec_id, link_id, qty, price, fee, side, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (exec_id, link_id, str(qty), str(price), str(fee), side, ts),
        )
        self._commit()
        return cur.rowcount == 1

    def add_cashflow(
        self,
        source: str,
        external_id: str,
        kind: str,
        cash: Decimal,
        ts: int,
        aliases: list[str],
    ) -> bool:
        """False if any alias is already stored. That blocks execution/log double counting."""
        keys: list[str] = []
        for alias in [external_id, *aliases]:
            if alias and alias not in keys:
                keys.append(alias)
        if not external_id or not keys:
            return False
        for alias in keys:
            found = self.conn.execute(
                "SELECT 1 FROM cashflow_aliases WHERE alias = ?",
                (alias,),
            ).fetchone()
            if found:
                return False
        self.conn.execute(
            "INSERT INTO cashflows (source, external_id, kind, cash, ts) VALUES (?, ?, ?, ?, ?)",
            (source, external_id, kind, str(cash), int(ts)),
        )
        for alias in keys:
            self.conn.execute(
                "INSERT INTO cashflow_aliases (alias, source, external_id) VALUES (?, ?, ?)",
                (alias, source, external_id),
            )
        self._commit()
        return True

    def sum_cashflow(self, kind: str, start_ms: int, end_ms: int) -> Decimal:
        rows = self.conn.execute(
            "SELECT cash FROM cashflows WHERE kind = ? AND ts >= ? AND ts < ?",
            (kind, int(start_ms), int(end_ms)),
        ).fetchall()
        total = Decimal("0")
        for (cash,) in rows:
            total += D(cash)
        return total

    def cashflow_events(self, kind: str) -> list[tuple[int, Decimal]]:
        rows = self.conn.execute(
            "SELECT ts, cash FROM cashflows WHERE kind = ? ORDER BY ts, external_id",
            (kind,),
        ).fetchall()
        return [(int(ts), D(cash)) for ts, cash in rows]

    def replace_cashflow_window(self, start_ms: int, end_ms: int, records: list[tuple]) -> None:
        """Atomically repair a fully fetched funding interval using transaction-log only.

        Funding execId and transaction tradeId are not documented as a cross-source key.
        Legacy execution/transaction rows in the covered interval are replaced, not added.
        Trades, daily limits and records outside the covered interval are preserved.
        """
        with self.transaction():
            self.conn.execute(
                "DELETE FROM cashflow_aliases WHERE (source, external_id) IN "
                "(SELECT source, external_id FROM cashflows WHERE kind = 'funding' AND ts >= ? AND ts < ?)",
                (start_ms, end_ms),
            )
            self.conn.execute(
                "DELETE FROM cashflows WHERE kind = 'funding' AND ts >= ? AND ts < ?",
                (start_ms, end_ms),
            )
            for kind, external, cash, ts, _aliases in records:
                # A single authoritative source makes cross-source ID guesses unnecessary.
                self.add_cashflow("transaction", external, kind, cash, ts, [external])
            self.set_kv("cashflow_synced_ms", str(end_ms - 1))

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
        self._commit()

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
        self._commit()

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
            self._commit()
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
        self._commit()

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
        self._commit()

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
        self._commit()

    def get_kv(self, key: str, default: str = "") -> str:
        row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return default if row is None else str(row[0])
