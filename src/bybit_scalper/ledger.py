from __future__ import annotations

from decimal import Decimal

from bybit_scalper.decimals import D


def funding_from_execution(row: dict) -> tuple[str, Decimal, int, list[str]] | None:
    """Funding execution. execFee > 0 is cash paid by the account."""
    if str(row.get("execType") or "") != "Funding":
        return None
    exec_id = str(row.get("execId") or "")
    if not exec_id:
        return None
    cash = -D(row.get("execFee") or "0")
    ts = int(row.get("execTime") or 0)
    return exec_id, cash, ts, [exec_id]


def cashflow_from_transaction(row: dict) -> tuple[str, str, Decimal, int, list[str]] | None:
    """Account log row. Trade fees are ignored here so they are not booked twice.

    Returns kind, external id, signed cash, timestamp, alias ids.
    """
    external = str(row.get("id") or "")
    if not external:
        return None
    kind_raw = str(row.get("type") or "")
    cash = D(row.get("cashFlow") or "0")
    ts = int(row.get("transactionTime") or 0)
    aliases = [external]
    trade_id = str(row.get("tradeId") or "")
    if trade_id:
        aliases.append(trade_id)
    if kind_raw == "TRANSFER_IN":
        return "transfer_in", external, cash, ts, aliases
    if kind_raw == "TRANSFER_OUT":
        return "transfer_out", external, cash, ts, aliases
    if kind_raw == "SETTLEMENT":
        return "funding", external, cash, ts, aliases
    return None
