from __future__ import annotations

from decimal import Decimal

from bybit_scalper.decimals import D
from bybit_scalper.types import ClosedTrade, Report


def summarize(
    trades: list[ClosedTrade],
    submitted: int,
    filled_entries: int,
    label: str,
    preliminary: bool,
    notes: list[str] | None = None,
) -> Report:
    net = sum((trade.pnl for trade in trades), Decimal("0"))
    fees = sum((trade.fees for trade in trades), Decimal("0"))
    slip = sum((trade.slippage for trade in trades), Decimal("0"))
    expectancy = net / D(len(trades)) if trades else Decimal("0")
    wins = sum((trade.pnl for trade in trades if trade.pnl > 0), Decimal("0"))
    losses = sum((trade.pnl for trade in trades if trade.pnl < 0), Decimal("0"))
    if not trades:
        factor = "n/a"
    elif losses == 0:
        factor = "inf" if wins > 0 else "n/a"
    else:
        factor = f"{(wins / abs(losses)):.4f}"
    equity = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")
    for trade in trades:
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_dd:
            max_dd = drawdown
    if submitted <= 0:
        fill_rate = "n/a"
    else:
        fill_rate = f"{(D(filled_entries) / D(submitted)):.4f}"
    return Report(
        label=label,
        net_pnl=net,
        expectancy=expectancy,
        profit_factor=factor,
        max_drawdown=max_dd,
        trades=len(trades),
        submitted=submitted,
        filled_entries=filled_entries,
        fill_rate=fill_rate,
        fees=fees,
        slippage=slip,
        preliminary=preliminary,
        notes=list(notes or []),
    )


def format_report(report: Report) -> str:
    lines = [
        f"=== {report.label} ===",
        f"Чистый PnL: {report.net_pnl}",
        f"Ожидание на сделку: {report.expectancy}",
        f"Profit factor: {report.profit_factor}",
        f"Макс. просадка: {report.max_drawdown}",
        f"Сделок: {report.trades}",
        f"Заявок отправлено: {report.submitted}",
        f"Входов с исполнением: {report.filled_entries}",
        f"Доля исполнений: {report.fill_rate}",
        f"Комиссии: {report.fees}",
        f"Оценка проскальзывания: {report.slippage}",
        f"Предварительный отчёт: {'да' if report.preliminary else 'нет'}",
    ]
    for note in report.notes:
        lines.append(f"- {note}")
    return "\n".join(lines)
