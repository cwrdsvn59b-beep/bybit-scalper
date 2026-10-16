from __future__ import annotations

from decimal import Decimal

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D

# A dedicated subaccount should match allocated_capital. Anything larger can be
# lost on a cross liquidation even when the bot sizes off the smaller budget.
_EQUITY_SLACK = D("1.05")
_AVAILABLE_FLOOR = D("0.90")


def live_blockers(
    cfg: Settings,
    *,
    margin_verified: bool,
    margin_mode: str,
    leverage: Decimal,
    equity: Decimal,
    available: Decimal,
    funding_ok: bool,
    clock_skew_ms: int,
    position_open: bool,
) -> list[str]:
    """Reasons live must not open a new risk. Empty means the account shape is acceptable.

    This does not prove the strategy has an edge.
    """
    reasons: list[str] = []
    allocated = cfg.d("allocated_capital")
    if not margin_verified or margin_mode != "cross":
        reasons.append("нужен подтверждённый REGULAR_MARGIN (cross); isolated и portfolio не торгуются")
    if not (D("0") < leverage <= cfg.d("max_leverage")):
        reasons.append("плечо не подтверждено или выше 2")
    if not funding_ok:
        reasons.append("журнал funding не подтверждён, новые входы закрыты")
    if clock_skew_ms > cfg.max_clock_skew_ms:
        reasons.append("расхождение часов больше допуска")
    if not equity.is_finite() or not available.is_finite() or equity <= 0 or allocated <= 0:
        reasons.append("equity или allocated_capital не являются положительным числом")
        return reasons
    if allocated > equity:
        reasons.append("allocated_capital больше equity счёта")
    elif equity > allocated * _EQUITY_SLACK:
        reasons.append(
            "на счёте больше денег, чем выделенный капитал. "
            "Для live нужен отдельный субсчёт примерно равный allocated_capital, "
            "иначе cross-ликвидация задевает лишние средства"
        )
    if not position_open and available < equity * _AVAILABLE_FLOOR:
        reasons.append("при пустой позиции доступная маржа сильно меньше equity")
    return reasons
