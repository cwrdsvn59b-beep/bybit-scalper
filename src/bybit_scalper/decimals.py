from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP


def D(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def floor_tick(value: Decimal, tick: Decimal) -> Decimal:
    return floor_step(value, tick)


def ceil_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        raise ValueError("tick must be positive")
    units = (value / tick).to_integral_value(rounding=ROUND_UP)
    return units * tick
