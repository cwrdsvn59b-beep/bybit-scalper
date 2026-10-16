from __future__ import annotations

from decimal import Decimal

from bybit_scalper.decimals import D
from bybit_scalper.types import Candle


def ema(values: list[Decimal], period: int) -> list[Decimal | None]:
    """EMA с посевом SMA. Индексы до period-1 равны None. Будущие значения не читаются."""
    out: list[Decimal | None] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    seed = sum(values[:period], Decimal("0")) / D(period)
    out[period - 1] = seed
    k = D(2) / D(period + 1)
    one = Decimal("1")
    prev = seed
    for i in range(period, len(values)):
        prev = values[i] * k + prev * (one - k)
        out[i] = prev
    return out


def atr(candles: list[Candle], period: int) -> list[Decimal | None]:
    """Wilder ATR. Бар i использует только свечи 0..i."""
    out: list[Decimal | None] = [None] * len(candles)
    if period <= 0 or len(candles) < period:
        return out
    trs: list[Decimal] = []
    for i, bar in enumerate(candles):
        if i == 0:
            trs.append(bar.h - bar.l)
        else:
            prev_c = candles[i - 1].c
            trs.append(max(bar.h - bar.l, abs(bar.h - prev_c), abs(bar.l - prev_c)))
    seed = sum(trs[:period], Decimal("0")) / D(period)
    out[period - 1] = seed
    prev = seed
    n = D(period)
    for i in range(period, len(trs)):
        prev = (prev * (n - 1) + trs[i]) / n
        out[i] = prev
    return out


def vwap_utc_day(candles: list[Candle], index: int) -> Decimal | None:
    """Дневной VWAP с 00:00 UTC по типичной цене. Только свечи этого дня до index включительно."""
    if index < 0 or index >= len(candles):
        return None
    day = candles[index].ts // 86_400_000
    pv = Decimal("0")
    vol = Decimal("0")
    for bar in candles[: index + 1]:
        if bar.ts // 86_400_000 != day:
            continue
        typical = (bar.h + bar.l + bar.c) / D(3)
        pv += typical * bar.v
        vol += bar.v
    if vol <= 0:
        return None
    return pv / vol


def closed_5m(bars: list[Candle]) -> list[Candle]:
    """Только полные пятиминутки. Неполная последняя группа отбрасывается."""
    groups: dict[int, list[Candle]] = {}
    for bar in bars:
        groups.setdefault(bar.ts // 300_000, []).append(bar)
    out: list[Candle] = []
    for bucket in sorted(groups):
        group = sorted(groups[bucket], key=lambda x: x.ts)
        if len(group) < 5:
            continue
        group = group[:5]
        out.append(
            Candle(
                ts=group[0].ts,
                o=group[0].o,
                h=max(x.h for x in group),
                l=min(x.l for x in group),
                c=group[-1].c,
                v=sum((x.v for x in group), Decimal("0")),
            )
        )
    return out


def confirmed_swings(candles: list[Candle], up_to: int) -> tuple[list[int], list[int]]:
    """Локальный экстремум подтверждается следующим уже закрытым баром.

    Для индекса i нужен бар i+1 <= up_to. Бар up_to сам экстремумом ещё не считается.
    """
    lows: list[int] = []
    highs: list[int] = []
    last = min(up_to, len(candles) - 1)
    for i in range(1, last):
        if candles[i].l < candles[i - 1].l and candles[i].l < candles[i + 1].l:
            lows.append(i)
        if candles[i].h > candles[i - 1].h and candles[i].h > candles[i + 1].h:
            highs.append(i)
    return lows, highs
