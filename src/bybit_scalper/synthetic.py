from __future__ import annotations

from decimal import Decimal

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.indicators import atr, ema
from bybit_scalper.signals import evaluate
from bybit_scalper.types import Candle


def _bar(ts: int, price: Decimal, wick: Decimal = Decimal("1"), volume: Decimal = Decimal("10")) -> Candle:
    return Candle(ts=ts, o=price, h=price + wick, l=price - wick, c=price + D("0.2"), v=volume)


def build_long_fixture(cfg: Settings, start_ms: int = 1_700_000_000_000) -> list[Candle]:
    """Восходящий ряд и один откат. Параметры стратегии здесь не подбираются.

    Последний бар подгоняется циклом только для того, чтобы фикстура вообще
    проходила уже зафиксированные правила (касание обновлённой EMA).
    """
    start_ms = start_ms - (start_ms % 300_000)
    price = D("100000")
    bars: list[Candle] = []
    for n in range(420):
        price += D("4")
        bars.append(_bar(start_ms + n * 60_000, price, wick=D("20")))

    def plant(
        index: int,
        low: Decimal | None = None,
        high: Decimal | None = None,
        close: Decimal | None = None,
    ) -> None:
        bar = bars[index]
        new_h = high if high is not None else bar.h
        new_l = low if low is not None else bar.l
        new_c = close if close is not None else bar.c
        if new_h < max(bar.o, new_c):
            new_h = max(bar.o, new_c)
        if new_l > min(bar.o, new_c):
            new_l = min(bar.o, new_c)
        if new_l > new_h:
            new_l = new_h
        bars[index] = Candle(ts=bar.ts, o=bar.o, h=new_h, l=new_l, c=new_c, v=bar.v)

    i = len(bars) - 1
    plant(i - 24, low=bars[i - 24].l - D("30"))
    plant(i - 12, low=bars[i - 12].l - D("10"))
    last_reason = "не запускалась"
    for _ in range(12):
        ema_now = ema([bar.c for bar in bars], cfg.ema_fast)[i]
        atr_now = atr(bars, cfg.atr_period)[i]
        if ema_now is None or atr_now is None:
            raise RuntimeError("EMA/ATR фикстуры не готовы")
        close = max(bars[i - 1].h + D("0.5"), ema_now + D("1"))
        plant(i, low=ema_now, high=close + D("0.4"), close=close)
        plant(i - 5, high=close + D("5000"))
        decision = evaluate(bars, cfg)
        last_reason = decision.reason
        if decision.accepted:
            return bars
    raise RuntimeError(f"фикстура не дала long: {last_reason}")


def candles_to_csv(bars: list[Candle]) -> str:
    lines = ["ts,open,high,low,close,volume"]
    for bar in bars:
        lines.append(f"{bar.ts},{bar.o},{bar.h},{bar.l},{bar.c},{bar.v}")
    return "\n".join(lines) + "\n"


def candles_from_csv(text: str) -> list[Candle]:
    rows = []
    for line in text.strip().splitlines()[1:]:
        ts, o, h, l, c, v = line.split(",")
        rows.append(Candle(ts=int(ts), o=D(o), h=D(h), l=D(l), c=D(c), v=D(v)))
    return rows
