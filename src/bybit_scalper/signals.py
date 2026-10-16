from __future__ import annotations

from decimal import Decimal

from bybit_scalper.config import Settings
from bybit_scalper.decimals import D
from bybit_scalper.indicators import atr, closed_5m, confirmed_swings, ema, vwap_utc_day
from bybit_scalper.types import Candle, Decision, Side, Signal


def _rising(series: list[Decimal | None], lookback: int) -> bool:
    if len(series) <= lookback:
        return False
    last = series[-1]
    prev = series[-1 - lookback]
    if last is None or prev is None:
        return False
    return last > prev


def evaluate(bars: list[Candle], cfg: Settings) -> Decision:
    """Сигнал только по уже закрытым свечам из `bars`.

    Откат к EMA20: минимум бара лежит в полосе EMA20 ± ATR, закрытие вернулось
    по сторону тренда от EMA20.

    Растущая средняя: значение EMA на последней закрытой 5m выше, чем
    `rise_lookback_5m` закрытых 5m назад.

    Локальный экстремум: бар i ниже/выше обоих соседей, сосед справа уже закрыт
    (индекс <= последнего бара минус 1). Более высокий минимум — последний
    подтверждённый минимум строго выше предыдущего подтверждённого.
    """
    if len(bars) < cfg.min_1m_bars:
        return Decision(False, "мало минутных свечей")
    i = len(bars) - 1
    if i < 1:
        return Decision(False, "нет предыдущей свечи")

    slow = closed_5m(bars)
    if len(slow) < cfg.ema_slow + cfg.rise_lookback_5m:
        return Decision(False, "мало закрытых 5m")
    closes_5 = [bar.c for bar in slow]
    ema_fast_5 = ema(closes_5, cfg.ema_fast)
    ema_slow_5 = ema(closes_5, cfg.ema_slow)
    fast_last = ema_fast_5[-1]
    slow_last = ema_slow_5[-1]
    if fast_last is None or slow_last is None:
        return Decision(False, "EMA 5m ещё не готова")
    vwap = vwap_utc_day(bars, i)
    if vwap is None:
        return Decision(False, "нет объёма для VWAP")

    long_trend = (
        fast_last > slow_last
        and _rising(ema_fast_5, cfg.rise_lookback_5m)
        and _rising(ema_slow_5, cfg.rise_lookback_5m)
        and bars[i].c > vwap
    )
    short_trend = (
        fast_last < slow_last
        and not _rising(ema_fast_5, cfg.rise_lookback_5m)
        and ema_fast_5[-1] is not None
        and ema_fast_5[-1 - cfg.rise_lookback_5m] is not None
        and ema_fast_5[-1] < ema_fast_5[-1 - cfg.rise_lookback_5m]
        and ema_slow_5[-1] < ema_slow_5[-1 - cfg.rise_lookback_5m]
        and bars[i].c < vwap
    )
    if long_trend == short_trend:
        return Decision(False, "нет направления 5m или VWAP")
    side = Side.LONG if long_trend else Side.SHORT

    closes_1 = [bar.c for bar in bars]
    ema20 = ema(closes_1, cfg.ema_fast)
    atrs = atr(bars, cfg.atr_period)
    ema_now = ema20[i]
    atr_now = atrs[i]
    if ema_now is None or atr_now is None or atr_now <= 0 or bars[i].c <= 0:
        return Decision(False, "EMA20 или ATR не готовы")
    atr_pct = atr_now / bars[i].c
    if not (cfg.d("atr_min_pct") <= atr_pct <= cfg.d("atr_max_pct")):
        return Decision(False, "ATR вне допустимой волатильности")

    below = atr_now * cfg.d("pullback_atr_below")
    above = atr_now * cfg.d("pullback_atr_above")
    low = bars[i].l
    high = bars[i].h
    if side is Side.LONG:
        touched = ema_now - below <= low <= ema_now + above
        reclaimed = bars[i].c > ema_now
        broke = bars[i].c > bars[i - 1].h
        if not (touched and reclaimed and broke):
            return Decision(False, "нет отката long к EMA20")
    else:
        touched = ema_now - above <= high <= ema_now + below
        reclaimed = bars[i].c < ema_now
        broke = bars[i].c < bars[i - 1].l
        if not (touched and reclaimed and broke):
            return Decision(False, "нет отката short к EMA20")

    lows, highs = confirmed_swings(bars, i)
    if side is Side.LONG:
        recent = [n for n in lows if i - n <= cfg.swing_max_age]
        if len(recent) < 2:
            return Decision(False, "нет двух подтверждённых минимумов")
        if bars[recent[-1]].l <= bars[recent[-2]].l:
            return Decision(False, "последний минимум не выше предыдущего")
        opposite_idx = [n for n in highs if bars[n].h > bars[i].c and i - n <= cfg.swing_max_age * 3]
        if not opposite_idx:
            return Decision(False, "нет противоположного максимума выше цены")
        opposite = min(bars[n].h for n in opposite_idx)
        stop_ref = low
    else:
        recent_h = [n for n in highs if i - n <= cfg.swing_max_age]
        if len(recent_h) < 2:
            return Decision(False, "нет двух подтверждённых максимумов")
        if bars[recent_h[-1]].h >= bars[recent_h[-2]].h:
            return Decision(False, "последний максимум не ниже предыдущего")
        opposite_idx = [n for n in lows if bars[n].l < bars[i].c and i - n <= cfg.swing_max_age * 3]
        if not opposite_idx:
            return Decision(False, "нет противоположного минимума ниже цены")
        opposite = max(bars[n].l for n in opposite_idx)
        stop_ref = high

    return Decision(
        True,
        "откат по тренду",
        Signal(side=side, stop_ref=stop_ref, atr=atr_now, opposite=opposite, bar_ts=bars[i].ts),
    )
