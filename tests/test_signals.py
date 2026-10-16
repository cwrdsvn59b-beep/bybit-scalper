from decimal import Decimal

from bybit_scalper.indicators import atr, confirmed_swings, ema, vwap_utc_day
from bybit_scalper.signals import evaluate
from bybit_scalper.synthetic import build_long_fixture, candles_from_csv, candles_to_csv
from bybit_scalper.types import Candle


def test_fixture_long_uses_only_closed_bars(cfg):
    bars = build_long_fixture(cfg)
    decision = evaluate(bars, cfg)
    assert decision.accepted
    assert decision.signal is not None
    assert decision.signal.bar_ts == bars[-1].ts


def test_future_bar_does_not_change_prefix(cfg):
    bars = build_long_fixture(cfg)
    prefix = bars[:-1]
    first = evaluate(prefix, cfg)
    mutated = prefix + [Candle(bars[-1].ts, Decimal("1"), Decimal("2"), Decimal("1"), Decimal("1"), Decimal("9"))]
    second = evaluate(mutated[:-1], cfg)
    assert first.accepted == second.accepted
    assert first.reason == second.reason


def test_indicator_prefix_is_stable(cfg):
    bars = build_long_fixture(cfg)
    closes = [bar.c for bar in bars]
    full_ema = ema(closes, 20)
    full_atr = atr(bars, 14)
    for index in (40, 120, 250, 400):
        assert full_ema[index] == ema(closes[: index + 1], 20)[index]
        assert full_atr[index] == atr(bars[: index + 1], 14)[index]
        assert vwap_utc_day(bars, index) == vwap_utc_day(bars[: index + 1], index)


def test_unconfirmed_bar_is_not_a_swing(cfg):
    bars = build_long_fixture(cfg)
    last = len(bars) - 1
    lows, highs = confirmed_swings(bars, last)
    assert last not in lows and last not in highs
    earlier_lows, earlier_highs = confirmed_swings(bars, last - 1)
    assert last - 1 not in earlier_lows and last - 1 not in earlier_highs


def test_csv_roundtrip(cfg):
    bars = build_long_fixture(cfg)
    restored = candles_from_csv(candles_to_csv(bars))
    assert len(restored) == len(bars)
    assert restored[-1].c == bars[-1].c
    assert evaluate(restored, cfg).accepted
