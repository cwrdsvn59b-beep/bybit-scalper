from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from bybit_scalper.decimals import D
from bybit_scalper.types import Fees, Instrument


@dataclass
class Settings:
    mode: str
    symbol: str
    category: str
    eth_enabled: bool
    state_db: str
    testnet: bool
    demo: bool
    queue_max: int
    stale_book_ms: int
    stale_candle_ms: int
    max_clock_skew_ms: int
    max_requests_per_sec: int
    entry_timeout_sec: int
    max_hold_sec: int
    risk_pct: object
    daily_loss_pct: object
    max_consecutive_losses: int
    pause_after_losses_sec: int
    max_leverage: object
    ema_fast: int
    ema_slow: int
    rise_lookback_5m: int
    atr_period: int
    atr_min_pct: object
    atr_max_pct: object
    pullback_atr_below: object
    pullback_atr_above: object
    stop_atr_buffer: object
    reward_multiple: object
    swing_max_age: int
    min_1m_bars: int
    max_spread_bps: object
    min_depth_qty: object
    depth_bps: object
    slippage_bps: object
    entry_fee_assumption: str
    maker_fee: object
    taker_fee: object
    trade_flow_enabled: bool
    offline_equity: object
    tick_size: object
    qty_step: object
    min_qty: object
    min_notional: object
    train_ratio: object

    def d(self, name: str):
        return D(getattr(self, name))

    def instrument(self) -> Instrument:
        return Instrument(
            symbol=self.symbol,
            tick=self.d("tick_size"),
            qty_step=self.d("qty_step"),
            min_qty=self.d("min_qty"),
            min_notional=self.d("min_notional"),
        )

    def fallback_fees(self) -> Fees:
        return Fees(maker=self.d("maker_fee"), taker=self.d("taker_fee"), source="config-offline")

    def symbol_allowed(self) -> bool:
        if self.symbol == "BTCUSDT":
            return True
        if self.symbol == "ETHUSDT" and self.eth_enabled:
            return True
        return False


def load_settings(path: str | Path, mode: str | None = None) -> Settings:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    raw.setdefault("testnet", False)
    raw.setdefault("demo", False)
    if mode:
        raw["mode"] = mode
    env_mode = os.environ.get("MODE")
    if mode is None and env_mode:
        raw["mode"] = env_mode
    allowed = {f.name for f in Settings.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"Неизвестные поля конфигурации: {sorted(unknown)}")
    return Settings(**raw)
