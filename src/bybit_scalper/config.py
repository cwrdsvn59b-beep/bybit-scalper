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
    allocated_capital: object
    adverse_reserve_bps: object
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
    cfg = Settings(**raw)
    if cfg.category != "linear" or cfg.mode not in {"offline", "paper", "testnet", "live"}:
        raise ValueError("Поддерживаются только linear и известные режимы запуска")
    for name in ["risk_pct", "daily_loss_pct"]:
        if not D('0') < cfg.d(name) <= D('0.01'):
            raise ValueError(f"{name}: допустимо больше нуля и не выше 1%")
    if not D('0') < cfg.d('max_leverage') <= D('2'):
        raise ValueError("max_leverage: допустимо больше нуля и не выше 2")
    for name in ["entry_timeout_sec", "max_hold_sec", "queue_max", "max_requests_per_sec",
                 "ema_fast", "ema_slow", "atr_period", "min_1m_bars", "max_consecutive_losses"]:
        if not isinstance(getattr(cfg, name), int) or getattr(cfg, name) <= 0:
            raise ValueError(f"{name}: требуется положительное целое число")
    for name in ["tick_size", "qty_step", "min_qty", "offline_equity", "reward_multiple"]:
        if not cfg.d(name).is_finite() or cfg.d(name) <= 0:
            raise ValueError(f"{name}: требуется конечное положительное число")
    if not D('0') < cfg.d('train_ratio') < D('1'):
        raise ValueError("train_ratio должен лежать между 0 и 1")
    if not cfg.d("allocated_capital").is_finite() or cfg.d("allocated_capital") <= 0:
        raise ValueError("allocated_capital: требуется конечное положительное число")
    if not cfg.d("adverse_reserve_bps").is_finite() or cfg.d("adverse_reserve_bps") < 0:
        raise ValueError("adverse_reserve_bps: требуется конечное неотрицательное число")
    if cfg.mode in {"offline", "paper"} and cfg.d("allocated_capital") > cfg.d("offline_equity"):
        raise ValueError("В offline и paper выделенный капитал не может быть больше offline_equity")
    if cfg.mode == "live" and _uses_paper_database(cfg.state_db):
        raise ValueError("Режим live не может использовать paper-базу data/state.db")
    return cfg


def _uses_paper_database(path: str) -> bool:
    norm = os.path.normpath(path).replace("\\", "/")
    return norm == "data/state.db" or norm.endswith("/data/state.db")
