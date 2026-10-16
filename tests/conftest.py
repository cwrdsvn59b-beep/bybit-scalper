from __future__ import annotations

from pathlib import Path

import pytest

from bybit_scalper.config import load_settings


@pytest.fixture
def cfg():
    return load_settings(Path(__file__).resolve().parents[1] / "config" / "default.yaml", mode="offline")
