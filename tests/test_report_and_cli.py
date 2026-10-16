from decimal import Decimal

from bybit_scalper.__main__ import main
from bybit_scalper.logging_setup import RedactFilter, register_secret
from bybit_scalper.report import summarize
from bybit_scalper.types import ClosedTrade


def test_profit_factor_and_drawdown():
    trades = [
        ClosedTrade(1, "long", Decimal("1"), Decimal("10"), Decimal("12"), Decimal("2"), Decimal("0.1"), Decimal("0.01"), "target"),
        ClosedTrade(2, "long", Decimal("1"), Decimal("12"), Decimal("9"), Decimal("-3"), Decimal("0.2"), Decimal("0.02"), "stop"),
    ]
    report = summarize(trades, 4, 2, "проверка", True, ["заметка"])
    assert report.net_pnl == Decimal("-1")
    assert report.profit_factor == "0.6667"
    assert report.max_drawdown == Decimal("3")
    assert report.fill_rate == "0.5000"
    assert report.fees == Decimal("0.3")


def test_logs_redact_secrets():
    register_secret("super-secret-key")
    record = type("R", (), {})()
    record.msg = "key super-secret-key leaked"
    record.args = ()

    def get_message():
        return record.msg % record.args if record.args else record.msg

    record.getMessage = get_message
    assert RedactFilter().filter(record)
    assert "super-secret-key" not in record.msg
    assert "***" in record.msg


def test_live_and_testnet_do_not_start_without_explicit_secrets(cfg, monkeypatch):
    monkeypatch.delenv("BYBIT_API_KEY", raising=False)
    monkeypatch.delenv("BYBIT_API_SECRET", raising=False)
    monkeypatch.delenv("LIVE_CONFIRM", raising=False)
    assert main(["live", "--config", "config/default.yaml"]) == 2
    assert main(["testnet", "--config", "config/testnet.yaml"]) == 2


def test_unknown_config_key_is_rejected(tmp_path, cfg):
    from bybit_scalper.config import load_settings

    path = tmp_path / "bad.yaml"
    path.write_text("mode: paper\nnot_a_field: 1\n", encoding="utf-8")
    try:
        load_settings(path, mode="offline")
    except ValueError as exc:
        assert "not_a_field" in str(exc)
    else:
        raise AssertionError("unknown key must fail")
