from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from bybit_scalper.config import load_settings
from bybit_scalper.logging_setup import register_secret, setup_logging
from bybit_scalper.runner import format_saved_report, run_live, run_offline, run_paper, run_testnet


def load_dotenv(path: str = ".env") -> None:
    file = Path(path)
    if not file.exists():
        return
    for line in file.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    register_secret(os.environ.get("BYBIT_API_KEY"))
    register_secret(os.environ.get("BYBIT_API_SECRET"))
    setup_logging()
    parser = argparse.ArgumentParser(prog="bybit_scalper")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, default in (
        ("offline", "config/default.yaml"),
        ("paper", "config/default.yaml"),
        ("testnet", "config/testnet.yaml"),
        ("live", "config/default.yaml"),
        ("report", "config/default.yaml"),
    ):
        item = sub.add_parser(name)
        item.add_argument("--config", default=default)
    args = parser.parse_args(argv)
    if args.cmd == "live" and os.environ.get("LIVE_CONFIRM") != "YES":
        print("Реальная торговля не запускается автоматически. Нужна переменная LIVE_CONFIRM=YES.")
        return 2
    if args.cmd in {"testnet", "live"} and (
        not os.environ.get("BYBIT_API_KEY") or not os.environ.get("BYBIT_API_SECRET")
    ):
        print("Нужны BYBIT_API_KEY и BYBIT_API_SECRET. В файлы проекта их записывать нельзя.")
        return 2
    mode = None if args.cmd == "report" else args.cmd
    cfg = load_settings(args.config, mode=mode)
    if args.cmd == "offline":
        print(run_offline(cfg))
        return 0
    if args.cmd == "paper":
        run_paper(cfg)
        return 0
    if args.cmd == "testnet":
        run_testnet(cfg)
        return 0
    if args.cmd == "live":
        run_live(cfg)
        return 0
    print(format_saved_report(cfg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
