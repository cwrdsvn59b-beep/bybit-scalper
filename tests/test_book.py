from decimal import Decimal

from bybit_scalper.book import OrderBook
from bybit_scalper.decimals import D


def _level(price, size):
    return [str(price), str(size)]


def test_snapshot_delta_gap_and_delete():
    book = OrderBook()
    book.apply(
        {
            "type": "snapshot",
            "ts": 10,
            "data": {"u": 1, "b": [_level(100, 2), _level(99, 1)], "a": [_level(101, 3), _level(102, 1)]},
        }
    )
    assert book.valid
    assert book.top()[:2] == (D(100), D(101))
    book.apply({"type": "delta", "ts": 11, "data": {"u": 2, "b": [_level(100, 0)], "a": []}})
    assert book.valid
    assert book.top()[0] == D(99)
    book.apply({"type": "delta", "ts": 12, "data": {"u": 4, "b": [], "a": []}})
    assert book.valid  # Bybit does not guarantee u increases by one.
    book.reset()  # Actual transport loss invalidates the book.
    assert not book.valid and book.reset_needed


def test_crossed_book_is_invalid():
    book = OrderBook()
    book.apply(
        {
            "type": "snapshot",
            "ts": 1,
            "data": {"u": 1, "b": [_level(105, 1)], "a": [_level(104, 1)]},
        }
    )
    assert not book.valid


def test_depth_within_bps():
    book = OrderBook()
    book.apply(
        {
            "type": "snapshot",
            "ts": 1,
            "data": {
                "u": 1,
                "b": [_level(100, 1), _level(90, 5)],
                "a": [_level(101, 2), _level(130, 7)],
            },
        }
    )
    bid_qty, ask_qty = book.depth_qty(Decimal("50"))
    assert bid_qty == D(1)
    assert ask_qty == D(2)
