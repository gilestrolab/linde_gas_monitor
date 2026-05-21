"""Tests for order-history parsing: must accept both legacy 2-column
last_alert.log lines and the new 3-column (with PO) format, and compute
the per-bank median interval and per-order days-since-previous correctly.
"""
from datetime import datetime

import pytest


def test_history_parses_mixed_format(make_link):
    link = make_link(
        pos=[{'number': 'PO-A', 'ratio': 1}],
        log_lines=[
            '2025-01-01 10:00,left',           # legacy 2-col
            '2025-01-11 10:00,left,PO-A',      # new 3-col
            '2025-01-15 10:00,right,PO-A',
            '2025-01-25 10:00,right,PO-A',
        ],
    )
    orders, median = link.get_orders_history()

    assert len(orders) == 4
    # First left has no prior; second left is 10 days later
    assert orders[0] == (datetime(2025, 1, 1, 10, 0), 'left', None)
    assert orders[1][2] == pytest.approx(10.0)
    # First right has no prior; second right is 10 days later
    assert orders[2][2] is None
    assert orders[3][2] == pytest.approx(10.0)
    # Median per bank is the only interval available
    assert median['left'] == pytest.approx(10.0)
    assert median['right'] == pytest.approx(10.0)


def test_history_median_with_multiple_intervals(make_link):
    """Median should be the middle value (or average of two middles)."""
    link = make_link(
        pos=[],
        log_lines=[
            '2025-01-01 00:00,left',
            '2025-01-06 00:00,left',   # 5d
            '2025-01-16 00:00,left',   # 10d
            '2025-02-05 00:00,left',   # 20d
        ],
    )
    _, median = link.get_orders_history()
    # intervals: [5, 10, 20] -> median 10
    assert median['left'] == pytest.approx(10.0)
    assert median['right'] is None  # no right-bank orders


def test_history_empty_log(make_link):
    link = make_link(pos=[])
    orders, median = link.get_orders_history()
    assert orders == []
    assert median == {'left': None, 'right': None}


def test_history_skips_malformed_lines(make_link):
    """Lines that don't have a parseable timestamp should be skipped silently."""
    link = make_link(
        pos=[],
        log_lines=[
            'this is not a valid line',
            '2025-01-01 10:00,left,PO-A',
            ',,',                         # too few useful fields
            'garbage,left,PO-A',          # bad timestamp
            '2025-01-11 10:00,left,PO-A',
        ],
    )
    orders, _ = link.get_orders_history()
    assert len(orders) == 2
    assert all(o[1] == 'left' for o in orders)
