from risk_pipeline.calculations import summarize_trades


def test_summarize_trades():
    """Verify trade count, signed MTM aggregation, and the large-trade count for a sample portfolio."""
    trades = [
        {"trade_id": "T001", "mtm": 100_000},
        {"trade_id": "T002", "mtm": -25_000},
        {"trade_id": "T003", "mtm": 70_000},
    ]

    result = summarize_trades(
        trades,
        threshold=50_000,
    )

    assert result["trade_count"] == 3
    assert result["total_mtm"] == 145_000
    assert result["large_trade_count"] == 2

def test_summarize_trades_empty():
    """Verify an empty portfolio produces zero totals and counts."""
    result = summarize_trades(
        [],
        threshold=50_000,
    )

    assert result["trade_count"] == 0
    assert result["total_mtm"] == 0
    assert result["large_trade_count"] == 0

def test_large_trade_threshold():
    """Check that positive and negative MTM values exactly at the threshold are counted."""
    trades = [
        {"trade_id": "T001", "mtm": 49_999},
        {"trade_id": "T002", "mtm": 50_000},
        {"trade_id": "T003", "mtm": -50_000},
    ]

    result = summarize_trades(
        trades,
        threshold=50_000,
    )

    assert result["large_trade_count"] == 2

import pytest

from risk_pipeline.calculations import summarize_trades


@pytest.mark.parametrize(
    "threshold, expected_count",
    [
        (20_000, 3),
        (50_000, 2),
        (80_000, 1),
        (120_000, 0),
    ],
)
def test_large_trade_count(threshold, expected_count):
    """Check large-trade counts across several configured thresholds."""
    trades = [
        {"trade_id": "T001", "mtm": 100_000},
        {"trade_id": "T002", "mtm": -25_000},
        {"trade_id": "T003", "mtm": 70_000},
    ]

    result = summarize_trades(trades, threshold)

    assert result["large_trade_count"] == expected_count