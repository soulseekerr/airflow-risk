

def summarize_trades(trades, threshold):
    """Return trade count, signed total MTM, and count above the absolute-MTM threshold."""
    total_mtm = sum(trade["mtm"] for trade in trades)

    large_trade_count = sum(
        1
        for trade in trades
        if abs(trade["mtm"]) >= threshold
    )

    return {
        "trade_count": len(trades),
        "total_mtm": total_mtm,
        "large_trade_count": large_trade_count,
    }
