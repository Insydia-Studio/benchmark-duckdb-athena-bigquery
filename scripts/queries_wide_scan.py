#!/usr/bin/env python3
"""
Additional benchmark queries optimized for wide data scans.

These queries represent realistic analytical use cases that require
scanning multiple columns or full tables:
- Data exports for downstream processing
- Feature engineering for ML pipelines
- Correlation analysis across price fields
- Portfolio analytics requiring all OHLCV data

15 queries (Q43-Q57) to supplement the original 42.
"""

from dataclasses import dataclass


@dataclass
class BenchmarkQuery:
    """A benchmark query with metadata."""
    id: str
    name: str
    description: str
    sql_template: str
    category: str


WIDE_SCAN_QUERIES = [
    # ===== FULL TABLE EXPORTS (3 queries) =====
    BenchmarkQuery(
        id="Q43",
        name="Full OHLCV Export",
        description="Export all price columns for recent data - typical data pipeline use case",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                open,
                high,
                low,
                close,
                volume,
                adjClose,
                changePercent
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
            ORDER BY symbol, dateEpoch
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q44",
        name="S&P 500 Full History Export",
        description="Complete OHLCV data for S&P 500 constituents - backtesting data prep",
        category="wide_scan",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.open,
                e.high,
                e.low,
                e.close,
                e.volume,
                e.adjClose,
                e.changePercent
            FROM {table_prefix}stock_eod e
            WHERE e.symbol IN (SELECT symbol FROM {table_prefix}sp500_constituent)
            ORDER BY e.symbol, e.dateEpoch
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q45",
        name="Full Table Sample",
        description="Random sample of complete records for data quality review",
        category="wide_scan",
        sql_template="""
            SELECT *
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            ORDER BY volume DESC
            LIMIT 50000
        """
    ),

    # ===== MULTI-COLUMN ANALYTICS (4 queries) =====
    BenchmarkQuery(
        id="Q46",
        name="Daily Range Analysis",
        description="Calculate daily trading range metrics using high/low/open/close",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                open,
                high,
                low,
                close,
                volume,
                (high - low) as daily_range,
                (high - low) / NULLIF(open, 0) * 100 as range_pct,
                (close - open) / NULLIF(open, 0) * 100 as body_pct,
                CASE
                    WHEN close > open THEN 'bullish'
                    WHEN close < open THEN 'bearish'
                    ELSE 'doji'
                END as candle_type
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND volume > 1000000
            ORDER BY range_pct DESC
            LIMIT 10000
        """
    ),
    BenchmarkQuery(
        id="Q47",
        name="OHLCV Correlation Dataset",
        description="Prepare dataset for price-volume correlation analysis",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                open,
                high,
                low,
                close,
                adjClose,
                volume,
                (close - open) as price_change,
                (high - low) as intraday_range,
                CASE WHEN close > open THEN volume ELSE 0 END as up_volume,
                CASE WHEN close < open THEN volume ELSE 0 END as down_volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            ORDER BY symbol, dateEpoch
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q48",
        name="True Range Calculation",
        description="Calculate true range (requires high, low, close, prev close)",
        category="wide_scan",
        sql_template="""
            WITH price_data AS (
                SELECT
                    symbol,
                    dateEpoch,
                    high,
                    low,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
            )
            SELECT
                symbol,
                dateEpoch,
                high,
                low,
                close,
                prev_close,
                GREATEST(
                    high - low,
                    ABS(high - COALESCE(prev_close, close)),
                    ABS(low - COALESCE(prev_close, close))
                ) as true_range
            FROM price_data
            WHERE prev_close IS NOT NULL
            ORDER BY true_range DESC
            LIMIT 50000
        """
    ),
    BenchmarkQuery(
        id="Q49",
        name="Candlestick Pattern Features",
        description="Extract candlestick features for pattern recognition ML",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                open,
                high,
                low,
                close,
                volume,
                -- Body metrics
                ABS(close - open) as body_size,
                (close - open) / NULLIF(open, 0) as body_pct,
                -- Wick metrics
                high - GREATEST(open, close) as upper_wick,
                LEAST(open, close) - low as lower_wick,
                -- Normalized features
                (high - low) as total_range,
                ABS(close - open) / NULLIF(high - low, 0) as body_to_range_ratio
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND high > low  -- valid candle
            ORDER BY symbol, dateEpoch
            LIMIT 100000
        """
    ),

    # ===== WIDE AGGREGATIONS (4 queries) =====
    BenchmarkQuery(
        id="Q50",
        name="Complete Symbol Statistics",
        description="Full statistical profile per symbol using all price columns",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as trading_days,
                -- Open stats
                AVG(open) as avg_open,
                STDDEV(open) as stddev_open,
                -- High stats
                MAX(high) as all_time_high,
                AVG(high) as avg_high,
                -- Low stats
                MIN(low) as all_time_low,
                AVG(low) as avg_low,
                -- Close stats
                AVG(close) as avg_close,
                STDDEV(close) as stddev_close,
                -- Volume stats
                AVG(volume) as avg_volume,
                MAX(volume) as max_volume,
                SUM(volume) as total_volume,
                -- Derived
                AVG(high - low) as avg_range,
                AVG(ABS(close - open)) as avg_body
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol
            HAVING COUNT(*) > 100
            ORDER BY total_volume DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q51",
        name="Monthly OHLCV Rollup",
        description="Monthly aggregation of all price and volume data",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                DATE_TRUNC('month', to_timestamp(dateEpoch)) as month,
                -- OHLC (first open, max high, min low, last close)
                MIN(open) as month_open,
                MAX(high) as month_high,
                MIN(low) as month_low,
                MAX(close) as month_close,
                -- Volume
                SUM(volume) as total_volume,
                AVG(volume) as avg_daily_volume,
                -- Stats
                COUNT(*) as trading_days,
                STDDEV(close) as price_volatility,
                AVG(high - low) as avg_daily_range
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol, month
            ORDER BY symbol, month
            LIMIT 50000
        """
    ),
    BenchmarkQuery(
        id="Q52",
        name="Sector-Wide OHLCV Analysis",
        description="Aggregate all price data by sector and date",
        category="wide_scan",
        sql_template="""
            SELECT
                p.sector,
                DATE_TRUNC('week', to_timestamp(e.dateEpoch)) as week,
                COUNT(DISTINCT e.symbol) as num_stocks,
                AVG(e.open) as avg_open,
                AVG(e.high) as avg_high,
                AVG(e.low) as avg_low,
                AVG(e.close) as avg_close,
                SUM(e.volume) as total_volume,
                AVG(e.high - e.low) as avg_range,
                AVG((e.close - e.open) / NULLIF(e.open, 0) * 100) as avg_daily_return
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1672531200
            GROUP BY p.sector, week
            ORDER BY week DESC, total_volume DESC
            LIMIT 10000
        """
    ),
    BenchmarkQuery(
        id="Q53",
        name="Price Level Distribution",
        description="Distribution of prices across all OHLC columns",
        category="wide_scan",
        sql_template="""
            SELECT
                FLOOR(close / 10) * 10 as price_bucket,
                COUNT(*) as record_count,
                AVG(open) as avg_open,
                AVG(high) as avg_high,
                AVG(low) as avg_low,
                AVG(close) as avg_close,
                AVG(volume) as avg_volume,
                AVG(high - low) as avg_range,
                COUNT(DISTINCT symbol) as unique_symbols
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND close > 0 AND close < 1000
            GROUP BY price_bucket
            ORDER BY price_bucket
        """
    ),

    # ===== CROSS-COLUMN JOINS (4 queries) =====
    BenchmarkQuery(
        id="Q54",
        name="Enriched OHLCV with Profile",
        description="Full price data enriched with company fundamentals",
        category="wide_scan",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.open,
                e.high,
                e.low,
                e.close,
                e.volume,
                e.adjClose,
                p.sector,
                p.industry,
                p.marketCap,
                p.beta,
                p.volAvg,
                e.volume / NULLIF(p.volAvg, 0) as relative_volume
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1704067200
            ORDER BY e.dateEpoch DESC, e.volume DESC
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q55",
        name="Large Cap Full Data Extract",
        description="Complete OHLCV for large cap stocks with all metadata",
        category="wide_scan",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.open,
                e.high,
                e.low,
                e.close,
                e.volume,
                e.adjClose,
                e.changePercent,
                p.companyName,
                p.sector,
                p.industry,
                p.marketCap,
                p.beta
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE p.marketCap > 10000000000  -- > $10B
              AND e.dateEpoch >= 1672531200
            ORDER BY p.marketCap DESC, e.dateEpoch DESC
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q56",
        name="Index Constituent Full Scan",
        description="All OHLCV data for S&P 500 with constituent metadata",
        category="wide_scan",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.open,
                e.high,
                e.low,
                e.close,
                e.volume,
                e.adjClose,
                s.sector as sp_sector,
                s.subSector,
                s.dateFirstAdded
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}sp500_constituent s ON e.symbol = s.symbol
            WHERE e.dateEpoch >= 1672531200
            ORDER BY e.symbol, e.dateEpoch
            LIMIT 100000
        """
    ),
    BenchmarkQuery(
        id="Q57",
        name="Multi-Table Wide Join",
        description="Comprehensive stock data combining EOD, profile, and index membership",
        category="wide_scan",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.open,
                e.high,
                e.low,
                e.close,
                e.volume,
                e.adjClose,
                p.sector,
                p.industry,
                p.marketCap,
                p.beta,
                p.volAvg,
                CASE WHEN s.symbol IS NOT NULL THEN 1 ELSE 0 END as is_sp500,
                s.subSector as sp500_subsector
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            LEFT JOIN {table_prefix}sp500_constituent s ON e.symbol = s.symbol
            WHERE e.dateEpoch >= 1704067200
              AND e.volume > 100000
            ORDER BY e.dateEpoch DESC, e.volume DESC
            LIMIT 100000
        """
    ),
]


# Platform-specific overrides for compatibility
BIGQUERY_OVERRIDES = {
    "Q51": BenchmarkQuery(
        id="Q51",
        name="Monthly OHLCV Rollup",
        description="Monthly aggregation of all price and volume data (BigQuery)",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                DATE_TRUNC(TIMESTAMP_SECONDS(dateEpoch), MONTH) as month,
                MIN(open) as month_open,
                MAX(high) as month_high,
                MIN(low) as month_low,
                MAX(close) as month_close,
                SUM(volume) as total_volume,
                AVG(volume) as avg_daily_volume,
                COUNT(*) as trading_days,
                STDDEV(close) as price_volatility,
                AVG(high - low) as avg_daily_range
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol, month
            ORDER BY symbol, month
            LIMIT 50000
        """
    ),
    "Q52": BenchmarkQuery(
        id="Q52",
        name="Sector-Wide OHLCV Analysis",
        description="Aggregate all price data by sector and date (BigQuery)",
        category="wide_scan",
        sql_template="""
            SELECT
                p.sector,
                DATE_TRUNC(TIMESTAMP_SECONDS(e.dateEpoch), WEEK) as week,
                COUNT(DISTINCT e.symbol) as num_stocks,
                AVG(e.open) as avg_open,
                AVG(e.high) as avg_high,
                AVG(e.low) as avg_low,
                AVG(e.close) as avg_close,
                SUM(e.volume) as total_volume,
                AVG(e.high - e.low) as avg_range,
                AVG((e.close - e.open) / NULLIF(e.open, 0) * 100) as avg_daily_return
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1672531200
            GROUP BY p.sector, week
            ORDER BY week DESC, total_volume DESC
            LIMIT 10000
        """
    ),
}

ATHENA_OVERRIDES = {
    "Q51": BenchmarkQuery(
        id="Q51",
        name="Monthly OHLCV Rollup",
        description="Monthly aggregation of all price and volume data (Athena)",
        category="wide_scan",
        sql_template="""
            SELECT
                symbol,
                DATE_TRUNC('month', from_unixtime(dateEpoch)) as month,
                MIN(open) as month_open,
                MAX(high) as month_high,
                MIN(low) as month_low,
                MAX(close) as month_close,
                SUM(volume) as total_volume,
                AVG(volume) as avg_daily_volume,
                COUNT(*) as trading_days,
                STDDEV(close) as price_volatility,
                AVG(high - low) as avg_daily_range
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol, DATE_TRUNC('month', from_unixtime(dateEpoch))
            ORDER BY symbol, month
            LIMIT 50000
        """
    ),
    "Q52": BenchmarkQuery(
        id="Q52",
        name="Sector-Wide OHLCV Analysis",
        description="Aggregate all price data by sector and date (Athena)",
        category="wide_scan",
        sql_template="""
            SELECT
                p.sector,
                DATE_TRUNC('week', from_unixtime(e.dateEpoch)) as week,
                COUNT(DISTINCT e.symbol) as num_stocks,
                AVG(e.open) as avg_open,
                AVG(e.high) as avg_high,
                AVG(e.low) as avg_low,
                AVG(e.close) as avg_close,
                SUM(e.volume) as total_volume,
                AVG(e.high - e.low) as avg_range,
                AVG((e.close - e.open) / NULLIF(e.open, 0) * 100) as avg_daily_return
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1672531200
            GROUP BY p.sector, DATE_TRUNC('week', from_unixtime(e.dateEpoch))
            ORDER BY week DESC, total_volume DESC
            LIMIT 10000
        """
    ),
}


def get_queries(platform: str = "duckdb") -> list[BenchmarkQuery]:
    """Get queries with platform-specific overrides applied."""
    queries = WIDE_SCAN_QUERIES.copy()

    if platform == "bigquery":
        overrides = BIGQUERY_OVERRIDES
    elif platform == "athena":
        overrides = ATHENA_OVERRIDES
    else:
        return queries

    # Apply overrides
    for i, q in enumerate(queries):
        if q.id in overrides:
            queries[i] = overrides[q.id]

    return queries


if __name__ == "__main__":
    print("Wide Scan Queries (Q43-Q57)")
    print("=" * 60)
    for q in WIDE_SCAN_QUERIES:
        print(f"\n{q.id}: {q.name}")
        print(f"Category: {q.category}")
        print(f"Description: {q.description}")
