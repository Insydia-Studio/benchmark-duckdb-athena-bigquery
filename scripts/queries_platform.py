#!/usr/bin/env python3
"""
Platform-specific query adaptations.

DuckDB supports QUALIFY, PERCENTILE_CONT WITHIN GROUP, etc.
BigQuery/Athena need standard SQL alternatives.
"""

from queries import QUERIES as BASE_QUERIES, BenchmarkQuery

# Platform-specific overrides for problematic queries
DUCKDB_OVERRIDES = {
    "Q18": BenchmarkQuery(
        id="Q18",
        name="Rank by Volume",
        description="Daily volume rank within each sector (DuckDB optimized)",
        category="window",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.volume,
                p.sector,
                RANK() OVER (PARTITION BY e.dateEpoch, p.sector ORDER BY e.volume DESC) as volume_rank
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1734307200
            QUALIFY volume_rank <= 10
            ORDER BY e.dateEpoch DESC, p.sector, volume_rank
            LIMIT 1000
        """
    ),
    "Q20": BenchmarkQuery(
        id="Q20",
        name="Gap Detection",
        description="Detect price gaps (DuckDB optimized)",
        category="window",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                open,
                close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                ((open - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                    / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) * 100) as gap_pct
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
            QUALIFY ABS(gap_pct) > 5
            ORDER BY ABS(gap_pct) DESC
            LIMIT 500
        """
    ),
    "Q28": BenchmarkQuery(
        id="Q28",
        name="Median Return by Symbol",
        description="Calculate median daily return (DuckDB optimized)",
        category="analytics",
        sql_template="""
            WITH daily_returns AS (
                SELECT
                    symbol,
                    dateEpoch,
                    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            )
            SELECT
                symbol,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_return) as median_return,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY daily_return) as q1_return,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY daily_return) as q3_return,
                STDDEV(daily_return) as volatility,
                COUNT(*) as days
            FROM daily_returns
            WHERE daily_return IS NOT NULL
            GROUP BY symbol
            HAVING COUNT(*) > 200
            ORDER BY volatility DESC
            LIMIT 100
        """
    ),
}

BIGQUERY_OVERRIDES = {
    # Q04: Division by zero protection - add NULLIF(open, 0)
    "Q04": BenchmarkQuery(
        id="Q04",
        name="Price Movement Filter",
        description="Stocks with >5% daily move (BigQuery)",
        category="scan",
        sql_template="""
            SELECT symbol, dateEpoch, open, close,
                   ((close - open) / NULLIF(open, 0) * 100) as pct_change
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1734307200
              AND ABS((close - open) / NULLIF(open, 0)) > 0.05
            ORDER BY ABS((close - open) / NULLIF(open, 0)) DESC
            LIMIT 1000
        """
    ),

    # Q10: BigQuery uses TIMESTAMP_SECONDS() instead of to_timestamp()
    "Q10": BenchmarkQuery(
        id="Q10",
        name="Monthly Volume Aggregation",
        description="Total volume by month for top symbols (BigQuery)",
        category="aggregation",
        sql_template="""
            SELECT
                symbol,
                DATE_TRUNC(TIMESTAMP_SECONDS(dateEpoch), MONTH) as month,
                SUM(volume) as total_volume,
                AVG(close) as avg_close
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
            GROUP BY symbol, month
            ORDER BY symbol, month
        """
    ),

    # Q22: 3-way join - BigQuery column names differ from DuckDB
    # roeTTM doesn't exist, use netProfitMargin instead
    "Q22": BenchmarkQuery(
        id="Q22",
        name="3-Way Join - EOD + Profile + Ratios",
        description="Combine price, company info, and financial ratios (BigQuery)",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                p.sector,
                p.marketCap,
                r.netProfitMargin,
                r.assetTurnover,
                r.currentRatio
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            JOIN {table_prefix}financial_ratios r ON e.symbol = r.symbol
            WHERE e.dateEpoch >= 1734307200
              AND r.netProfitMargin > 0.15
            ORDER BY r.netProfitMargin DESC
            LIMIT 1000
        """
    ),

    # Q23: Window function in aggregate - BigQuery needs different approach
    "Q23": BenchmarkQuery(
        id="Q23",
        name="Join with Aggregation",
        description="Average returns by sector (BigQuery)",
        category="join",
        sql_template="""
            WITH price_changes AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            returns AS (
                SELECT
                    symbol,
                    AVG(SAFE_DIVIDE(close - prev_close, prev_close)) as avg_return
                FROM price_changes
                WHERE prev_close IS NOT NULL AND prev_close > 0
                GROUP BY symbol
            )
            SELECT
                p.sector,
                COUNT(*) as num_stocks,
                AVG(r.avg_return) as sector_avg_return,
                STDDEV(r.avg_return) as sector_return_stddev
            FROM returns r
            JOIN {table_prefix}profile p ON r.symbol = p.symbol
            GROUP BY p.sector
            ORDER BY sector_avg_return DESC
        """
    ),

    # Q24: analyst_estimates join - BigQuery uses different column names
    # estimatedEpsAvg -> epsAvg, numberAnalystEstimatedEps -> numAnalystsEps
    "Q24": BenchmarkQuery(
        id="Q24",
        name="Analyst Coverage Join",
        description="Stocks with analyst estimates and recent prices (BigQuery)",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                a.epsAvg,
                a.numAnalystsEps,
                p.companyName,
                p.marketCap
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}analyst_estimates a ON e.symbol = a.symbol
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1734307200
              AND a.numAnalystsEps >= 5
            ORDER BY a.numAnalystsEps DESC
            LIMIT 1000
        """
    ),

    # Q25: stock_peers join - BigQuery table has different structure (no peersList)
    # Use available columns: symbol, companyName, price, mktCap
    "Q25": BenchmarkQuery(
        id="Q25",
        name="Peer Comparison",
        description="Compare stocks to their peers (BigQuery)",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                p.sector,
                peers.companyName as peer_company,
                p.marketCap
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            LEFT JOIN {table_prefix}stock_peers peers ON e.symbol = peers.symbol
            WHERE e.dateEpoch >= 1734307200
              AND e.symbol IN (SELECT symbol FROM {table_prefix}sp500_constituent LIMIT 50)
            ORDER BY p.marketCap DESC
            LIMIT 1000
        """
    ),

    # Q26: sp500_constituent join - use dateFirstAdded instead of dateAdded
    "Q26": BenchmarkQuery(
        id="Q26",
        name="Index Constituent Historical Join",
        description="Track when stocks joined S&P 500 (BigQuery)",
        category="join",
        sql_template="""
            SELECT
                sp.symbol,
                sp.dateFirstAdded as dateAdded,
                p.companyName,
                p.sector,
                p.marketCap
            FROM {table_prefix}sp500_constituent sp
            JOIN {table_prefix}profile p ON sp.symbol = p.symbol
            ORDER BY sp.dateFirstAdded DESC
            LIMIT 500
        """
    ),

    # Q28: BigQuery APPROX_QUANTILE syntax is different
    "Q28": BenchmarkQuery(
        id="Q28",
        name="Median Return by Symbol",
        description="Calculate median daily return (BigQuery)",
        category="analytics",
        sql_template="""
            WITH daily_returns AS (
                SELECT
                    symbol,
                    dateEpoch,
                    SAFE_DIVIDE(close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch),
                               LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch)) as daily_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            filtered AS (
                SELECT symbol, daily_return
                FROM daily_returns
                WHERE daily_return IS NOT NULL
            )
            SELECT
                symbol,
                APPROX_QUANTILES(daily_return, 100)[OFFSET(50)] as median_return,
                APPROX_QUANTILES(daily_return, 100)[OFFSET(25)] as q1_return,
                APPROX_QUANTILES(daily_return, 100)[OFFSET(75)] as q3_return,
                STDDEV(daily_return) as volatility,
                COUNT(*) as days
            FROM filtered
            GROUP BY symbol
            HAVING COUNT(*) > 200
            ORDER BY volatility DESC
            LIMIT 100
        """
    ),

    # Q31: Value screening - use BigQuery column names
    # peRatio -> priceToEarningsRatio, evToEbitda -> enterpriseValueMultiple
    # roeTTM -> netProfitMargin, debtToEquity -> debtToEquityRatio
    "Q31": BenchmarkQuery(
        id="Q31",
        name="Value Screening",
        description="Find undervalued stocks by multiple metrics (BigQuery)",
        category="analytics",
        sql_template="""
            SELECT
                p.symbol,
                p.companyName,
                p.sector,
                p.marketCap,
                r.priceToEarningsRatio,
                r.priceToBookRatio,
                r.enterpriseValueMultiple,
                r.netProfitMargin,
                r.debtToEquityRatio,
                (SAFE_DIVIDE(1, r.priceToEarningsRatio) +
                 SAFE_DIVIDE(1, r.priceToBookRatio) +
                 r.netProfitMargin) as value_score
            FROM {table_prefix}profile p
            JOIN {table_prefix}financial_ratios r ON p.symbol = r.symbol
            WHERE p.marketCap > 1000000000
              AND r.priceToEarningsRatio > 0 AND r.priceToEarningsRatio < 20
              AND r.priceToBookRatio > 0 AND r.priceToBookRatio < 3
              AND r.netProfitMargin > 0.15
              AND r.debtToEquityRatio < 1.5
            ORDER BY value_score DESC
            LIMIT 100
        """
    ),

    # Q33: Quality Score - use BigQuery column names
    # roeTTM -> netProfitMargin, roa -> (netProfitMargin * assetTurnover)
    # debtToEquity -> debtToEquityRatio
    "Q33": BenchmarkQuery(
        id="Q33",
        name="Quality Score",
        description="Composite quality score from financial metrics (BigQuery)",
        category="analytics",
        sql_template="""
            SELECT
                p.symbol,
                p.companyName,
                p.sector,
                r.netProfitMargin,
                (r.netProfitMargin * r.assetTurnover) as roa_calc,
                r.currentRatio,
                r.debtToEquityRatio,
                r.grossProfitMargin,
                r.operatingProfitMargin,
                (r.netProfitMargin * 0.3 +
                 (r.netProfitMargin * r.assetTurnover) * 0.2 +
                 LEAST(SAFE_DIVIDE(r.currentRatio, 2), 1.0) * 0.2 +
                 SAFE_DIVIDE(1, (1 + r.debtToEquityRatio)) * 0.1 +
                 r.grossProfitMargin * 0.1 +
                 r.operatingProfitMargin * 0.1) as quality_score
            FROM {table_prefix}profile p
            JOIN {table_prefix}financial_ratios r ON p.symbol = r.symbol
            WHERE p.marketCap > 1000000000
              AND r.netProfitMargin > 0
              AND r.assetTurnover > 0
            ORDER BY quality_score DESC
            LIMIT 100
        """
    ),

    # Q34: Sector Relative Strength - use SAFE_DIVIDE
    "Q34": BenchmarkQuery(
        id="Q34",
        name="Sector Relative Strength",
        description="Compare stock performance vs sector average (BigQuery)",
        category="analytics",
        sql_template="""
            WITH price_changes AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            stock_returns AS (
                SELECT
                    symbol,
                    AVG(SAFE_DIVIDE(close - prev_close, prev_close)) as avg_return
                FROM price_changes
                WHERE prev_close IS NOT NULL AND prev_close > 0
                GROUP BY symbol
            ),
            sector_returns AS (
                SELECT
                    p.sector,
                    AVG(sr.avg_return) as sector_avg_return
                FROM stock_returns sr
                JOIN {table_prefix}profile p ON sr.symbol = p.symbol
                GROUP BY p.sector
            )
            SELECT
                sr.symbol,
                p.sector,
                sr.avg_return as stock_return,
                secr.sector_avg_return,
                (sr.avg_return - secr.sector_avg_return) as relative_strength
            FROM stock_returns sr
            JOIN {table_prefix}profile p ON sr.symbol = p.symbol
            JOIN sector_returns secr ON p.sector = secr.sector
            ORDER BY relative_strength DESC
            LIMIT 100
        """
    ),

    # Q35: Golden Cross - nested window functions need subquery
    "Q35": BenchmarkQuery(
        id="Q35",
        name="Golden Cross Detection",
        description="Detect 50-day SMA crossing above 200-day SMA (BigQuery)",
        category="timeseries",
        sql_template="""
            WITH sma_calc AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1640995200
            ),
            sma_data AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    sma_50,
                    sma_200,
                    LAG(sma_50) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_sma_50
                FROM sma_calc
            )
            SELECT
                symbol,
                dateEpoch,
                close,
                sma_50,
                sma_200
            FROM sma_data
            WHERE prev_sma_50 < sma_200 AND sma_50 > sma_200
              AND dateEpoch >= 1704067200
            ORDER BY dateEpoch DESC
            LIMIT 100
        """
    ),

    # Q39: Data Completeness - use TIMESTAMP_SECONDS
    "Q39": BenchmarkQuery(
        id="Q39",
        name="Data Completeness Check",
        description="Count records per table and date range (BigQuery)",
        category="quality",
        sql_template="""
            SELECT
                'stock_eod' as table_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT dateEpoch) as unique_dates,
                MIN(TIMESTAMP_SECONDS(dateEpoch)) as earliest_date,
                MAX(TIMESTAMP_SECONDS(dateEpoch)) as latest_date
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
        """
    ),

    # Q40: Missing Data Analysis - use TIMESTAMP_SECONDS and CAST
    "Q40": BenchmarkQuery(
        id="Q40",
        name="Missing Data Analysis",
        description="Identify symbols with gaps in trading data (BigQuery)",
        category="quality",
        sql_template="""
            WITH date_ranges AS (
                SELECT
                    symbol,
                    MIN(dateEpoch) as first_date,
                    MAX(dateEpoch) as last_date,
                    COUNT(*) as actual_records
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
                GROUP BY symbol
            ),
            expected_days AS (
                SELECT
                    symbol,
                    first_date,
                    last_date,
                    actual_records,
                    CAST((last_date - first_date) / 86400 AS INT64) as total_days
                FROM date_ranges
            )
            SELECT
                symbol,
                TIMESTAMP_SECONDS(first_date) as first_date,
                TIMESTAMP_SECONDS(last_date) as last_date,
                actual_records,
                total_days,
                (total_days - actual_records) as missing_days,
                SAFE_DIVIDE(CAST(actual_records AS FLOAT64), total_days) as completeness_ratio
            FROM expected_days
            WHERE total_days > 0 AND actual_records < total_days * 0.8
            ORDER BY missing_days DESC
            LIMIT 100
        """
    ),

    # Q42: Schema Exploration - use TIMESTAMP_SECONDS
    "Q42": BenchmarkQuery(
        id="Q42",
        name="Schema Exploration",
        description="Explore available columns and data types (BigQuery)",
        category="quality",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as records,
                MIN(TIMESTAMP_SECONDS(dateEpoch)) as min_date,
                MAX(TIMESTAMP_SECONDS(dateEpoch)) as max_date,
                AVG(close) as avg_close,
                MIN(close) as min_close,
                MAX(close) as max_close,
                AVG(volume) as avg_volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol
            HAVING COUNT(*) > 100
            ORDER BY records DESC
            LIMIT 100
        """
    ),
}

ATHENA_OVERRIDES = {
    # Q10: Athena uses from_unixtime() instead of to_timestamp()
    "Q10": BenchmarkQuery(
        id="Q10",
        name="Monthly Volume Aggregation",
        description="Total volume by month for top symbols (Athena)",
        category="aggregation",
        sql_template="""
            SELECT
                symbol,
                date_trunc('month', from_unixtime(dateEpoch)) as month,
                SUM(volume) as total_volume,
                AVG(close) as avg_close
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
            GROUP BY symbol, date_trunc('month', from_unixtime(dateEpoch))
            ORDER BY symbol, month
        """
    ),

    # Q28: Athena uses approx_percentile() instead of APPROX_QUANTILE
    "Q28": BenchmarkQuery(
        id="Q28",
        name="Median Return by Symbol",
        description="Calculate median daily return (Athena)",
        category="analytics",
        sql_template="""
            WITH daily_returns AS (
                SELECT
                    symbol,
                    dateEpoch,
                    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            filtered AS (
                SELECT symbol, daily_return
                FROM daily_returns
                WHERE daily_return IS NOT NULL
            )
            SELECT
                symbol,
                approx_percentile(daily_return, 0.5) as median_return,
                approx_percentile(daily_return, 0.25) as q1_return,
                approx_percentile(daily_return, 0.75) as q3_return,
                stddev(daily_return) as volatility,
                COUNT(*) as days
            FROM filtered
            GROUP BY symbol
            HAVING COUNT(*) > 200
            ORDER BY volatility DESC
            LIMIT 100
        """
    ),

    # Q39: Data Completeness - use from_unixtime()
    "Q39": BenchmarkQuery(
        id="Q39",
        name="Data Completeness Check",
        description="Count records per table and date range (Athena)",
        category="quality",
        sql_template="""
            SELECT
                'stock_eod' as table_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT dateEpoch) as unique_dates,
                MIN(from_unixtime(dateEpoch)) as earliest_date,
                MAX(from_unixtime(dateEpoch)) as latest_date
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
        """
    ),

    # Q40: Missing Data Analysis - use from_unixtime() and CAST
    "Q40": BenchmarkQuery(
        id="Q40",
        name="Missing Data Analysis",
        description="Identify symbols with gaps in trading data (Athena)",
        category="quality",
        sql_template="""
            WITH date_ranges AS (
                SELECT
                    symbol,
                    MIN(dateEpoch) as first_date,
                    MAX(dateEpoch) as last_date,
                    COUNT(*) as actual_records
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
                GROUP BY symbol
            ),
            expected_days AS (
                SELECT
                    symbol,
                    first_date,
                    last_date,
                    actual_records,
                    CAST((last_date - first_date) / 86400 AS INTEGER) as total_days
                FROM date_ranges
            )
            SELECT
                symbol,
                from_unixtime(first_date) as first_date,
                from_unixtime(last_date) as last_date,
                actual_records,
                total_days,
                (total_days - actual_records) as missing_days,
                CAST(actual_records AS DOUBLE) / NULLIF(total_days, 0) as completeness_ratio
            FROM expected_days
            WHERE total_days > 0 AND actual_records < total_days * 0.8
            ORDER BY missing_days DESC
            LIMIT 100
        """
    ),

    # Q42: Schema Exploration - use from_unixtime()
    "Q42": BenchmarkQuery(
        id="Q42",
        name="Schema Exploration",
        description="Explore available columns and data types (Athena)",
        category="quality",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as records,
                MIN(from_unixtime(dateEpoch)) as min_date,
                MAX(from_unixtime(dateEpoch)) as max_date,
                AVG(close) as avg_close,
                MIN(close) as min_close,
                MAX(close) as max_close,
                AVG(volume) as avg_volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol
            HAVING COUNT(*) > 100
            ORDER BY records DESC
            LIMIT 100
        """
    ),

    # Q23: Athena cannot nest window functions inside aggregation
    # Rewrite to compute returns first, then aggregate
    "Q23": BenchmarkQuery(
        id="Q23",
        name="Join with Aggregation",
        description="Average returns by sector (Athena)",
        category="join",
        sql_template="""
            WITH price_data AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            returns AS (
                SELECT
                    symbol,
                    AVG((close - prev_close) / NULLIF(prev_close, 0)) as avg_return
                FROM price_data
                WHERE prev_close IS NOT NULL AND prev_close > 0
                GROUP BY symbol
            )
            SELECT
                p.sector,
                COUNT(*) as num_stocks,
                AVG(r.avg_return) as sector_avg_return,
                stddev(r.avg_return) as sector_return_stddev
            FROM returns r
            JOIN {table_prefix}profile p ON r.symbol = p.symbol
            GROUP BY p.sector
            ORDER BY sector_avg_return DESC
        """
    ),

    # Q26: Athena uses dateFirstAdded (lowercase)
    "Q26": BenchmarkQuery(
        id="Q26",
        name="Index Constituent Historical Join",
        description="Track when stocks joined S&P 500 (Athena)",
        category="join",
        sql_template="""
            SELECT
                sp.symbol,
                sp.datefirstadded as dateAdded,
                p.companyName,
                p.sector,
                p.marketCap
            FROM {table_prefix}sp500_constituent sp
            JOIN {table_prefix}profile p ON sp.symbol = p.symbol
            ORDER BY sp.datefirstadded DESC
            LIMIT 500
        """
    ),

    # Q34: Athena cannot nest window functions inside aggregation
    # Rewrite with explicit CTEs
    "Q34": BenchmarkQuery(
        id="Q34",
        name="Sector Relative Strength",
        description="Compare stock performance vs sector average (Athena)",
        category="analytics",
        sql_template="""
            WITH price_data AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            stock_returns AS (
                SELECT
                    symbol,
                    AVG((close - prev_close) / NULLIF(prev_close, 0)) as avg_return
                FROM price_data
                WHERE prev_close IS NOT NULL AND prev_close > 0
                GROUP BY symbol
            ),
            sector_returns AS (
                SELECT
                    p.sector,
                    AVG(sr.avg_return) as sector_avg_return
                FROM stock_returns sr
                JOIN {table_prefix}profile p ON sr.symbol = p.symbol
                GROUP BY p.sector
            )
            SELECT
                sr.symbol,
                p.sector,
                sr.avg_return as stock_return,
                secr.sector_avg_return,
                (sr.avg_return - secr.sector_avg_return) as relative_strength
            FROM stock_returns sr
            JOIN {table_prefix}profile p ON sr.symbol = p.symbol
            JOIN sector_returns secr ON p.sector = secr.sector
            ORDER BY relative_strength DESC
            LIMIT 100
        """
    ),

    # Q35: Athena cannot nest window functions inside window functions
    # Rewrite with explicit CTEs
    "Q35": BenchmarkQuery(
        id="Q35",
        name="Golden Cross Detection",
        description="Detect 50-day SMA crossing above 200-day SMA (Athena)",
        category="timeseries",
        sql_template="""
            WITH sma_calc AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1640995200
            ),
            sma_with_prev AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    sma_50,
                    sma_200,
                    LAG(sma_50) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_sma_50
                FROM sma_calc
            )
            SELECT
                symbol,
                dateEpoch,
                close,
                sma_50,
                sma_200
            FROM sma_with_prev
            WHERE prev_sma_50 < sma_200 AND sma_50 > sma_200
              AND dateEpoch >= 1704067200
            ORDER BY dateEpoch DESC
            LIMIT 100
        """
    ),
}


def get_queries_for_platform(platform: str):
    """Get the appropriate query set for a platform."""
    queries = list(BASE_QUERIES)

    if platform in ["duckdb", "duckdb-s3"]:
        # Use DuckDB-optimized versions
        for i, q in enumerate(queries):
            if q.id in DUCKDB_OVERRIDES:
                queries[i] = DUCKDB_OVERRIDES[q.id]
    elif platform in ["bigquery", "bigquery-cached"]:
        # Use BigQuery-specific versions
        for i, q in enumerate(queries):
            if q.id in BIGQUERY_OVERRIDES:
                queries[i] = BIGQUERY_OVERRIDES[q.id]
    elif platform == "athena":
        # Use Athena-specific versions
        for i, q in enumerate(queries):
            if q.id in ATHENA_OVERRIDES:
                queries[i] = ATHENA_OVERRIDES[q.id]

    return queries
