#!/usr/bin/env python3
"""
Comprehensive benchmark queries for cloud data platform comparison.

42 queries across 7 categories:
- Scans & Filters: Simple to complex filtering operations
- Aggregations: Basic to advanced grouping
- Window Functions: Time-series and ranking operations
- Joins: 2-way to multi-way joins
- Analytics: Statistical and financial calculations
- Time Series: Trading signals and technical indicators
- Data Quality: Validation and exploration queries
"""

from dataclasses import dataclass


@dataclass
class BenchmarkQuery:
    """A benchmark query with metadata."""
    id: str
    name: str
    description: str
    sql_template: str  # May contain {table_prefix} placeholder
    category: str


QUERIES = [
    # ===== SCANS & FILTERS (6 queries) =====
    BenchmarkQuery(
        id="Q01",
        name="Point Query - Single Stock",
        description="Fetch all EOD data for AAPL since 2020",
        category="scan",
        sql_template="""
            SELECT * FROM {table_prefix}stock_eod
            WHERE symbol = 'AAPL' AND dateEpoch >= 1577836800
            ORDER BY dateEpoch
        """
    ),
    BenchmarkQuery(
        id="Q02",
        name="Date Range Scan - All Stocks",
        description="Scan recent week of data across all symbols",
        category="scan",
        sql_template="""
            SELECT symbol, dateEpoch, close, volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1736899200
            ORDER BY volume DESC
            LIMIT 10000
        """
    ),
    BenchmarkQuery(
        id="Q03",
        name="High Volume Filter",
        description="Find stocks with volume > 100M in last 30 days",
        category="scan",
        sql_template="""
            SELECT symbol, dateEpoch, close, volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1734307200 AND volume > 100000000
            ORDER BY volume DESC
        """
    ),
    BenchmarkQuery(
        id="Q04",
        name="Price Movement Filter",
        description="Stocks with >5% daily move",
        category="scan",
        sql_template="""
            SELECT symbol, dateEpoch, open, close,
                   ((close - open) / open * 100) as pct_change
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1734307200
              AND ABS((close - open) / open) > 0.05
            ORDER BY ABS(pct_change) DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q05",
        name="Sector Filter with Join",
        description="All tech stocks in last quarter",
        category="scan",
        sql_template="""
            SELECT e.symbol, e.dateEpoch, e.close, p.sector
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1727740800 AND p.sector = 'Technology'
            LIMIT 5000
        """
    ),
    BenchmarkQuery(
        id="Q06",
        name="Multi-Condition Filter",
        description="Stocks with high volume, low price, tech sector",
        category="scan",
        sql_template="""
            SELECT e.symbol, e.dateEpoch, e.close, e.volume, p.sector, p.marketCap
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1734307200
              AND e.volume > 50000000
              AND e.close < 50
              AND p.sector = 'Technology'
            ORDER BY e.volume DESC
            LIMIT 500
        """
    ),

    # ===== AGGREGATIONS (7 queries) =====
    BenchmarkQuery(
        id="Q07",
        name="Daily Market Stats",
        description="Aggregate stats for recent trading day",
        category="aggregation",
        sql_template="""
            SELECT
                COUNT(*) as num_stocks,
                AVG(close) as avg_close,
                AVG(volume) as avg_volume,
                SUM(volume) as total_volume
            FROM {table_prefix}stock_eod
            WHERE dateEpoch = 1736899200
        """
    ),
    BenchmarkQuery(
        id="Q08",
        name="Symbol-Level Aggregation",
        description="Average price and volume per symbol (2023+)",
        category="aggregation",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as trading_days,
                AVG(close) as avg_close,
                MAX(volume) as max_volume,
                MIN(close) as min_close,
                MAX(close) as max_close
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol
            ORDER BY max_volume DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q09",
        name="Sector Aggregation",
        description="Average market cap and count by sector",
        category="aggregation",
        sql_template="""
            SELECT
                sector,
                COUNT(*) as num_companies,
                AVG(marketCap) as avg_market_cap,
                SUM(marketCap) as total_market_cap
            FROM {table_prefix}profile
            WHERE marketCap > 0
            GROUP BY sector
            ORDER BY total_market_cap DESC
        """
    ),
    BenchmarkQuery(
        id="Q10",
        name="Monthly Volume Aggregation",
        description="Total volume by month for top symbols",
        category="aggregation",
        sql_template="""
            SELECT
                symbol,
                DATE_TRUNC('month', to_timestamp(dateEpoch)) as month,
                SUM(volume) as total_volume,
                AVG(close) as avg_close
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
              AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
            GROUP BY symbol, month
            ORDER BY symbol, month
        """
    ),
    BenchmarkQuery(
        id="Q11",
        name="Volatility by Symbol",
        description="Calculate price volatility (stddev) per symbol",
        category="aggregation",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as days,
                STDDEV(close) as price_stddev,
                AVG(close) as avg_price,
                STDDEV(close) / NULLIF(AVG(close), 0) as coeff_variation
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
            GROUP BY symbol
            HAVING COUNT(*) > 200
            ORDER BY coeff_variation DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q12",
        name="Top Gainers Aggregation",
        description="Stocks with best YTD performance",
        category="aggregation",
        sql_template="""
            WITH first_last AS (
                SELECT
                    symbol,
                    MIN(dateEpoch) as first_date,
                    MAX(dateEpoch) as last_date
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
                GROUP BY symbol
            ),
            prices AS (
                SELECT
                    e.symbol,
                    FIRST_VALUE(e.close) OVER (PARTITION BY e.symbol ORDER BY e.dateEpoch) as first_close,
                    LAST_VALUE(e.close) OVER (PARTITION BY e.symbol ORDER BY e.dateEpoch
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_close
                FROM {table_prefix}stock_eod e
                JOIN first_last fl ON e.symbol = fl.symbol
                WHERE e.dateEpoch >= 1704067200
            )
            SELECT DISTINCT
                symbol,
                first_close,
                last_close,
                ((last_close - first_close) / first_close * 100) as pct_return
            FROM prices
            WHERE first_close > 0
            ORDER BY pct_return DESC
            LIMIT 50
        """
    ),
    BenchmarkQuery(
        id="Q13",
        name="Industry Concentration",
        description="Count of companies and avg market cap by industry",
        category="aggregation",
        sql_template="""
            SELECT
                industry,
                sector,
                COUNT(*) as num_companies,
                AVG(marketCap) as avg_market_cap,
                MAX(marketCap) as largest_company_cap
            FROM {table_prefix}profile
            WHERE marketCap > 0
            GROUP BY industry, sector
            HAVING COUNT(*) >= 5
            ORDER BY avg_market_cap DESC
            LIMIT 100
        """
    ),

    # ===== WINDOW FUNCTIONS (7 queries) =====
    BenchmarkQuery(
        id="Q14",
        name="Simple Moving Average (20-day)",
        description="Calculate 20-day SMA for S&P 500 stocks",
        category="window",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                close,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY dateEpoch
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as sma_20
            FROM {table_prefix}stock_eod
            WHERE symbol IN (SELECT symbol FROM {table_prefix}sp500_constituent LIMIT 50)
              AND dateEpoch >= 1672531200
            ORDER BY symbol, dateEpoch
        """
    ),
    BenchmarkQuery(
        id="Q15",
        name="Multiple Moving Averages",
        description="Calculate 20-day, 50-day, and 200-day SMAs",
        category="window",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                close,
                AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
                AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
            FROM {table_prefix}stock_eod
            WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
              AND dateEpoch >= 1640995200
            ORDER BY symbol, dateEpoch
        """
    ),
    BenchmarkQuery(
        id="Q16",
        name="Daily Returns",
        description="Calculate day-over-day returns for all stocks",
        category="window",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                    / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1704067200
            ORDER BY symbol, dateEpoch
            LIMIT 50000
        """
    ),
    BenchmarkQuery(
        id="Q17",
        name="Cumulative Returns",
        description="Calculate cumulative returns since start of year",
        category="window",
        sql_template="""
            WITH base_prices AS (
                SELECT
                    symbol,
                    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as base_close
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
            )
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                b.base_close,
                ((e.close - b.base_close) / b.base_close * 100) as cumulative_return
            FROM {table_prefix}stock_eod e
            JOIN base_prices b ON e.symbol = b.symbol
            WHERE e.dateEpoch >= 1704067200
              AND e.symbol IN (SELECT symbol FROM {table_prefix}sp500_constituent LIMIT 20)
            ORDER BY e.symbol, e.dateEpoch
        """
    ),
    BenchmarkQuery(
        id="Q18",
        name="Rank by Volume",
        description="Daily volume rank within each sector",
        category="window",
        sql_template="""
            WITH ranked AS (
                SELECT
                    e.symbol,
                    e.dateEpoch,
                    e.volume,
                    p.sector,
                    RANK() OVER (PARTITION BY e.dateEpoch, p.sector ORDER BY e.volume DESC) as volume_rank
                FROM {table_prefix}stock_eod e
                JOIN {table_prefix}profile p ON e.symbol = p.symbol
                WHERE e.dateEpoch >= 1734307200
            )
            SELECT *
            FROM ranked
            WHERE volume_rank <= 10
            ORDER BY dateEpoch DESC, sector, volume_rank
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q19",
        name="Rolling Volatility",
        description="30-day rolling standard deviation",
        category="window",
        sql_template="""
            SELECT
                symbol,
                dateEpoch,
                close,
                STDDEV(close) OVER (
                    PARTITION BY symbol
                    ORDER BY dateEpoch
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as rolling_volatility_30d
            FROM {table_prefix}stock_eod
            WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
              AND dateEpoch >= 1672531200
            ORDER BY symbol, dateEpoch
        """
    ),
    BenchmarkQuery(
        id="Q20",
        name="Gap Detection",
        description="Detect price gaps (>5% from previous close)",
        category="window",
        sql_template="""
            WITH gaps AS (
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
            )
            SELECT *
            FROM gaps
            WHERE ABS(gap_pct) > 5
            ORDER BY ABS(gap_pct) DESC
            LIMIT 500
        """
    ),

    # ===== JOINS (7 queries) =====
    BenchmarkQuery(
        id="Q21",
        name="Simple Join - EOD + Profile",
        description="Join recent EOD data with company profiles",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                e.volume,
                p.companyName,
                p.sector,
                p.industry,
                p.marketCap
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1734307200
            ORDER BY e.volume DESC
            LIMIT 5000
        """
    ),
    BenchmarkQuery(
        id="Q22",
        name="3-Way Join - EOD + Profile + Ratios",
        description="Combine price, company info, and financial ratios",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                p.sector,
                p.marketCap,
                r.roeTTM,
                r.roa,
                r.currentRatio
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            JOIN {table_prefix}financial_ratios r ON e.symbol = r.symbol
            WHERE e.dateEpoch >= 1734307200
              AND r.roeTTM > 0.15
            ORDER BY r.roeTTM DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q23",
        name="Join with Aggregation",
        description="Average returns by sector",
        category="join",
        sql_template="""
            WITH returns AS (
                SELECT
                    symbol,
                    AVG((close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0)) as avg_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
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
    BenchmarkQuery(
        id="Q24",
        name="Analyst Coverage Join",
        description="Stocks with analyst estimates and recent prices",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                a.estimatedEpsAvg,
                a.numberAnalystEstimatedEps,
                p.companyName,
                p.marketCap
            FROM {table_prefix}stock_eod e
            JOIN {table_prefix}analyst_estimates a ON e.symbol = a.symbol
            JOIN {table_prefix}profile p ON e.symbol = p.symbol
            WHERE e.dateEpoch >= 1734307200
              AND a.numberAnalystEstimatedEps >= 5
            ORDER BY a.numberAnalystEstimatedEps DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q25",
        name="Peer Comparison",
        description="Compare stocks to their peers",
        category="join",
        sql_template="""
            SELECT
                e.symbol,
                e.dateEpoch,
                e.close,
                p.sector,
                peers.peersList,
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
    BenchmarkQuery(
        id="Q26",
        name="Index Constituent Historical Join",
        description="Track when stocks joined S&P 500",
        category="join",
        sql_template="""
            SELECT
                sp.symbol,
                sp.dateAdded,
                p.companyName,
                p.sector,
                p.marketCap
            FROM {table_prefix}sp500_constituent sp
            JOIN {table_prefix}profile p ON sp.symbol = p.symbol
            ORDER BY sp.dateAdded DESC
            LIMIT 500
        """
    ),
    BenchmarkQuery(
        id="Q27",
        name="Self-Join for Correlation",
        description="Find stocks with correlated movements",
        category="join",
        sql_template="""
            SELECT
                e1.symbol as symbol1,
                e2.symbol as symbol2,
                CORR(e1.close, e2.close) as price_correlation
            FROM {table_prefix}stock_eod e1
            JOIN {table_prefix}stock_eod e2
                ON e1.dateEpoch = e2.dateEpoch
                AND e1.symbol < e2.symbol
            WHERE e1.dateEpoch >= 1672531200
              AND e1.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX')
              AND e2.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX')
            GROUP BY e1.symbol, e2.symbol
            ORDER BY price_correlation DESC
        """
    ),

    # ===== COMPLEX ANALYTICS (7 queries) =====
    BenchmarkQuery(
        id="Q28",
        name="Median Return by Symbol",
        description="Calculate median daily return and volatility",
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
                APPROX_QUANTILE(daily_return, 0.5) as median_return,
                APPROX_QUANTILE(daily_return, 0.25) as q1_return,
                APPROX_QUANTILE(daily_return, 0.75) as q3_return,
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
    BenchmarkQuery(
        id="Q29",
        name="Sharpe Ratio Calculation",
        description="Calculate risk-adjusted returns (simplified Sharpe)",
        category="analytics",
        sql_template="""
            WITH returns AS (
                SELECT
                    symbol,
                    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            ),
            stats AS (
                SELECT
                    symbol,
                    AVG(daily_return) as avg_return,
                    STDDEV(daily_return) as stddev_return
                FROM returns
                WHERE daily_return IS NOT NULL
                GROUP BY symbol
                HAVING COUNT(*) > 200
            )
            SELECT
                symbol,
                avg_return,
                stddev_return,
                (avg_return / NULLIF(stddev_return, 0)) as sharpe_ratio
            FROM stats
            WHERE stddev_return > 0
            ORDER BY sharpe_ratio DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q30",
        name="Beta Calculation",
        description="Calculate stock beta relative to S&P 500",
        category="analytics",
        sql_template="""
            WITH spy_returns AS (
                SELECT
                    dateEpoch,
                    (close - LAG(close) OVER (ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (ORDER BY dateEpoch), 0) as market_return
                FROM {table_prefix}stock_eod
                WHERE symbol = 'SPY' AND dateEpoch >= 1672531200
            ),
            stock_returns AS (
                SELECT
                    symbol,
                    dateEpoch,
                    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as stock_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
                  AND symbol IN (SELECT symbol FROM {table_prefix}sp500_constituent LIMIT 50)
            )
            SELECT
                sr.symbol,
                COVAR_POP(sr.stock_return, spy.market_return) / NULLIF(VAR_POP(spy.market_return), 0) as beta
            FROM stock_returns sr
            JOIN spy_returns spy ON sr.dateEpoch = spy.dateEpoch
            WHERE sr.stock_return IS NOT NULL AND spy.market_return IS NOT NULL
            GROUP BY sr.symbol
            ORDER BY beta DESC
        """
    ),
    BenchmarkQuery(
        id="Q31",
        name="Value Screening",
        description="Find undervalued stocks by multiple metrics",
        category="analytics",
        sql_template="""
            SELECT
                p.symbol,
                p.companyName,
                p.sector,
                p.marketCap,
                r.peRatio,
                r.priceToBookRatio,
                r.evToEbitda,
                r.roeTTM,
                r.debtToEquity,
                (1 / NULLIF(r.peRatio, 0) +
                 1 / NULLIF(r.priceToBookRatio, 0) +
                 r.roeTTM) as value_score
            FROM {table_prefix}profile p
            JOIN {table_prefix}financial_ratios r ON p.symbol = r.symbol
            WHERE p.marketCap > 1000000000
              AND r.peRatio > 0 AND r.peRatio < 20
              AND r.priceToBookRatio > 0 AND r.priceToBookRatio < 3
              AND r.roeTTM > 0.15
              AND r.debtToEquity < 1.5
            ORDER BY value_score DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q32",
        name="Momentum Ranking",
        description="Rank stocks by price momentum (3m, 6m, 12m)",
        category="analytics",
        sql_template="""
            WITH price_points AS (
                SELECT DISTINCT
                    symbol,
                    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch DESC
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_close,
                    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                        CASE WHEN dateEpoch >= 1728777600 THEN dateEpoch END DESC) as close_3m_ago,
                    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                        CASE WHEN dateEpoch >= 1720137600 THEN dateEpoch END DESC) as close_6m_ago,
                    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                        CASE WHEN dateEpoch >= 1704067200 THEN dateEpoch END DESC) as close_12m_ago
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1704067200
            )
            SELECT
                symbol,
                latest_close,
                ((latest_close - close_3m_ago) / NULLIF(close_3m_ago, 0) * 100) as return_3m,
                ((latest_close - close_6m_ago) / NULLIF(close_6m_ago, 0) * 100) as return_6m,
                ((latest_close - close_12m_ago) / NULLIF(close_12m_ago, 0) * 100) as return_12m,
                (((latest_close - close_3m_ago) / NULLIF(close_3m_ago, 0)) +
                 ((latest_close - close_6m_ago) / NULLIF(close_6m_ago, 0)) +
                 ((latest_close - close_12m_ago) / NULLIF(close_12m_ago, 0))) as momentum_score
            FROM price_points
            WHERE close_3m_ago > 0 AND close_6m_ago > 0 AND close_12m_ago > 0
            ORDER BY momentum_score DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q33",
        name="Quality Score",
        description="Composite quality score from financial metrics",
        category="analytics",
        sql_template="""
            SELECT
                p.symbol,
                p.companyName,
                p.sector,
                r.roeTTM,
                r.roa,
                r.currentRatio,
                r.debtToEquity,
                r.grossProfitMargin,
                r.operatingProfitMargin,
                (r.roeTTM * 0.3 +
                 r.roa * 0.2 +
                 LEAST(r.currentRatio / 2, 1) * 0.2 +
                 (1 / (1 + r.debtToEquity)) * 0.1 +
                 r.grossProfitMargin * 0.1 +
                 r.operatingProfitMargin * 0.1) as quality_score
            FROM {table_prefix}profile p
            JOIN {table_prefix}financial_ratios r ON p.symbol = r.symbol
            WHERE p.marketCap > 1000000000
              AND r.roeTTM > 0
              AND r.roa > 0
            ORDER BY quality_score DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q34",
        name="Sector Relative Strength",
        description="Compare stock performance vs sector average",
        category="analytics",
        sql_template="""
            WITH stock_returns AS (
                SELECT
                    symbol,
                    AVG((close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0)) as avg_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
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

    # ===== TIME SERIES PATTERNS (4 queries) =====
    BenchmarkQuery(
        id="Q35",
        name="Golden Cross Detection",
        description="Detect 50-day SMA crossing above 200-day SMA",
        category="timeseries",
        sql_template="""
            WITH sma_data AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200,
                    LAG(AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_sma_50
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1640995200
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
    BenchmarkQuery(
        id="Q36",
        name="RSI Calculation",
        description="Calculate 14-day Relative Strength Index",
        category="timeseries",
        sql_template="""
            WITH price_changes AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as price_change
                FROM {table_prefix}stock_eod
                WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
                  AND dateEpoch >= 1672531200
            ),
            gains_losses AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
                    CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
                FROM price_changes
            ),
            avg_gains_losses AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    AVG(gain) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
                    AVG(loss) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss
                FROM gains_losses
            )
            SELECT
                symbol,
                dateEpoch,
                close,
                avg_gain,
                avg_loss,
                100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) as rsi
            FROM avg_gains_losses
            WHERE avg_loss > 0
            ORDER BY symbol, dateEpoch DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q37",
        name="Bollinger Bands",
        description="Calculate 20-day Bollinger Bands",
        category="timeseries",
        sql_template="""
            WITH stats AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
                    STDDEV(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev_20
                FROM {table_prefix}stock_eod
                WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
                  AND dateEpoch >= 1672531200
            )
            SELECT
                symbol,
                dateEpoch,
                close,
                sma_20,
                sma_20 + (2 * stddev_20) as upper_band,
                sma_20 - (2 * stddev_20) as lower_band,
                CASE
                    WHEN close > sma_20 + (2 * stddev_20) THEN 'Overbought'
                    WHEN close < sma_20 - (2 * stddev_20) THEN 'Oversold'
                    ELSE 'Normal'
                END as signal
            FROM stats
            ORDER BY symbol, dateEpoch DESC
            LIMIT 1000
        """
    ),
    BenchmarkQuery(
        id="Q38",
        name="Volume Spike Detection",
        description="Detect unusual volume (>2 std dev above average)",
        category="timeseries",
        sql_template="""
            WITH volume_stats AS (
                SELECT
                    symbol,
                    dateEpoch,
                    volume,
                    AVG(volume) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as avg_volume_30d,
                    STDDEV(volume) OVER (PARTITION BY symbol ORDER BY dateEpoch
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as stddev_volume_30d
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            )
            SELECT
                symbol,
                dateEpoch,
                volume,
                avg_volume_30d,
                ((volume - avg_volume_30d) / NULLIF(stddev_volume_30d, 0)) as z_score
            FROM volume_stats
            WHERE volume > avg_volume_30d + (2 * stddev_volume_30d)
              AND dateEpoch >= 1704067200
            ORDER BY z_score DESC
            LIMIT 500
        """
    ),

    # ===== DATA QUALITY & EXPLORATION (4 queries) =====
    BenchmarkQuery(
        id="Q39",
        name="Data Completeness Check",
        description="Count records per table and date range",
        category="quality",
        sql_template="""
            SELECT
                'stock_eod' as table_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT dateEpoch) as unique_dates,
                MIN(to_timestamp(dateEpoch)) as earliest_date,
                MAX(to_timestamp(dateEpoch)) as latest_date
            FROM {table_prefix}stock_eod
            WHERE dateEpoch >= 1672531200
        """
    ),
    BenchmarkQuery(
        id="Q40",
        name="Missing Data Analysis",
        description="Identify symbols with gaps in trading data",
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
                    ((last_date - first_date) / 86400) as total_days
                FROM date_ranges
            )
            SELECT
                symbol,
                to_timestamp(first_date) as first_date,
                to_timestamp(last_date) as last_date,
                actual_records,
                total_days,
                (total_days - actual_records) as missing_days,
                (actual_records::FLOAT / NULLIF(total_days, 0)) as completeness_ratio
            FROM expected_days
            WHERE total_days > 0 AND actual_records < total_days * 0.8
            ORDER BY missing_days DESC
            LIMIT 100
        """
    ),
    BenchmarkQuery(
        id="Q41",
        name="Outlier Detection",
        description="Find extreme price movements and data anomalies",
        category="quality",
        sql_template="""
            WITH returns AS (
                SELECT
                    symbol,
                    dateEpoch,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                FROM {table_prefix}stock_eod
                WHERE dateEpoch >= 1672531200
            )
            SELECT
                symbol,
                dateEpoch,
                close,
                prev_close,
                daily_return,
                ABS(daily_return) as abs_return
            FROM returns
            WHERE ABS(daily_return) > 0.2
              OR close <= 0
              OR prev_close <= 0
            ORDER BY abs_return DESC
            LIMIT 500
        """
    ),
    BenchmarkQuery(
        id="Q42",
        name="Schema Exploration",
        description="Explore available columns and data types",
        category="quality",
        sql_template="""
            SELECT
                symbol,
                COUNT(*) as records,
                MIN(to_timestamp(dateEpoch)) as min_date,
                MAX(to_timestamp(dateEpoch)) as max_date,
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
]
