#!/usr/bin/env python3
"""
DuckDB Local Benchmark with Full Profiling Metrics.

Runs benchmark queries on DuckDB with local parquet data and captures
all metrics needed for TS/CR cost calculation.

Metrics captured (matching ts-data-engine/pkg/duckdb/profiling.go):
  - CPU_TIME: CPU time in seconds
  - LATENCY: Total execution time
  - CUMULATIVE_ROWS_SCANNED: Rows examined
  - SYSTEM_PEAK_BUFFER_MEMORY: Peak memory usage
  - SYSTEM_PEAK_TEMP_DIR_SIZE: Peak temp disk usage

Cost calculation (matching ts-query-orchestrator/internal/models/models.go):
  cost = MAX(MinimumCost, CPUTime * CPUPrice + Latency * CPUPrice * LatencyMultiplier)

Usage:
    python3 run_duckdb_local_benchmark.py --queries Q01 --configs medium --runs 3
    python3 run_duckdb_local_benchmark.py --local-data-path /opt/insydia/data --track-cold-start
"""

import argparse
import csv
import json
import os
import tempfile
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

# ============================================================================
# CONFIGURATION
# ============================================================================

# Default local data path (France server)
DEFAULT_LOCAL_DATA_PATH = "/opt/insydia/data"
DATA_PREFIX = "data_source=fmp"

# Cost calculation constants (from ts_web_mono/backend/internal/services/pricing_service.go)
# These match the query_resource_limits table defaults
THREAD_RATE_PER_SEC = 0.00045    # $0.00045 per thread-second
RAM_RATE_PER_GB_SEC = 0.000081   # $0.000081 per GB-second RAM
DISK_RATE_PER_GB_SEC = 0.0000079 # $0.0000079 per GB-second disk
CPU_RATE_PER_SEC = 0.00045       # $0.00045 per CPU-second
MINIMUM_COST = 0.01              # $0.01 minimum per query

# Compute configurations (match data-engine defaults)
CONFIGS = [
    {"name": "minimal", "threads": 1, "memory": "2GB", "disk": "5GB"},
    {"name": "small", "threads": 4, "memory": "8GB", "disk": "10GB"},
    {"name": "medium", "threads": 8, "memory": "16GB", "disk": "20GB"},
    {"name": "large", "threads": 16, "memory": "32GB", "disk": "50GB"},
    {"name": "maximum", "threads": 32, "memory": "64GB", "disk": "100GB"},
]

# Table paths within data_source=fmp/
TABLE_PATHS = {
    "stock_eod": "tick_data/eod/*.parquet",
    "stock_crypto": "tick_data/crypto/granularity=1min/*.parquet",
    "financial_ratios": "statements/financial_ratios/*.parquet",
    "income_statement": "statements/income_statement/*.parquet",
    "balance_sheet": "statements/balance_sheet/*.parquet",
    "cashflow_statement": "statements/cash_flow_statement/*.parquet",
    "profile": "company/profile/*.parquet",
    "stock_peers": "company/stock_peers/*.parquet",
    "sp500_constituent": "indices/sp500_constituent/*.parquet",
    "nasdaq_constituent": "indices/nasdaq_constituent/*.parquet",
    "analyst_estimates": "analyst/analyst_estimates/*.parquet",
    "stock_grade": "analyst/stock_grade/*.parquet",
}

# ============================================================================
# QUERY CATEGORIES
# ============================================================================

QUERY_CATEGORIES = {
    # Category 1: Scans & Filters
    "Q01": "scan", "Q02": "scan", "Q03": "scan", "Q04": "scan", "Q05": "scan", "Q06": "scan",
    # Category 2: Aggregations
    "Q07": "aggregation", "Q08": "aggregation", "Q09": "aggregation", "Q10": "aggregation",
    "Q11": "aggregation", "Q12": "aggregation",
    # Category 3: Window Functions
    "Q13": "window", "Q14": "window", "Q15": "window", "Q16": "window", "Q17": "window", "Q18": "window",
    # Category 4: Joins
    "Q19": "join", "Q20": "join", "Q21": "join", "Q22": "join", "Q23": "join", "Q24": "join",
    "Q25": "join", "Q26": "join", "Q27": "join",
    # Category 5: Analytics (Complex)
    "Q28": "analytics", "Q29": "analytics", "Q30": "analytics", "Q31": "analytics",
    "Q32": "analytics", "Q33": "analytics", "Q34": "analytics",
    # Category 6: Time Series
    "Q35": "time_series", "Q36": "time_series", "Q37": "time_series", "Q38": "time_series",
    # Category 7: Data Quality
    "Q39": "data_quality", "Q40": "data_quality", "Q41": "data_quality", "Q42": "data_quality",
}

# ============================================================================
# QUERIES (subset - full 42 queries defined below)
# ============================================================================

QUERIES = {
    # Category 1: Scans & Filters
    "Q01": """SELECT * FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE symbol = 'AAPL' AND dateEpoch >= 1577836800
              ORDER BY dateEpoch""",

    "Q02": """SELECT symbol, dateEpoch, close, volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1736899200
              ORDER BY volume DESC LIMIT 10000""",

    "Q03": """SELECT symbol, dateEpoch, close, volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1734307200 AND volume > 100000000
              ORDER BY volume DESC""",

    "Q04": """SELECT symbol, dateEpoch, open, close,
                     ((close - open) / open * 100) as pct_change
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1734307200
                AND ABS((close - open) / open) > 0.05
              ORDER BY ABS(pct_change) DESC LIMIT 1000""",

    "Q05": """SELECT e.symbol, e.dateEpoch, e.close, p.sector
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1727740800 AND p.sector = 'Technology'
              LIMIT 5000""",

    "Q06": """SELECT e.symbol, e.dateEpoch, e.close, e.volume, p.sector, p.marketCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1734307200
                AND e.volume > 50000000 AND e.close < 50
                AND p.sector = 'Technology'
              ORDER BY e.volume DESC LIMIT 500""",

    # Category 2: Aggregations
    "Q07": """SELECT COUNT(*) as num_stocks, AVG(close) as avg_close,
                     AVG(volume) as avg_volume, SUM(volume) as total_volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch = 1736899200""",

    "Q08": """SELECT symbol, COUNT(*) as trading_days, AVG(close) as avg_close,
                     MAX(volume) as max_volume, MIN(close) as min_close, MAX(close) as max_close
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1672531200
              GROUP BY symbol ORDER BY max_volume DESC LIMIT 100""",

    "Q09": """SELECT sector, COUNT(*) as num_companies,
                     AVG(marketCap) as avg_market_cap, SUM(marketCap) as total_market_cap
              FROM read_parquet('{base_path}/company/profile/*.parquet')
              WHERE marketCap > 0 GROUP BY sector ORDER BY total_market_cap DESC""",

    "Q10": """SELECT symbol,
                     (dateEpoch - (dateEpoch % 2592000)) as month_epoch,
                     SUM(volume) as total_volume, AVG(close) as avg_close
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1704067200
                AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
              GROUP BY symbol, month_epoch ORDER BY symbol, month_epoch""",

    "Q11": """SELECT symbol, COUNT(*) as days, STDDEV(close) as price_stddev,
                     AVG(close) as avg_price,
                     STDDEV(close) / NULLIF(AVG(close), 0) as coeff_variation
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1672531200
              GROUP BY symbol HAVING COUNT(*) > 200
              ORDER BY coeff_variation DESC LIMIT 100""",

    "Q12": """WITH first_last AS (
                  SELECT symbol, MIN(dateEpoch) as first_date, MAX(dateEpoch) as last_date
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
                  WHERE dateEpoch >= 1704067200 GROUP BY symbol
              ),
              prices AS (
                  SELECT e.symbol,
                         FIRST_VALUE(e.close) OVER (PARTITION BY e.symbol ORDER BY e.dateEpoch) as first_close,
                         LAST_VALUE(e.close) OVER (PARTITION BY e.symbol ORDER BY e.dateEpoch
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_close
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
                  JOIN first_last fl ON e.symbol = fl.symbol
                  WHERE e.dateEpoch >= 1704067200
              )
              SELECT DISTINCT symbol, first_close, last_close,
                     ((last_close - first_close) / first_close * 100) as pct_return
              FROM prices WHERE first_close > 0
              ORDER BY pct_return DESC LIMIT 50""",

    "Q13": """SELECT industry, sector, COUNT(*) as num_companies,
                     AVG(marketCap) as avg_market_cap, MAX(marketCap) as largest_company_cap
              FROM read_parquet('{base_path}/company/profile/*.parquet')
              WHERE marketCap > 0
              GROUP BY industry, sector HAVING COUNT(*) >= 5
              ORDER BY avg_market_cap DESC LIMIT 100""",

    # Category 3: Window Functions
    "Q14": """SELECT symbol, dateEpoch, close,
                     AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE symbol IN (SELECT symbol FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') LIMIT 50)
                AND dateEpoch >= 1672531200
              ORDER BY symbol, dateEpoch""",

    "Q15": """SELECT symbol, dateEpoch, close,
                     AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
                     AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                     AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
                AND dateEpoch >= 1640995200
              ORDER BY symbol, dateEpoch""",

    "Q16": """SELECT symbol, dateEpoch, close,
                     LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                     (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                         / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1704067200
              ORDER BY symbol, dateEpoch LIMIT 50000""",

    "Q17": """WITH base_prices AS (
                  SELECT symbol,
                         FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as base_close
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1704067200
              )
              SELECT e.symbol, e.dateEpoch, e.close, b.base_close,
                     ((e.close - b.base_close) / b.base_close * 100) as cumulative_return
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN base_prices b ON e.symbol = b.symbol
              WHERE e.dateEpoch >= 1704067200
                AND e.symbol IN (SELECT symbol FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') LIMIT 20)
              ORDER BY e.symbol, e.dateEpoch""",

    "Q18": """WITH ranked AS (
                  SELECT e.symbol, e.dateEpoch, e.volume, p.sector,
                         RANK() OVER (PARTITION BY e.dateEpoch, p.sector ORDER BY e.volume DESC) as volume_rank
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
                  JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
                  WHERE e.dateEpoch >= 1734307200
              )
              SELECT * FROM ranked WHERE volume_rank <= 10
              ORDER BY dateEpoch DESC, sector, volume_rank LIMIT 1000""",

    "Q19": """SELECT symbol, dateEpoch, close,
                     STDDEV(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as rolling_volatility_30d
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
                AND dateEpoch >= 1672531200
              ORDER BY symbol, dateEpoch""",

    "Q20": """WITH gaps AS (
                  SELECT symbol, dateEpoch, open, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                         ((open - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) * 100) as gap_pct
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1704067200
              )
              SELECT * FROM gaps WHERE ABS(gap_pct) > 5
              ORDER BY ABS(gap_pct) DESC LIMIT 500""",

    # Category 4: Joins
    "Q21": """SELECT e.symbol, e.dateEpoch, e.close, e.volume,
                     p.companyName, p.sector, p.industry, p.marketCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1734307200
              ORDER BY e.volume DESC LIMIT 5000""",

    "Q22": """SELECT e.symbol, e.dateEpoch, e.close, p.sector, p.marketCap,
                     r.netProfitMargin, r.assetTurnover, r.currentRatio
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON e.symbol = r.symbol
              WHERE e.dateEpoch >= 1734307200 AND r.netProfitMargin > 0.15
              ORDER BY r.netProfitMargin DESC LIMIT 1000""",

    "Q23": """WITH daily_returns AS (
                  SELECT symbol, dateEpoch, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              ),
              stock_returns AS (
                  SELECT symbol, AVG((close - prev_close) / NULLIF(prev_close, 0)) as avg_return
                  FROM daily_returns WHERE prev_close IS NOT NULL
                  GROUP BY symbol
              )
              SELECT p.sector, COUNT(*) as num_stocks,
                     AVG(sr.avg_return) as sector_avg_return,
                     STDDEV(sr.avg_return) as sector_return_stddev
              FROM stock_returns sr
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON sr.symbol = p.symbol
              GROUP BY p.sector ORDER BY sector_avg_return DESC""",

    "Q24": """SELECT e.symbol, e.dateEpoch, e.close,
                     a.epsAvg, a.numAnalystsEps,
                     p.companyName, p.marketCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/analyst/analyst_estimates/*.parquet') a ON e.symbol = a.symbol
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1734307200 AND a.numAnalystsEps >= 5
              ORDER BY a.numAnalystsEps DESC LIMIT 1000""",

    "Q25": """SELECT e.symbol, e.dateEpoch, e.close, p.sector, peers.companyName as peer_name, peers.mktCap as peer_mktCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              LEFT JOIN read_parquet('{base_path}/company/stock_peers/*.parquet') peers ON e.symbol = peers.symbol
              WHERE e.dateEpoch >= 1734307200
                AND e.symbol IN (SELECT symbol FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') LIMIT 50)
              ORDER BY p.marketCap DESC LIMIT 1000""",

    "Q26": """SELECT sp.symbol, sp.dateFirstAdded, p.companyName, p.sector, p.marketCap
              FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') sp
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON sp.symbol = p.symbol
              ORDER BY sp.dateFirstAddedEpoch DESC LIMIT 500""",

    "Q27": """SELECT e1.symbol as symbol1, e2.symbol as symbol2,
                     CORR(e1.close, e2.close) as price_correlation
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e1
              JOIN read_parquet('{base_path}/tick_data/eod/*.parquet') e2
                  ON e1.dateEpoch = e2.dateEpoch AND e1.symbol < e2.symbol
              WHERE e1.dateEpoch >= 1672531200
                AND e1.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX')
                AND e2.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX')
              GROUP BY e1.symbol, e2.symbol ORDER BY price_correlation DESC""",

    # Category 5: Complex Analytics
    "Q28": """WITH daily_returns AS (
                  SELECT symbol, dateEpoch,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              )
              SELECT symbol,
                     APPROX_QUANTILE(daily_return, 0.5) as median_return,
                     APPROX_QUANTILE(daily_return, 0.25) as q1_return,
                     APPROX_QUANTILE(daily_return, 0.75) as q3_return,
                     STDDEV(daily_return) as volatility, COUNT(*) as days
              FROM daily_returns WHERE daily_return IS NOT NULL
              GROUP BY symbol HAVING COUNT(*) > 200
              ORDER BY volatility DESC LIMIT 100""",

    "Q29": """WITH returns AS (
                  SELECT symbol,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              ),
              stats AS (
                  SELECT symbol, AVG(daily_return) as avg_return, STDDEV(daily_return) as stddev_return
                  FROM returns WHERE daily_return IS NOT NULL
                  GROUP BY symbol HAVING COUNT(*) > 200
              )
              SELECT symbol, avg_return, stddev_return,
                     (avg_return / NULLIF(stddev_return, 0)) as sharpe_ratio
              FROM stats WHERE stddev_return > 0
              ORDER BY sharpe_ratio DESC LIMIT 100""",

    "Q30": """WITH spy_returns AS (
                  SELECT dateEpoch,
                         (close - LAG(close) OVER (ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (ORDER BY dateEpoch), 0) as market_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
                  WHERE symbol = 'SPY' AND dateEpoch >= 1672531200
              ),
              stock_returns AS (
                  SELECT symbol, dateEpoch,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as stock_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
                  WHERE dateEpoch >= 1672531200
                    AND symbol IN (SELECT symbol FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') LIMIT 50)
              )
              SELECT sr.symbol,
                     COVAR_POP(sr.stock_return, spy.market_return) / NULLIF(VAR_POP(spy.market_return), 0) as beta
              FROM stock_returns sr
              JOIN spy_returns spy ON sr.dateEpoch = spy.dateEpoch
              WHERE sr.stock_return IS NOT NULL AND spy.market_return IS NOT NULL
              GROUP BY sr.symbol ORDER BY beta DESC""",

    "Q31": """SELECT p.symbol, p.companyName, p.sector, p.marketCap,
                     r.priceToEarningsRatio, r.priceToBookRatio, r.enterpriseValueMultiple, r.netProfitMargin, r.debtToEquityRatio,
                     (1 / NULLIF(r.priceToEarningsRatio, 0) + 1 / NULLIF(r.priceToBookRatio, 0) + r.netProfitMargin) as value_score
              FROM read_parquet('{base_path}/company/profile/*.parquet') p
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON p.symbol = r.symbol
              WHERE p.marketCap > 1000000000
                AND r.priceToEarningsRatio > 0 AND r.priceToEarningsRatio < 20
                AND r.priceToBookRatio > 0 AND r.priceToBookRatio < 3
                AND r.netProfitMargin > 0.15 AND r.debtToEquityRatio < 1.5
              ORDER BY value_score DESC LIMIT 100""",

    "Q32": """WITH price_points AS (
                  SELECT DISTINCT symbol,
                         FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch DESC
                             RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_close,
                         FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                             CASE WHEN dateEpoch >= 1728777600 THEN dateEpoch END DESC) as close_3m_ago,
                         FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                             CASE WHEN dateEpoch >= 1720137600 THEN dateEpoch END DESC) as close_6m_ago,
                         FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY
                             CASE WHEN dateEpoch >= 1704067200 THEN dateEpoch END DESC) as close_12m_ago
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1704067200
              )
              SELECT symbol, latest_close,
                     ((latest_close - close_3m_ago) / NULLIF(close_3m_ago, 0) * 100) as return_3m,
                     ((latest_close - close_6m_ago) / NULLIF(close_6m_ago, 0) * 100) as return_6m,
                     ((latest_close - close_12m_ago) / NULLIF(close_12m_ago, 0) * 100) as return_12m,
                     (((latest_close - close_3m_ago) / NULLIF(close_3m_ago, 0)) +
                      ((latest_close - close_6m_ago) / NULLIF(close_6m_ago, 0)) +
                      ((latest_close - close_12m_ago) / NULLIF(close_12m_ago, 0))) as momentum_score
              FROM price_points
              WHERE close_3m_ago > 0 AND close_6m_ago > 0 AND close_12m_ago > 0
              ORDER BY momentum_score DESC LIMIT 100""",

    "Q33": """SELECT p.symbol, p.companyName, p.sector,
                     r.netProfitMargin, r.assetTurnover, r.currentRatio, r.debtToEquityRatio,
                     r.grossProfitMargin, r.operatingProfitMargin,
                     (r.netProfitMargin * 0.3 + r.assetTurnover * 0.2 + LEAST(r.currentRatio / 2, 1) * 0.2 +
                      (1 / (1 + COALESCE(r.debtToEquityRatio, 0))) * 0.1 + r.grossProfitMargin * 0.1 +
                      r.operatingProfitMargin * 0.1) as quality_score
              FROM read_parquet('{base_path}/company/profile/*.parquet') p
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON p.symbol = r.symbol
              WHERE p.marketCap > 1000000000 AND r.netProfitMargin > 0 AND r.assetTurnover > 0
              ORDER BY quality_score DESC LIMIT 100""",

    "Q34": """WITH daily_returns AS (
                  SELECT symbol, dateEpoch, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              ),
              stock_returns AS (
                  SELECT symbol, AVG((close - prev_close) / NULLIF(prev_close, 0)) as avg_return
                  FROM daily_returns WHERE prev_close IS NOT NULL
                  GROUP BY symbol
              ),
              sector_returns AS (
                  SELECT p.sector, AVG(sr.avg_return) as sector_avg_return
                  FROM stock_returns sr
                  JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON sr.symbol = p.symbol
                  GROUP BY p.sector
              )
              SELECT sr.symbol, p.sector, sr.avg_return as stock_return,
                     secr.sector_avg_return,
                     (sr.avg_return - secr.sector_avg_return) as relative_strength
              FROM stock_returns sr
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON sr.symbol = p.symbol
              JOIN sector_returns secr ON p.sector = secr.sector
              ORDER BY relative_strength DESC LIMIT 100""",

    # Category 6: Time Series
    "Q35": """WITH sma_calc AS (
                  SELECT symbol, dateEpoch, close,
                         AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                         AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1640995200
              ),
              sma_with_lag AS (
                  SELECT symbol, dateEpoch, close, sma_50, sma_200,
                         LAG(sma_50) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_sma_50
                  FROM sma_calc
              )
              SELECT symbol, dateEpoch, close, sma_50, sma_200
              FROM sma_with_lag
              WHERE prev_sma_50 < sma_200 AND sma_50 > sma_200 AND dateEpoch >= 1704067200
              ORDER BY dateEpoch DESC LIMIT 100""",

    "Q36": """WITH price_changes AS (
                  SELECT symbol, dateEpoch, close,
                         close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as price_change
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
                  WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA') AND dateEpoch >= 1672531200
              ),
              gains_losses AS (
                  SELECT symbol, dateEpoch, close,
                         CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
                         CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
                  FROM price_changes
              ),
              avg_gains_losses AS (
                  SELECT symbol, dateEpoch, close,
                         AVG(gain) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
                         AVG(loss) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss
                  FROM gains_losses
              )
              SELECT symbol, dateEpoch, close, avg_gain, avg_loss,
                     100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) as rsi
              FROM avg_gains_losses WHERE avg_loss > 0
              ORDER BY symbol, dateEpoch DESC LIMIT 1000""",

    "Q37": """WITH stats AS (
                  SELECT symbol, dateEpoch, close,
                         AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
                         STDDEV(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev_20
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
                  WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA') AND dateEpoch >= 1672531200
              )
              SELECT symbol, dateEpoch, close, sma_20,
                     sma_20 + (2 * stddev_20) as upper_band,
                     sma_20 - (2 * stddev_20) as lower_band,
                     CASE WHEN close > sma_20 + (2 * stddev_20) THEN 'Overbought'
                          WHEN close < sma_20 - (2 * stddev_20) THEN 'Oversold'
                          ELSE 'Normal' END as signal
              FROM stats ORDER BY symbol, dateEpoch DESC LIMIT 1000""",

    "Q38": """WITH volume_stats AS (
                  SELECT symbol, dateEpoch, volume,
                         AVG(volume) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as avg_volume_30d,
                         STDDEV(volume) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as stddev_volume_30d
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              )
              SELECT symbol, dateEpoch, volume, avg_volume_30d,
                     ((volume - avg_volume_30d) / NULLIF(stddev_volume_30d, 0)) as z_score
              FROM volume_stats
              WHERE volume > avg_volume_30d + (2 * stddev_volume_30d) AND dateEpoch >= 1704067200
              ORDER BY z_score DESC LIMIT 500""",

    # Category 7: Data Quality
    "Q39": """SELECT 'stock_eod' as table_name, COUNT(*) as total_records,
                     COUNT(DISTINCT symbol) as unique_symbols, COUNT(DISTINCT dateEpoch) as unique_dates,
                     MIN(dateEpoch) as earliest_epoch,
                     MAX(dateEpoch) as latest_epoch
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200""",

    "Q40": """WITH date_ranges AS (
                  SELECT symbol, MIN(dateEpoch) as first_date, MAX(dateEpoch) as last_date,
                         COUNT(*) as actual_records
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1704067200
                  GROUP BY symbol
              ),
              expected_days AS (
                  SELECT symbol, first_date, last_date, actual_records,
                         ((last_date - first_date) / 86400) as total_days
                  FROM date_ranges
              )
              SELECT symbol, first_date as first_epoch,
                     last_date as last_epoch, actual_records, total_days,
                     (total_days - actual_records) as missing_days,
                     (actual_records::FLOAT / NULLIF(total_days, 0)) as completeness_ratio
              FROM expected_days
              WHERE total_days > 0 AND actual_records < total_days * 0.8
              ORDER BY missing_days DESC LIMIT 100""",

    "Q41": """WITH returns AS (
                  SELECT symbol, dateEpoch, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              )
              SELECT symbol, dateEpoch, close, prev_close, daily_return, ABS(daily_return) as abs_return
              FROM returns
              WHERE ABS(daily_return) > 0.2 OR close <= 0 OR prev_close <= 0
              ORDER BY abs_return DESC LIMIT 500""",

    "Q42": """SELECT symbol, COUNT(*) as records,
                     MIN(dateEpoch) as min_epoch,
                     MAX(dateEpoch) as max_epoch,
                     AVG(close) as avg_close, MIN(close) as min_close, MAX(close) as max_close,
                     AVG(volume) as avg_volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              GROUP BY symbol HAVING COUNT(*) > 100
              ORDER BY records DESC LIMIT 100""",
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ProfilingMetrics:
    """Metrics extracted from DuckDB profiling (matching data-engine)."""
    cpu_time_seconds: float = 0.0
    latency_seconds: float = 0.0
    cumulative_rows_scanned: int = 0
    rows_returned: int = 0
    peak_memory_bytes: int = 0
    peak_disk_bytes: int = 0

    @property
    def peak_memory_mb(self) -> float:
        return self.peak_memory_bytes / (1024 * 1024)

    @property
    def peak_disk_mb(self) -> float:
        return self.peak_disk_bytes / (1024 * 1024)


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    query_id: str
    query_category: str
    config_name: str
    threads: int
    memory_limit: str
    disk_limit: str
    run_number: int
    is_cold_start: bool

    # Timing (in milliseconds for consistency)
    latency_ms: float
    cpu_time_ms: float

    # Resource usage
    peak_memory_mb: float
    peak_disk_mb: float
    rows_scanned: int
    rows_returned: int

    # Cost calculation
    calculated_cost: float

    # Metadata
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ============================================================================
# COST CALCULATION
# ============================================================================

def calculate_cost(
    threads: int,
    ram_mb: int,
    disk_mb: int,
    latency_sec: float,
    cpu_time_sec: float,
    thread_rate: float = THREAD_RATE_PER_SEC,
    ram_rate: float = RAM_RATE_PER_GB_SEC,
    disk_rate: float = DISK_RATE_PER_GB_SEC,
    cpu_rate: float = CPU_RATE_PER_SEC,
    minimum: float = MINIMUM_COST,
) -> float:
    """
    Calculate query cost matching TS/CR backend formula.

    From ts_web_mono/backend/internal/services/pricing_service.go:
      threadCost = threads × latency × threadRate
      ramCost    = (ramMB/1024) × latency × ramRate
      diskCost   = (diskMB/1024) × latency × diskRate
      cpuCost    = cpuTime × cpuRate
      total      = MAX(minimum, threadCost + ramCost + diskCost + cpuCost)
    """
    thread_cost = threads * latency_sec * thread_rate
    ram_cost = (ram_mb / 1024) * latency_sec * ram_rate
    disk_cost = (disk_mb / 1024) * latency_sec * disk_rate
    cpu_cost = cpu_time_sec * cpu_rate

    total = thread_cost + ram_cost + disk_cost + cpu_cost
    return max(minimum, round(total, 9))  # 9 decimal precision like backend


# ============================================================================
# DUCKDB PROFILING
# ============================================================================

def create_connection(
    threads: int,
    memory_limit: str,
    disk_limit: str,
    temp_dir: str,
    enable_profiling: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with profiling enabled."""
    conn = duckdb.connect(":memory:")

    # Resource limits
    conn.execute(f"SET threads = {threads}")
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET max_temp_directory_size = '{disk_limit}'")
    conn.execute(f"SET temp_directory = '{temp_dir}'")

    # Profiling - use 'no_output' to prevent stdout spam, then extract via API
    if enable_profiling:
        conn.execute("SET enable_profiling = 'no_output'")
        conn.execute("SET profiling_mode = 'detailed'")

    conn.execute("SET enable_object_cache = true")
    conn.execute("SET preserve_insertion_order = false")

    return conn


def extract_profiling_metrics(conn: duckdb.DuckDBPyConnection, rows_returned: int) -> ProfilingMetrics:
    """
    Extract profiling metrics from DuckDB after query execution.

    This mimics the data-engine's ExtractProfilingMetrics function.
    Uses EXPLAIN ANALYZE to get detailed timing metrics.
    """
    metrics = ProfilingMetrics(rows_returned=rows_returned)

    # DuckDB Python doesn't expose the profiling API directly like Go does
    # We'll rely on wall-clock timing and estimate CPU from execution time
    # The data-engine Go code uses duckdb.GetProfilingInfo() which isn't available in Python

    return metrics


def run_query_with_profiling(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    profile_file: str,
) -> tuple[list, ProfilingMetrics]:
    """
    Execute a query and extract profiling metrics.

    Returns (result_rows, profiling_metrics)
    """
    metrics = ProfilingMetrics()

    # Set profiling output to a file
    conn.execute(f"SET profiling_output = '{profile_file}'")
    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_mode = 'detailed'")

    # Execute query
    start_time = time.perf_counter()
    result = conn.execute(sql).fetchall()
    wall_time = time.perf_counter() - start_time
    metrics.rows_returned = len(result)

    # Disable profiling
    conn.execute("SET enable_profiling = 'no_output'")

    # Read profiling output from file
    try:
        with open(profile_file, 'r') as f:
            json_output = f.read()

        # Find the JSON object in the output
        json_start = json_output.find('{')
        if json_start >= 0:
            # Find matching closing brace
            brace_count = 0
            json_end = json_start
            for i, c in enumerate(json_output[json_start:], json_start):
                if c == '{':
                    brace_count += 1
                elif c == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break

            json_str = json_output[json_start:json_end]
            profile_data = json.loads(json_str)

            # Extract metrics
            metrics.cpu_time_seconds = profile_data.get("cpu_time", 0.0)
            metrics.latency_seconds = profile_data.get("latency", wall_time)
            metrics.cumulative_rows_scanned = profile_data.get("cumulative_rows_scanned", 0)
            metrics.peak_memory_bytes = profile_data.get("system_peak_buffer_memory", 0)
            metrics.peak_disk_bytes = profile_data.get("system_peak_temp_dir_size", 0)
        else:
            metrics.latency_seconds = wall_time

    except (json.JSONDecodeError, ValueError, FileNotFoundError, IOError):
        # Fallback to wall time
        metrics.latency_seconds = wall_time

    # Clean up profile file
    try:
        os.remove(profile_file)
    except OSError:
        pass

    return result, metrics


def _sum_rows_scanned(profile_node: dict) -> int:
    """Recursively sum rows scanned from profiling tree."""
    total = 0
    if "cardinality" in profile_node:
        total += profile_node.get("cardinality", 0)
    for child in profile_node.get("children", []):
        total += _sum_rows_scanned(child)
    return total


def _parse_memory_string(mem_str: str) -> int:
    """Parse memory string like '256MB' or '1.5GB' to bytes."""
    try:
        mem_str = mem_str.strip().upper()
        if mem_str.endswith("GB"):
            return int(float(mem_str[:-2]) * 1024 * 1024 * 1024)
        elif mem_str.endswith("MB"):
            return int(float(mem_str[:-2]) * 1024 * 1024)
        elif mem_str.endswith("KB"):
            return int(float(mem_str[:-2]) * 1024)
        else:
            return int(mem_str)
    except Exception:
        return 0


def _parse_size_to_mb(size_str: str) -> int:
    """Parse size string like '16GB' or '512MB' to MB."""
    try:
        size_str = size_str.strip().upper()
        if size_str.endswith("GB"):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith("MB"):
            return int(float(size_str[:-2]))
        elif size_str.endswith("KB"):
            return int(float(size_str[:-2]) / 1024)
        else:
            return int(size_str)
    except Exception:
        return 0


# ============================================================================
# BENCHMARK RUNNER
# ============================================================================

def run_query(
    conn: duckdb.DuckDBPyConnection,
    query_id: str,
    sql: str,
    config: dict,
    run_number: int,
    is_cold_start: bool,
    temp_dir: str,
) -> BenchmarkResult:
    """Execute a single query and capture metrics."""

    # Create unique profile file for this query
    profile_file = os.path.join(temp_dir, f"profile_{query_id}_{run_number}_{int(time.time()*1000)}.json")
    query_category = QUERY_CATEGORIES.get(query_id, "unknown")

    try:
        # Execute with profiling
        result, metrics = run_query_with_profiling(conn, sql, profile_file)

        # Parse memory/disk from config (e.g., "16GB" -> 16384 MB)
        ram_mb = _parse_size_to_mb(config["memory"])
        disk_mb = _parse_size_to_mb(config["disk"])

        # Calculate cost using backend formula
        cost = calculate_cost(
            threads=config["threads"],
            ram_mb=ram_mb,
            disk_mb=disk_mb,
            latency_sec=metrics.latency_seconds,
            cpu_time_sec=metrics.cpu_time_seconds,
        )

        return BenchmarkResult(
            query_id=query_id,
            query_category=query_category,
            config_name=config["name"],
            threads=config["threads"],
            memory_limit=config["memory"],
            disk_limit=config["disk"],
            run_number=run_number,
            is_cold_start=is_cold_start,
            latency_ms=metrics.latency_seconds * 1000,
            cpu_time_ms=metrics.cpu_time_seconds * 1000,
            peak_memory_mb=metrics.peak_memory_mb,
            peak_disk_mb=metrics.peak_disk_mb,
            rows_scanned=metrics.cumulative_rows_scanned,
            rows_returned=metrics.rows_returned,
            calculated_cost=cost,
        )

    except Exception as e:
        return BenchmarkResult(
            query_id=query_id,
            query_category=query_category,
            config_name=config["name"],
            threads=config["threads"],
            memory_limit=config["memory"],
            disk_limit=config["disk"],
            run_number=run_number,
            is_cold_start=is_cold_start,
            latency_ms=0,
            cpu_time_ms=0,
            peak_memory_mb=0,
            peak_disk_mb=0,
            rows_scanned=0,
            rows_returned=0,
            calculated_cost=MINIMUM_COST,
            error=str(e),
        )


def run_benchmark(
    local_data_path: str,
    queries_to_run: dict,
    configs: list,
    runs: int,
    warmup: int,
    track_cold_start: bool,
) -> list[BenchmarkResult]:
    """Run the full benchmark suite."""

    all_results = []
    base_path = f"{local_data_path}/{DATA_PREFIX}"

    # Create temp directory for DuckDB
    temp_dir = tempfile.mkdtemp(prefix="duckdb_benchmark_")

    for config in configs:
        print(f"\n{'='*60}")
        print(f"Config: {config['name']} ({config['threads']} threads, {config['memory']})")
        print(f"{'='*60}")

        for query_id, sql_template in queries_to_run.items():
            sql = sql_template.format(base_path=base_path)
            print(f"\n  Query {query_id}:")

            # Cold start (fresh connection)
            if track_cold_start:
                print(f"    Cold start...", end=" ", flush=True)
                cold_conn = create_connection(
                    config["threads"], config["memory"], config["disk"], temp_dir
                )
                result = run_query(cold_conn, query_id, sql, config, 0, is_cold_start=True, temp_dir=temp_dir)
                cold_conn.close()

                if result.error:
                    print(f"ERROR: {result.error}")
                else:
                    print(f"{result.latency_ms:.1f}ms (cpu: {result.cpu_time_ms:.1f}ms, cost: ${result.calculated_cost:.4f})")
                    all_results.append(result)

            # Create connection for warm runs
            conn = create_connection(
                config["threads"], config["memory"], config["disk"], temp_dir
            )

            # Warmup runs (not recorded)
            for i in range(warmup):
                print(f"    Warmup {i+1}...", end=" ", flush=True)
                result = run_query(conn, query_id, sql, config, -(warmup - i), is_cold_start=False, temp_dir=temp_dir)
                if result.error:
                    print(f"ERROR: {result.error}")
                    break
                print(f"{result.latency_ms:.1f}ms")

            # Measured runs
            latencies = []
            for i in range(runs):
                print(f"    Run {i+1}...", end=" ", flush=True)
                result = run_query(conn, query_id, sql, config, i + 1, is_cold_start=False, temp_dir=temp_dir)

                if result.error:
                    print(f"ERROR: {result.error}")
                    break
                else:
                    print(f"{result.latency_ms:.1f}ms (cpu: {result.cpu_time_ms:.1f}ms, cost: ${result.calculated_cost:.4f})")
                    latencies.append(result.latency_ms)
                    all_results.append(result)

            conn.close()

            # Print summary
            if latencies:
                avg = sum(latencies) / len(latencies)
                print(f"    Avg: {avg:.1f}ms, Min: {min(latencies):.1f}ms, Max: {max(latencies):.1f}ms")

    return all_results


def save_results(results: list[BenchmarkResult], output_dir: Path):
    """Save results to CSV and JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # CSV
    csv_file = output_dir / f"duckdb_local_benchmark_{timestamp}.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_id", "query_category", "config", "threads", "memory", "disk", "run_number", "is_cold_start",
            "latency_ms", "cpu_time_ms", "peak_memory_mb", "peak_disk_mb",
            "rows_scanned", "rows_returned", "calculated_cost", "error", "timestamp"
        ])
        for r in results:
            writer.writerow([
                r.query_id, r.query_category, r.config_name, r.threads, r.memory_limit, r.disk_limit,
                r.run_number, r.is_cold_start, r.latency_ms, r.cpu_time_ms,
                r.peak_memory_mb, r.peak_disk_mb, r.rows_scanned, r.rows_returned,
                r.calculated_cost, r.error, r.timestamp
            ])
    print(f"\nResults saved to: {csv_file}")

    # JSON
    json_file = output_dir / f"duckdb_local_benchmark_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)
    print(f"Results saved to: {json_file}")

    # Summary
    print_summary(results)

    return csv_file


def print_summary(results: list[BenchmarkResult]):
    """Print summary table."""
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"{'Config':<12} {'Threads':>8} {'Memory':>8} {'Query':>6} {'Cold':>12} {'Warm Avg':>12} {'Cost':>10}")
    print("-" * 100)

    # Group by config and query
    from itertools import groupby
    from operator import attrgetter

    sorted_results = sorted(results, key=lambda r: (r.config_name, r.query_id))
    for (config_name, query_id), group in groupby(sorted_results, key=lambda r: (r.config_name, r.query_id)):
        group_list = list(group)
        cold = [r for r in group_list if r.is_cold_start and not r.error]
        warm = [r for r in group_list if not r.is_cold_start and not r.error]

        cold_ms = f"{cold[0].latency_ms:.0f}ms" if cold else "N/A"
        warm_avg = f"{sum(r.latency_ms for r in warm)/len(warm):.0f}ms" if warm else "N/A"
        avg_cost = f"${sum(r.calculated_cost for r in warm)/len(warm):.4f}" if warm else "N/A"

        config = next((c for c in CONFIGS if c["name"] == config_name), {})
        print(f"{config_name:<12} {config.get('threads', ''):>8} {config.get('memory', ''):>8} "
              f"{query_id:>6} {cold_ms:>12} {warm_avg:>12} {avg_cost:>10}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="DuckDB Local Benchmark with Full Profiling")
    parser.add_argument("--local-data-path", type=str, default=DEFAULT_LOCAL_DATA_PATH,
                        help=f"Path to local parquet data (default: {DEFAULT_LOCAL_DATA_PATH})")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated query IDs (e.g., Q01,Q02,Q07). Default: all")
    parser.add_argument("--configs", type=str, default=None,
                        help="Comma-separated config names (minimal,small,medium,large,maximum). Default: all")
    parser.add_argument("--runs", type=int, default=3, help="Number of measured runs per query")
    parser.add_argument("--warmup", type=int, default=1, help="Number of warmup runs")
    parser.add_argument("--track-cold-start", action="store_true", help="Track cold start separately")
    parser.add_argument("--output-dir", type=Path, default=Path("results"),
                        help="Output directory for results")
    args = parser.parse_args()

    # Parse queries
    if args.queries:
        query_ids = [q.strip().upper() for q in args.queries.split(",")]
        queries_to_run = {qid: QUERIES[qid] for qid in query_ids if qid in QUERIES}
    else:
        queries_to_run = QUERIES

    # Parse configs
    if args.configs:
        config_names = [c.strip().lower() for c in args.configs.split(",")]
        configs = [c for c in CONFIGS if c["name"] in config_names]
    else:
        configs = CONFIGS

    print("=" * 60)
    print("DuckDB LOCAL BENCHMARK - Full Profiling")
    print("=" * 60)
    print(f"Data path: {args.local_data_path}/{DATA_PREFIX}/")
    print(f"Queries: {list(queries_to_run.keys())}")
    print(f"Configs: {[c['name'] for c in configs]}")
    print(f"Runs: {args.runs} (+{args.warmup} warmup)")
    print(f"Cold start: {args.track_cold_start}")
    print()
    print("Cost formula (from backend pricing_service.go):")
    print("  thread_cost = threads × latency × $0.00045/thread-sec")
    print("  ram_cost    = RAM_GB × latency × $0.000081/GB-sec")
    print("  disk_cost   = DISK_GB × latency × $0.0000079/GB-sec")
    print("  cpu_cost    = cpu_time × $0.00045/cpu-sec")
    print("  total       = MAX($0.01, thread + ram + disk + cpu)")

    # Run benchmark
    results = run_benchmark(
        local_data_path=args.local_data_path,
        queries_to_run=queries_to_run,
        configs=configs,
        runs=args.runs,
        warmup=args.warmup,
        track_cold_start=args.track_cold_start,
    )

    # Save results
    if results:
        save_results(results, args.output_dir)
    else:
        print("\nNo results to save.")


if __name__ == "__main__":
    main()
