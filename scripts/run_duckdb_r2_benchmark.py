#!/usr/bin/env python3
"""
DuckDB R2 Benchmark with Full Profiling Metrics.

Runs benchmark queries on DuckDB reading from Cloudflare R2 and captures
all metrics needed for TS/CR cost calculation (matching local benchmark).

Metrics captured:
  - CPU_TIME: CPU time in seconds
  - LATENCY: Total execution time
  - CUMULATIVE_ROWS_SCANNED: Rows examined
  - SYSTEM_PEAK_BUFFER_MEMORY: Peak memory usage
  - SYSTEM_PEAK_TEMP_DIR_SIZE: Peak temp disk usage

Cost calculation matches local benchmark + R2 storage cost.

Usage:
    python3 run_duckdb_r2_benchmark.py --queries Q01 --configs medium --runs 3
    python3 run_duckdb_r2_benchmark.py --track-cold-start  # All 42 queries
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
# R2 CONFIGURATION
# ============================================================================

R2_BUCKET = os.environ.get("R2_BUCKET", "")
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BASE_PATH = f"s3://{R2_BUCKET}/data_source=fmp"

# Validate R2 credentials
if not R2_ENDPOINT or not R2_ACCESS_KEY or not R2_SECRET_KEY or not R2_BUCKET:
    import sys
    print("ERROR: R2 credentials not set. Please set environment variables:")
    print("  export R2_ENDPOINT=<your-r2-endpoint>")
    print("  export R2_ACCESS_KEY_ID=<your-access-key>")
    print("  export R2_SECRET_ACCESS_KEY=<your-secret-key>")
    print("  export R2_BUCKET=<your-bucket>")
    sys.exit(1)

# R2 Storage pricing (for cost calculation)
# R2 egress is FREE, but storage is $0.015/GB/month
# For benchmark, we calculate compute cost only (egress is free)
R2_EGRESS_COST_PER_GB = 0.0  # FREE!

# ============================================================================
# COST CALCULATION (same as local benchmark)
# ============================================================================

THREAD_RATE_PER_SEC = 0.00045
RAM_RATE_PER_GB_SEC = 0.000081
DISK_RATE_PER_GB_SEC = 0.0000079
CPU_RATE_PER_SEC = 0.00045
MINIMUM_COST = 0.01

# ============================================================================
# COMPUTE CONFIGURATIONS
# ============================================================================

CONFIGS = [
    {"name": "minimal", "threads": 1, "memory": "2GB", "disk": "5GB"},
    {"name": "small", "threads": 4, "memory": "8GB", "disk": "10GB"},
    {"name": "medium", "threads": 8, "memory": "16GB", "disk": "20GB"},
    {"name": "large", "threads": 16, "memory": "32GB", "disk": "50GB"},
    {"name": "maximum", "threads": 32, "memory": "64GB", "disk": "100GB"},
]

# ============================================================================
# QUERY CATEGORIES
# ============================================================================

QUERY_CATEGORIES = {
    "Q01": "scan", "Q02": "scan", "Q03": "scan", "Q04": "scan", "Q05": "scan", "Q06": "scan",
    "Q07": "aggregation", "Q08": "aggregation", "Q09": "aggregation", "Q10": "aggregation",
    "Q11": "aggregation", "Q12": "aggregation",
    "Q13": "window", "Q14": "window", "Q15": "window", "Q16": "window", "Q17": "window", "Q18": "window",
    "Q19": "join", "Q20": "join", "Q21": "join", "Q22": "join", "Q23": "join", "Q24": "join",
    "Q25": "join", "Q26": "join", "Q27": "join",
    "Q28": "analytics", "Q29": "analytics", "Q30": "analytics", "Q31": "analytics",
    "Q32": "analytics", "Q33": "analytics", "Q34": "analytics",
    "Q35": "time_series", "Q36": "time_series", "Q37": "time_series", "Q38": "time_series",
    "Q39": "data_quality", "Q40": "data_quality", "Q41": "data_quality", "Q42": "data_quality",
}

# ============================================================================
# QUERIES (all 42, same as local benchmark)
# ============================================================================

QUERIES = {
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
    "Q12": """SELECT p.sector, COUNT(DISTINCT e.symbol) as stocks,
                     AVG(e.close) as avg_close, SUM(e.volume) as total_volume,
                     AVG(p.marketCap) as avg_market_cap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1704067200
              GROUP BY p.sector ORDER BY total_volume DESC LIMIT 50""",
    "Q13": """SELECT symbol, dateEpoch, close,
                     FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch) as first_price,
                     close / NULLIF(FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) - 1 as total_return
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL') AND dateEpoch >= 1672531200
              ORDER BY symbol, dateEpoch DESC LIMIT 100""",
    "Q14": """SELECT symbol, dateEpoch, close, volume,
                     AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
                     AVG(volume) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_sma_20
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1704067200 ORDER BY symbol, dateEpoch""",
    "Q15": """SELECT symbol, dateEpoch, close,
                     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY close DESC) as price_rank
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1704067200
              QUALIFY price_rank <= 10
              ORDER BY symbol, price_rank""",
    "Q16": """SELECT symbol, dateEpoch, close,
                     LAG(close, 1) OVER (PARTITION BY symbol ORDER BY dateEpoch) as prev_close,
                     (close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                         / NULLIF(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet')
              WHERE dateEpoch >= 1672531200
              ORDER BY symbol, dateEpoch LIMIT 50000""",
    "Q17": """WITH daily_returns AS (
                  SELECT symbol, dateEpoch, close,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as daily_return
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              )
              SELECT symbol, dateEpoch, close, daily_return,
                     AVG(daily_return) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as rolling_avg_return,
                     STDDEV(daily_return) OVER (PARTITION BY symbol ORDER BY dateEpoch
                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as rolling_stddev
              FROM daily_returns""",
    "Q18": """SELECT p.sector,
                     (e.dateEpoch - (e.dateEpoch % 604800)) as week_epoch,
                     COUNT(DISTINCT e.symbol) as active_stocks,
                     AVG(e.close) as avg_close, SUM(e.volume) as total_volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1704067200
              GROUP BY p.sector, week_epoch ORDER BY week_epoch DESC, total_volume DESC LIMIT 1000""",
    "Q19": """SELECT e.symbol, e.dateEpoch, e.close, e.volume, p.sector, p.marketCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1704067200 AND p.marketCap > 100000000000
              ORDER BY e.dateEpoch DESC, p.marketCap DESC""",
    "Q20": """SELECT e.symbol, e.dateEpoch, e.close, p.sector, p.industry,
                     r.currentRatio, r.debtToEquityRatio
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON e.symbol = r.symbol
              WHERE e.dateEpoch >= 1704067200 AND p.sector = 'Technology'
              ORDER BY r.currentRatio DESC LIMIT 500""",
    "Q21": """SELECT e.symbol, MAX(e.close) as max_price, MIN(e.close) as min_price,
                     AVG(e.close) as avg_price, p.sector, p.industry, p.marketCap
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              WHERE e.dateEpoch >= 1704067200 AND p.sector IN ('Technology', 'Healthcare', 'Finance')
              GROUP BY e.symbol, p.sector, p.industry, p.marketCap
              ORDER BY p.marketCap DESC LIMIT 5000""",
    "Q22": """SELECT e.symbol, p.companyName, p.sector,
                     r.netProfitMargin, r.assetTurnover, r.currentRatio,
                     AVG(e.close) as avg_price, AVG(e.volume) as avg_volume
              FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON e.symbol = r.symbol
              WHERE e.dateEpoch >= 1704067200 AND r.netProfitMargin > 0.1 AND r.assetTurnover > 0.5
              GROUP BY e.symbol, p.companyName, p.sector, r.netProfitMargin, r.assetTurnover, r.currentRatio
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
    "Q24": """SELECT p.symbol, p.companyName, a.epsAvg, a.numAnalystsEps
              FROM read_parquet('{base_path}/company/profile/*.parquet') p
              JOIN read_parquet('{base_path}/analyst/analyst_estimates/*.parquet') a ON p.symbol = a.symbol
              WHERE a.numAnalystsEps > 10
              ORDER BY a.numAnalystsEps DESC LIMIT 1000""",
    "Q25": """SELECT c.symbol, c.name, c.sector, p.companyName as peer_name, p.marketCap as peer_mktCap
              FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') c
              JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON c.sector = p.sector
              WHERE p.marketCap > 10000000000
              ORDER BY c.symbol, p.marketCap DESC LIMIT 1000""",
    "Q26": """SELECT symbol, name, sector, dateFirstAdded
              FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet')
              WHERE dateFirstAdded IS NOT NULL
              ORDER BY dateFirstAdded DESC LIMIT 500""",
    "Q27": """WITH sector_stats AS (
                  SELECT p.sector, AVG(e.close) as avg_close, STDDEV(e.close) as stddev_close,
                         COUNT(DISTINCT e.symbol) as num_stocks
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
                  JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
                  WHERE e.dateEpoch >= 1704067200
                  GROUP BY p.sector
              )
              SELECT * FROM sector_stats WHERE num_stocks > 10 ORDER BY avg_close DESC""",
    "Q28": """WITH price_history AS (
                  SELECT symbol, dateEpoch, close,
                         AVG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch
                             ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) as sma_252
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1640995200
              )
              SELECT symbol, dateEpoch, close, sma_252,
                     (close / NULLIF(sma_252, 0) - 1) as distance_from_ma
              FROM price_history
              WHERE dateEpoch >= 1704067200
              ORDER BY ABS(close / NULLIF(sma_252, 0) - 1) DESC LIMIT 100""",
    "Q29": """WITH daily_rets AS (
                  SELECT symbol, dateEpoch,
                         (close - LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch))
                             / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY dateEpoch), 0) as ret
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') WHERE dateEpoch >= 1672531200
              ),
              vol_stats AS (
                  SELECT symbol, STDDEV(ret) * SQRT(252) as annualized_vol,
                         AVG(ret) * 252 as annualized_return, COUNT(*) as trading_days
                  FROM daily_rets WHERE ret IS NOT NULL
                  GROUP BY symbol HAVING COUNT(*) > 200
              )
              SELECT symbol, annualized_vol, annualized_return,
                     annualized_return / NULLIF(annualized_vol, 0) as sharpe_ratio
              FROM vol_stats ORDER BY sharpe_ratio DESC LIMIT 100""",
    "Q30": """SELECT c.symbol, c.name, c.sector,
                     COUNT(g.newGrade) as grade_changes,
                     SUM(CASE WHEN g.newGrade IN ('Buy', 'Strong Buy', 'Outperform') THEN 1 ELSE 0 END) as bullish_grades
              FROM read_parquet('{base_path}/indices/sp500_constituent/*.parquet') c
              LEFT JOIN read_parquet('{base_path}/analyst/stock_grade/*.parquet') g ON c.symbol = g.symbol
              GROUP BY c.symbol, c.name, c.sector
              ORDER BY grade_changes DESC LIMIT 50""",
    "Q31": """SELECT p.symbol, p.companyName, p.sector, p.marketCap,
                     r.priceToEarningsRatio, r.netProfitMargin, r.enterpriseValueMultiple, r.debtToEquityRatio
              FROM read_parquet('{base_path}/company/profile/*.parquet') p
              JOIN read_parquet('{base_path}/statements/financial_ratios/*.parquet') r ON p.symbol = r.symbol
              WHERE p.marketCap > 1000000000
                AND r.priceToEarningsRatio > 0 AND r.priceToEarningsRatio < 50
                AND r.netProfitMargin > 0
              ORDER BY r.priceToEarningsRatio LIMIT 100""",
    "Q32": """WITH sector_perf AS (
                  SELECT p.sector, e.dateEpoch,
                         AVG(e.close) as avg_close,
                         SUM(e.volume * e.close) as dollar_volume
                  FROM read_parquet('{base_path}/tick_data/eod/*.parquet') e
                  JOIN read_parquet('{base_path}/company/profile/*.parquet') p ON e.symbol = p.symbol
                  WHERE e.dateEpoch >= 1672531200
                  GROUP BY p.sector, e.dateEpoch
              )
              SELECT sector, dateEpoch, avg_close, dollar_volume,
                     LAG(avg_close) OVER (PARTITION BY sector ORDER BY dateEpoch) as prev_avg_close
              FROM sector_perf ORDER BY dateEpoch DESC, dollar_volume DESC LIMIT 100""",
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
    """Metrics extracted from DuckDB profiling."""
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

    latency_ms: float
    cpu_time_ms: float
    peak_memory_mb: float
    peak_disk_mb: float
    rows_scanned: int
    rows_returned: int
    calculated_cost: float

    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ============================================================================
# COST CALCULATION
# ============================================================================

def calculate_cost(threads, ram_mb, disk_mb, latency_sec, cpu_time_sec):
    """Calculate query cost matching TS/CR backend formula."""
    thread_cost = threads * latency_sec * THREAD_RATE_PER_SEC
    ram_cost = (ram_mb / 1024) * latency_sec * RAM_RATE_PER_GB_SEC
    disk_cost = (disk_mb / 1024) * latency_sec * DISK_RATE_PER_GB_SEC
    cpu_cost = cpu_time_sec * CPU_RATE_PER_SEC
    total = thread_cost + ram_cost + disk_cost + cpu_cost
    return max(MINIMUM_COST, round(total, 9))


def _parse_size_to_mb(size_str: str) -> int:
    """Parse size string like '16GB' to MB."""
    size_str = size_str.upper().strip()
    try:
        if size_str.endswith("GB"):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith("MB"):
            return int(float(size_str[:-2]))
        return int(size_str)
    except:
        return 0


# ============================================================================
# DUCKDB CONNECTION & PROFILING
# ============================================================================

def create_connection(threads: int, memory: str, disk: str, temp_dir: str):
    """Create DuckDB connection configured for R2 with profiling."""
    conn = duckdb.connect(":memory:")

    # Resource limits
    conn.execute(f"SET threads = {threads}")
    conn.execute(f"SET memory_limit = '{memory}'")
    conn.execute(f"SET max_temp_directory_size = '{disk}'")
    conn.execute(f"SET temp_directory = '{temp_dir}'")

    # R2 configuration
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint = '{R2_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id = '{R2_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key = '{R2_SECRET_KEY}';")
    conn.execute("SET s3_region = 'auto';")
    conn.execute("SET s3_url_style = 'path';")

    # Performance settings
    conn.execute("SET enable_object_cache = true")
    conn.execute("SET preserve_insertion_order = false")

    return conn


def run_query_with_profiling(conn, sql: str, profile_file: str) -> tuple[list, ProfilingMetrics]:
    """Execute query with profiling and return metrics."""
    metrics = ProfilingMetrics()

    # Set profiling output
    conn.execute(f"SET profiling_output = '{profile_file}'")
    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_mode = 'detailed'")

    # Execute
    start_time = time.perf_counter()
    result = conn.execute(sql).fetchall()
    wall_time = time.perf_counter() - start_time
    metrics.rows_returned = len(result)

    # Disable profiling
    conn.execute("SET enable_profiling = 'no_output'")

    # Parse profiling output
    try:
        with open(profile_file, 'r') as f:
            json_output = f.read()

        json_start = json_output.find('{')
        if json_start >= 0:
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

            profile_data = json.loads(json_output[json_start:json_end])
            metrics.cpu_time_seconds = profile_data.get("cpu_time", 0.0)
            metrics.latency_seconds = profile_data.get("latency", wall_time)
            metrics.cumulative_rows_scanned = profile_data.get("cumulative_rows_scanned", 0)
            metrics.peak_memory_bytes = profile_data.get("system_peak_buffer_memory", 0)
            metrics.peak_disk_bytes = profile_data.get("system_peak_temp_dir_size", 0)
        else:
            metrics.latency_seconds = wall_time
    except:
        metrics.latency_seconds = wall_time

    return result, metrics


def run_query(conn, query_id: str, sql: str, config: dict, run_number: int,
              is_cold_start: bool, temp_dir: str) -> BenchmarkResult:
    """Execute a single query and capture metrics."""
    profile_file = os.path.join(temp_dir, f"profile_{query_id}_{run_number}_{int(time.time()*1000)}.json")
    query_category = QUERY_CATEGORIES.get(query_id, "unknown")
    sql = sql.format(base_path=R2_BASE_PATH)

    try:
        result, metrics = run_query_with_profiling(conn, sql, profile_file)

        ram_mb = _parse_size_to_mb(config["memory"])
        disk_mb = _parse_size_to_mb(config["disk"])

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


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="DuckDB R2 Benchmark with Full Profiling")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated query IDs (default: all 42)")
    parser.add_argument("--configs", type=str, default=None,
                        help="Comma-separated configs (default: small,medium,large,maximum)")
    parser.add_argument("--runs", type=int, default=3, help="Number of measured runs")
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs")
    parser.add_argument("--track-cold-start", action="store_true", help="Track cold start")
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    args = parser.parse_args()

    # Parse queries
    if args.queries:
        query_ids = [q.strip().upper() for q in args.queries.split(",")]
        queries_to_run = {qid: QUERIES[qid] for qid in query_ids if qid in QUERIES}
    else:
        queries_to_run = QUERIES

    # Parse configs (skip minimal by default - often OOM)
    if args.configs:
        config_names = [c.strip().lower() for c in args.configs.split(",")]
        configs = [c for c in CONFIGS if c["name"] in config_names]
    else:
        configs = [c for c in CONFIGS if c["name"] != "minimal"]

    print("=" * 60)
    print("DuckDB R2 BENCHMARK - Full Profiling")
    print("=" * 60)
    print(f"R2 Bucket: {R2_BUCKET}")
    print(f"Queries: {len(queries_to_run)} ({', '.join(list(queries_to_run.keys())[:5])}...)")
    print(f"Configs: {[c['name'] for c in configs]}")
    print(f"Runs: {args.runs} (+ {args.warmup} warmup)")
    print(f"Cold start: {args.track_cold_start}")
    print(f"R2 Egress Cost: FREE")
    print()

    all_results = []
    temp_dir = tempfile.mkdtemp(prefix="duckdb_r2_benchmark_")

    for config in configs:
        print(f"\n{'='*60}")
        print(f"Config: {config['name']} ({config['threads']} threads, {config['memory']})")
        print(f"{'='*60}")

        conn = create_connection(config["threads"], config["memory"], config["disk"], temp_dir)

        for query_id, sql in queries_to_run.items():
            print(f"\n  Query {query_id} ({QUERY_CATEGORIES.get(query_id, 'unknown')}):")

            # Cold start (fresh connection)
            if args.track_cold_start:
                cold_conn = create_connection(config["threads"], config["memory"], config["disk"], temp_dir)
                print(f"    Cold start...", end=" ", flush=True)
                result = run_query(cold_conn, query_id, sql, config, 0, True, temp_dir)
                if result.error:
                    print(f"ERROR: {result.error[:60]}")
                else:
                    print(f"{result.latency_ms:.1f}ms | CPU: {result.cpu_time_ms:.1f}ms | Mem: {result.peak_memory_mb:.1f}MB")
                all_results.append(result)
                cold_conn.close()

            # Warmup
            for i in range(args.warmup):
                print(f"    Warmup {i+1}...", end=" ", flush=True)
                result = run_query(conn, query_id, sql, config, -(args.warmup - i), False, temp_dir)
                if result.error:
                    print(f"ERROR: {result.error[:60]}")
                else:
                    print(f"{result.latency_ms:.1f}ms")

            # Measured runs
            latencies = []
            for i in range(args.runs):
                print(f"    Run {i+1}...", end=" ", flush=True)
                result = run_query(conn, query_id, sql, config, i + 1, False, temp_dir)
                if result.error:
                    print(f"ERROR: {result.error[:60]}")
                else:
                    print(f"{result.latency_ms:.1f}ms | CPU: {result.cpu_time_ms:.1f}ms")
                    latencies.append(result.latency_ms)
                all_results.append(result)

            if latencies:
                avg = sum(latencies) / len(latencies)
                print(f"    Avg: {avg:.1f}ms")

        conn.close()

    # Save results
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_file = args.output_dir / f"duckdb_r2_benchmark_{timestamp}.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_id", "query_category", "config", "threads", "memory", "disk", "run_number", "is_cold_start",
            "latency_ms", "cpu_time_ms", "peak_memory_mb", "peak_disk_mb",
            "rows_scanned", "rows_returned", "calculated_cost", "error", "timestamp"
        ])
        for r in all_results:
            writer.writerow([
                r.query_id, r.query_category, r.config_name, r.threads, r.memory_limit, r.disk_limit,
                r.run_number, r.is_cold_start, r.latency_ms, r.cpu_time_ms,
                r.peak_memory_mb, r.peak_disk_mb, r.rows_scanned, r.rows_returned,
                r.calculated_cost, r.error, r.timestamp
            ])
    print(f"\nResults saved to: {csv_file}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    successful = [r for r in all_results if not r.error and not r.is_cold_start]
    if successful:
        total_queries = len(set(r.query_id for r in successful))
        total_configs = len(set(r.config_name for r in successful))
        print(f"Successful: {len(successful)} runs ({total_queries} queries × {total_configs} configs)")
    failed = [r for r in all_results if r.error]
    if failed:
        print(f"Failed: {len(failed)} runs")
        for f in failed[:5]:
            print(f"  {f.query_id} ({f.config_name}): {f.error[:50]}")


if __name__ == "__main__":
    main()
