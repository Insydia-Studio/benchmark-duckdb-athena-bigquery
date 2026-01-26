-- BigQuery External Table Setup for Benchmark
-- Run with: bq query --use_legacy_sql=false < setup_bigquery.sql

-- Create dataset
CREATE SCHEMA IF NOT EXISTS `ceta_benchmark`
OPTIONS(
  location="europe-west1",
  description="Cloud platform benchmark dataset"
);

-- stock_eod - EOD stock prices
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.stock_eod`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/tick_data/eod/*.parquet'],
  hive_partition_uri_prefix = 'gs://ceta-benchmark/data_source=fmp/tick_data/eod',
  require_hive_partition_filter = false
);

-- stock_crypto - Crypto 1-min data
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.stock_crypto`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/tick_data/crypto/granularity=1min/*.parquet'],
  hive_partition_uri_prefix = 'gs://ceta-benchmark/data_source=fmp/tick_data/crypto',
  require_hive_partition_filter = false
);

-- financial_ratios - Financial ratios
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.financial_ratios`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/statements/ratios/*.parquet']
);

-- income_statement - Income statements
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.income_statement`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/statements/income/*.parquet']
);

-- balance_sheet - Balance sheets
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.balance_sheet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/statements/balance/*.parquet']
);

-- cashflow_statement - Cash flow statements
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.cashflow_statement`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/statements/cashflow/*.parquet']
);

-- profile - Company profiles
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.profile`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/company/profile/*.parquet']
);

-- stock_peers - Stock peer data
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.stock_peers`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/company/stock_peers/*.parquet']
);

-- sp500_constituent - S&P 500 constituents
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.sp500_constituent`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/indices/sp500_constituent/*.parquet']
);

-- nasdaq_constituent - NASDAQ constituents
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.nasdaq_constituent`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/indices/nasdaq_constituent/*.parquet']
);

-- analyst_estimates - Analyst estimates
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.analyst_estimates`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/analyst/analyst_estimates/*.parquet']
);

-- stock_grade - Stock grades
CREATE OR REPLACE EXTERNAL TABLE `ceta_benchmark.stock_grade`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ceta-benchmark/data_source=fmp/analyst/stock_grade/*.parquet']
);

-- Verify table creation
SELECT table_name, table_type
FROM `ceta_benchmark.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;
