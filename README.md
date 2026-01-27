# Cloud Data Platform Benchmark: DuckDB vs BigQuery vs Athena

Benchmark comparing analytical query performance across DuckDB, BigQuery, and Athena on ~20GB of financial time-series data in Parquet format.

## Results Summary

| Platform | Avg Cost/Query | Warm Median | Data Scanned |
|----------|----------------|-------------|--------------|
| Athena | $0.0064 | 4,211 ms | 277 GB |
| DuckDB (local SSD) | - | 505 ms | - |
| BigQuery | $0.0282 | 2,775 ms | 1,140 GB |
| DuckDB + R2 | - | 881 ms | - |

**Key findings:**
- DuckDB is 5-8x faster than cloud platforms
- BigQuery scans 4x more data than Athena (same $5/TB pricing)
- Wide scans (SELECT *) eliminate cost differences between cloud platforms
- DuckDB + remote storage has significant cold start overhead (~1,935%)
- R2 offers 35% cheaper storage with zero egress fees

## Dataset

- **Size:** ~20GB
- **Format:** Parquet, ZSTD compressed
- **Partitioning:** None
- **Sort order:** `(symbol, dateEpoch)` for time-series tables

**Tables:**
```
stock_eod          → tick_data/eod/*                 (sorted: symbol, dateEpoch)
financial_ratios   → statements/financial_ratios/*   (sorted: symbol, period, dateEpoch)
income_statement   → statements/income_statement/*   (sorted: symbol, period, dateEpoch)
balance_sheet      → statements/balance_sheet/*      (sorted: symbol, period, dateEpoch)
profile            → company/profile/*               (sorted: symbol)
sp500_constituent  → indices/sp500_constituent/*     (sorted: symbol)
```

**Locations:**

| Platform | Data Location | Compute Location |
|----------|---------------|------------------|
| BigQuery | europe-west1 (Belgium) | europe-west1 |
| Athena | S3 eu-west-1 (Ireland) | eu-west-1 |
| DuckDB + R2 | Cloudflare R2 (EU) | Lauterbourg, FR |
| DuckDB Local | Local SSD | Lauterbourg, FR |

**DuckDB configurations:**
- Minimal: 1 thread, 2GB RAM, 5GB temp (disk-spill)
- Small: 4 threads, 8GB RAM, 10GB temp (disk-spill)
- Medium: 8 threads, 16GB RAM, 20GB temp (disk-spill)
- Large: 16 threads, 32GB RAM, 50GB temp (disk-spill)
- XL: 32 threads, 64GB RAM, 100GB temp (disk-spill)

## Queries

57 queries across categories:
- Table scans
- Aggregations
- Window functions
- Joins
- Wide scans (SELECT *)

See `scripts/queries.py` for full query definitions.

## Structure

```
benchmark-duckdb-athena-bq/
├── README.md
├── scripts/
│   ├── queries.py                    # Core query definitions (Q01-Q42)
│   ├── queries_platform.py           # Platform-specific SQL overrides
│   ├── queries_wide_scan.py          # Wide scan queries (Q43-Q57)
│   ├── run_duckdb_local_benchmark.py # DuckDB local SSD runner
│   ├── run_duckdb_r2_benchmark.py    # DuckDB + Cloudflare R2 runner
│   ├── run_cloud_benchmark.py        # BigQuery & Athena runner
│   ├── run_wide_scan_benchmark.py    # Wide scan benchmark runner
│   ├── analyze_all_results.py        # Result analysis script
│   ├── consolidate_results.py        # Combine all results
│   ├── setup_athena.sql              # Athena table DDL
│   └── setup_bigquery.sql            # BigQuery table DDL
└── results/
    ├── duckdb_local_benchmark_20260125_162021.csv  # 672 runs
    ├── duckdb_r2_benchmark_20260125_181354.csv     # 672 runs
    ├── cloud_benchmark_20260125_205503.csv         # 168 BigQuery runs
    ├── athena_benchmark_20260126_complete.csv      # 168 Athena runs
    ├── wide_scan_bigquery_20260126_110850.csv      # 40 wide scan runs
    └── wide_scan_athena_20260126_111401.csv        # 44 wide scan runs
```

## Running the Benchmarks

### Prerequisites

```bash
pip install duckdb pandas google-cloud-bigquery boto3
```

### DuckDB Local

```bash
python scripts/run_duckdb_local_benchmark.py
```

### DuckDB + R2

Requires R2 credentials in environment:
```bash
export R2_ENDPOINT=<your-account-id>.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export R2_BUCKET=your-bucket-name  # optional, defaults to ts-data-backups

python scripts/run_duckdb_r2_benchmark.py
```

### BigQuery

Requires GCP credentials and table setup:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Create tables (one-time setup)
# Run SQL from scripts/setup_bigquery.sql in BigQuery console

python scripts/run_cloud_benchmark.py --platform bigquery
```

### Athena

Requires AWS credentials and table setup:
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=eu-west-1

# Create tables (one-time setup)
# Run SQL from scripts/setup_athena.sql in Athena console

python scripts/run_cloud_benchmark.py --platform athena
```

## Known Limitations

- **BigQuery wide scan queries:** Some wide scan queries (Q43+) reference columns not present in the BigQuery schema (e.g., `changePercent`). These queries error on BigQuery but run successfully on Athena.
- **DuckDB + R2 cold start:** First query takes 20-30 seconds due to Parquet metadata fetching over the network.

## Storage Costs

| Provider | Storage ($/GB/mo) | Egress ($/GB) |
|----------|-------------------|---------------|
| AWS S3 | $0.023 | $0.09 |
| Google GCS | $0.020 | $0.12 |
| Cloudflare R2 | $0.015 | $0.00 |

R2 is 35% cheaper than S3 with zero egress fees.

## License

MIT

---

*Benchmark: January 2026*
