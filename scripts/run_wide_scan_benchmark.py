#!/usr/bin/env python3
"""
Wide Scan Benchmark Runner - Q43-Q57.

Runs the additional wide-scan queries on all platforms to capture
more realistic data scanning costs.

Usage:
    # Run all platforms
    python run_wide_scan_benchmark.py --platforms all

    # Run specific platform
    python run_wide_scan_benchmark.py --platforms bigquery
    python run_wide_scan_benchmark.py --platforms athena
    python run_wide_scan_benchmark.py --platforms duckdb_local
    python run_wide_scan_benchmark.py --platforms duckdb_r2

    # Dry run (show queries without executing)
    python run_wide_scan_benchmark.py --dry-run
"""

import argparse
import csv
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Import wide scan queries
from queries_wide_scan import WIDE_SCAN_QUERIES, BIGQUERY_OVERRIDES, ATHENA_OVERRIDES

# Configuration
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Platform configs
ATHENA_DATABASE = "ceta_benchmark"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = "s3://ceta-benchmark-eu/athena-results/"
ATHENA_REGION = "eu-west-1"

BIGQUERY_PROJECT = os.environ.get("GCP_PROJECT", "project-8521de6f-e81e-4971-b58")
BIGQUERY_DATASET = "ceta_benchmark"

# DuckDB configs
DUCKDB_LOCAL_DATA = "/opt/insydia/data"
DUCKDB_R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "https://e6e4d5e3ad12aa7f4f5f5d5f5e5f5e5f.r2.cloudflarestorage.com")

# Cost constants
ATHENA_COST_PER_TB = 5.0
BIGQUERY_COST_PER_TB = 5.0
MINIMUM_COST = 0.01
CPU_RATE_PER_SEC = 0.00045


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    platform: str
    query_id: str
    query_category: str
    run_number: int
    is_cold_start: bool
    latency_ms: float
    execution_time_ms: float = 0
    bytes_scanned: int = 0
    bytes_billed: int = 0
    rows_returned: int = 0
    cpu_time_ms: float = 0
    cache_hit: Optional[bool] = None
    slot_ms: int = 0
    calculated_cost: float = 0.0
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def run_bigquery_query(query_sql: str, query_id: str, category: str, run_number: int, is_cold: bool) -> BenchmarkResult:
    """Run a single query on BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=BIGQUERY_PROJECT)
    table_prefix = f"`{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}`."
    sql = query_sql.format(table_prefix=table_prefix)

    start = time.perf_counter()
    try:
        job = client.query(sql)
        results = list(job.result())
        latency_ms = (time.perf_counter() - start) * 1000

        # Extract metrics
        bytes_billed = job.total_bytes_billed or 0
        bytes_scanned = job.total_bytes_processed or 0
        slot_ms = job.slot_millis or 0
        cache_hit = job.cache_hit

        # Cost calculation: $5 per TB billed
        cost = max(MINIMUM_COST, (bytes_billed / (1024**4)) * BIGQUERY_COST_PER_TB)

        return BenchmarkResult(
            platform="bigquery",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=latency_ms,
            execution_time_ms=slot_ms,
            bytes_scanned=bytes_scanned,
            bytes_billed=bytes_billed,
            rows_returned=len(results),
            cache_hit=cache_hit,
            slot_ms=slot_ms,
            calculated_cost=cost,
        )
    except Exception as e:
        return BenchmarkResult(
            platform="bigquery",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=(time.perf_counter() - start) * 1000,
            error=str(e),
        )


def run_athena_query(query_sql: str, query_id: str, category: str, run_number: int, is_cold: bool) -> BenchmarkResult:
    """Run a single query on Athena."""
    import boto3

    client = boto3.client('athena', region_name=ATHENA_REGION)
    table_prefix = ""  # Athena uses database prefix in FROM
    sql = query_sql.format(table_prefix=table_prefix)

    start = time.perf_counter()
    try:
        response = client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        execution_id = response['QueryExecutionId']

        # Poll for completion
        while True:
            status = client.get_query_execution(QueryExecutionId=execution_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.1)

        latency_ms = (time.perf_counter() - start) * 1000

        if state != 'SUCCEEDED':
            error_msg = status['QueryExecution']['Status'].get('StateChangeReason', state)
            return BenchmarkResult(
                platform="athena",
                query_id=query_id,
                query_category=category,
                run_number=run_number,
                is_cold_start=is_cold,
                latency_ms=latency_ms,
                error=error_msg,
            )

        # Get metrics
        stats = status['QueryExecution'].get('Statistics', {})
        bytes_scanned = stats.get('DataScannedInBytes', 0)
        exec_time_ms = stats.get('EngineExecutionTimeInMillis', 0)

        # Get row count
        result_response = client.get_query_results(QueryExecutionId=execution_id, MaxResults=1)
        rows_returned = result_response.get('ResultSet', {}).get('ResultSetMetadata', {})

        # Cost: $5 per TB scanned
        cost = max(0, (bytes_scanned / (1024**4)) * ATHENA_COST_PER_TB)

        return BenchmarkResult(
            platform="athena",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=latency_ms,
            execution_time_ms=exec_time_ms,
            bytes_scanned=bytes_scanned,
            bytes_billed=bytes_scanned,  # Athena bills on scanned
            calculated_cost=cost,
        )
    except Exception as e:
        return BenchmarkResult(
            platform="athena",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=(time.perf_counter() - start) * 1000,
            error=str(e),
        )


def run_duckdb_query(query_sql: str, query_id: str, category: str, run_number: int, is_cold: bool,
                     data_path: str, config: dict) -> BenchmarkResult:
    """Run a single query on DuckDB."""
    import duckdb

    # Create connection with config
    conn = duckdb.connect(":memory:")
    conn.execute(f"SET threads = {config['threads']}")
    conn.execute(f"SET memory_limit = '{config['memory']}'")
    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_output = '/tmp/duckdb_profile.json'")

    # Set up table views
    table_prefix = ""
    for table_name, path in [
        ("stock_eod", f"{data_path}/data_source=fmp/tick_data/eod/*.parquet"),
        ("profile", f"{data_path}/data_source=fmp/company/profile/*.parquet"),
        ("sp500_constituent", f"{data_path}/data_source=fmp/indices/sp500_constituent/*.parquet"),
    ]:
        conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{path}')")

    sql = query_sql.format(table_prefix=table_prefix)

    start = time.perf_counter()
    try:
        result = conn.execute(sql)
        rows = result.fetchall()
        latency_ms = (time.perf_counter() - start) * 1000

        # Get profiling info
        try:
            with open('/tmp/duckdb_profile.json', 'r') as f:
                profile = __import__('json').load(f)
            cpu_time_ms = profile.get('cpu_time', 0) * 1000
        except:
            cpu_time_ms = latency_ms  # Fallback

        # Cost calculation based on CPU time
        cost = max(MINIMUM_COST, (cpu_time_ms / 1000) * CPU_RATE_PER_SEC)

        conn.close()
        return BenchmarkResult(
            platform="duckdb_local",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=latency_ms,
            cpu_time_ms=cpu_time_ms,
            rows_returned=len(rows),
            calculated_cost=cost,
        )
    except Exception as e:
        conn.close()
        return BenchmarkResult(
            platform="duckdb_local",
            query_id=query_id,
            query_category=category,
            run_number=run_number,
            is_cold_start=is_cold,
            latency_ms=(time.perf_counter() - start) * 1000,
            error=str(e),
        )


def get_queries_for_platform(platform: str):
    """Get queries with platform-specific overrides."""
    queries = list(WIDE_SCAN_QUERIES)

    if platform == "bigquery":
        for i, q in enumerate(queries):
            if q.id in BIGQUERY_OVERRIDES:
                queries[i] = BIGQUERY_OVERRIDES[q.id]
    elif platform == "athena":
        for i, q in enumerate(queries):
            if q.id in ATHENA_OVERRIDES:
                queries[i] = ATHENA_OVERRIDES[q.id]

    return queries


def run_benchmark(platform: str, runs: int = 4, dry_run: bool = False):
    """Run benchmark for a platform."""
    queries = get_queries_for_platform(platform)
    results = []

    print(f"\n{'='*60}")
    print(f"Running Wide Scan Benchmark on {platform.upper()}")
    print(f"Queries: {len(queries)} | Runs per query: {runs}")
    print(f"{'='*60}\n")

    for q in queries:
        print(f"  {q.id}: {q.name}")

        if dry_run:
            print(f"    SQL: {q.sql_template[:100]}...")
            continue

        for run_num in range(runs):
            is_cold = (run_num == 0)

            if platform == "bigquery":
                result = run_bigquery_query(q.sql_template, q.id, q.category, run_num, is_cold)
            elif platform == "athena":
                result = run_athena_query(q.sql_template, q.id, q.category, run_num, is_cold)
            elif platform == "duckdb_local":
                config = {"threads": 4, "memory": "8GB"}
                result = run_duckdb_query(q.sql_template, q.id, q.category, run_num, is_cold,
                                         DUCKDB_LOCAL_DATA, config)
            else:
                print(f"    Unknown platform: {platform}")
                continue

            results.append(result)

            status = "OK" if not result.error else f"ERR: {result.error[:30]}"
            print(f"    Run {run_num}: {result.latency_ms:.0f}ms, ${result.calculated_cost:.6f} [{status}]")

    return results


def save_results(results: list, platform: str):
    """Save results to CSV."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = RESULTS_DIR / f"wide_scan_{platform}_{timestamp}.csv"

    with open(filename, 'w', newline='') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=asdict(results[0]).keys())
            writer.writeheader()
            for r in results:
                writer.writerow(asdict(r))

    print(f"\nSaved {len(results)} results to {filename}")
    return filename


def main():
    parser = argparse.ArgumentParser(description="Run wide scan benchmark")
    parser.add_argument("--platforms", nargs="+",
                       choices=["all", "bigquery", "athena", "duckdb_local", "duckdb_r2"],
                       default=["bigquery", "athena"],
                       help="Platforms to benchmark")
    parser.add_argument("--runs", type=int, default=4, help="Runs per query")
    parser.add_argument("--dry-run", action="store_true", help="Show queries without executing")
    args = parser.parse_args()

    platforms = args.platforms
    if "all" in platforms:
        platforms = ["bigquery", "athena", "duckdb_local"]

    all_results = {}
    for platform in platforms:
        results = run_benchmark(platform, args.runs, args.dry_run)
        if results and not args.dry_run:
            filename = save_results(results, platform)
            all_results[platform] = results

    # Summary
    if all_results:
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        for platform, results in all_results.items():
            valid = [r for r in results if not r.error]
            total_cost = sum(r.calculated_cost for r in valid)
            avg_cost = total_cost / len(valid) if valid else 0
            avg_latency = sum(r.latency_ms for r in valid) / len(valid) if valid else 0
            print(f"\n{platform}:")
            print(f"  Queries: {len(valid)}/{len(results)}")
            print(f"  Total cost: ${total_cost:.4f}")
            print(f"  Avg cost/query: ${avg_cost:.6f}")
            print(f"  Avg latency: {avg_latency:.0f}ms")


if __name__ == "__main__":
    main()
