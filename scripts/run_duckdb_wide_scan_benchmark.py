#!/usr/bin/env python3
"""
DuckDB Wide Scan Benchmark (Q43-Q57).

Runs wide scan queries on DuckDB with both local and R2 storage backends.
These queries simulate data exports, ML feature extraction, and full table scans.

Usage:
    # Local SSD
    python3 run_duckdb_wide_scan_benchmark.py --backend local --local-data-path /opt/insydia/data

    # Cloudflare R2
    python3 run_duckdb_wide_scan_benchmark.py --backend r2

    # Both backends
    python3 run_duckdb_wide_scan_benchmark.py --backend both --configs small
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

# Import wide scan queries
from queries_wide_scan import get_queries, WIDE_SCAN_QUERIES

# ============================================================================
# CONFIGURATION
# ============================================================================

# Local data path
DEFAULT_LOCAL_DATA_PATH = "/opt/insydia/data"
DATA_PREFIX = "data_source=fmp"

# R2 configuration (from environment)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")
R2_DATA_PREFIX = "data_source=fmp"

# Cost calculation constants
THREAD_RATE_PER_SEC = 0.00045
RAM_RATE_PER_GB_SEC = 0.000081
DISK_RATE_PER_GB_SEC = 0.0000079
CPU_RATE_PER_SEC = 0.00045
MINIMUM_COST = 0.01

# Compute configurations
CONFIGS = [
    {"name": "minimal", "threads": 1, "memory": "2GB", "disk": "5GB"},
    {"name": "small", "threads": 4, "memory": "8GB", "disk": "10GB"},
    {"name": "medium", "threads": 8, "memory": "16GB", "disk": "20GB"},
    {"name": "large", "threads": 16, "memory": "32GB", "disk": "50GB"},
    {"name": "maximum", "threads": 32, "memory": "64GB", "disk": "100GB"},
]

# Table path mappings for DuckDB read_parquet()
TABLE_PATHS = {
    "stock_eod": "tick_data/eod/*.parquet",
    "financial_ratios": "statements/financial_ratios/*.parquet",
    "income_statement": "statements/income_statement/*.parquet",
    "balance_sheet": "statements/balance_sheet/*.parquet",
    "profile": "company/profile/*.parquet",
    "sp500_constituent": "indices/sp500_constituent/*.parquet",
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    platform: str  # "duckdb_local" or "duckdb_r2"
    query_id: str
    query_category: str
    config_name: str
    threads: int
    memory_limit: str
    run_number: int
    is_cold_start: bool
    latency_ms: float
    cpu_time_ms: float
    peak_memory_mb: float
    rows_returned: int
    calculated_cost: float
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ============================================================================
# SQL TRANSFORMATION
# ============================================================================

def transform_sql_for_duckdb(sql_template: str, base_path: str, use_r2: bool = False) -> str:
    """
    Transform wide scan SQL template to DuckDB read_parquet() syntax.

    Input:  SELECT * FROM {table_prefix}stock_eod WHERE ...
    Output: SELECT * FROM read_parquet('/path/tick_data/eod/*.parquet') WHERE ...
    """
    sql = sql_template

    # Replace table references with read_parquet() calls
    for table_name, table_path in TABLE_PATHS.items():
        placeholder = f"{{table_prefix}}{table_name}"
        if use_r2:
            # R2 path format
            parquet_path = f"s3://{R2_BUCKET}/{R2_DATA_PREFIX}/{table_path}"
        else:
            # Local path format
            parquet_path = f"{base_path}/{DATA_PREFIX}/{table_path}"

        replacement = f"read_parquet('{parquet_path}')"
        sql = sql.replace(placeholder, replacement)

    # Also handle cases where table_prefix might be empty or different
    sql = sql.replace("{table_prefix}", "")

    return sql.strip()


# ============================================================================
# COST CALCULATION
# ============================================================================

def calculate_cost(
    threads: int,
    ram_mb: int,
    latency_sec: float,
    cpu_time_sec: float,
) -> float:
    """Calculate query cost using TS/CR backend formula."""
    thread_cost = threads * latency_sec * THREAD_RATE_PER_SEC
    ram_cost = (ram_mb / 1024) * latency_sec * RAM_RATE_PER_GB_SEC
    cpu_cost = cpu_time_sec * CPU_RATE_PER_SEC

    total = thread_cost + ram_cost + cpu_cost
    return max(MINIMUM_COST, round(total, 6))


def parse_size_to_mb(size_str: str) -> int:
    """Parse size string like '16GB' to MB."""
    size_str = size_str.strip().upper()
    if size_str.endswith("GB"):
        return int(float(size_str[:-2]) * 1024)
    elif size_str.endswith("MB"):
        return int(float(size_str[:-2]))
    return int(size_str)


# ============================================================================
# DUCKDB CONNECTION
# ============================================================================

def create_connection(
    threads: int,
    memory_limit: str,
    disk_limit: str,
    temp_dir: str,
    use_r2: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with optional R2 configuration."""
    conn = duckdb.connect(":memory:")

    # Resource limits
    conn.execute(f"SET threads = {threads}")
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET max_temp_directory_size = '{disk_limit}'")
    conn.execute(f"SET temp_directory = '{temp_dir}'")

    # Profiling
    conn.execute("SET enable_profiling = 'no_output'")
    conn.execute("SET profiling_mode = 'detailed'")

    # Performance settings
    conn.execute("SET enable_object_cache = true")
    conn.execute("SET preserve_insertion_order = false")

    # R2/S3 configuration
    if use_r2:
        if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET]):
            raise ValueError("R2 settings not set. Set R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET")

        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute(f"SET s3_endpoint = '{R2_ENDPOINT}'")
        conn.execute(f"SET s3_access_key_id = '{R2_ACCESS_KEY_ID}'")
        conn.execute(f"SET s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}'")
        conn.execute("SET s3_region = 'auto'")
        conn.execute("SET s3_url_style = 'path'")

    return conn


# ============================================================================
# QUERY EXECUTION
# ============================================================================

def run_query_with_profiling(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    temp_dir: str,
    query_id: str,
    run_number: int,
) -> tuple[int, float, float, float, str]:
    """
    Execute query and extract profiling metrics.

    Returns: (rows_returned, latency_sec, cpu_time_sec, peak_memory_mb, error)
    """
    profile_file = os.path.join(temp_dir, f"profile_{query_id}_{run_number}_{int(time.time()*1000)}.json")

    try:
        # Set profiling output
        conn.execute(f"SET profiling_output = '{profile_file}'")
        conn.execute("SET enable_profiling = 'json'")

        # Execute query
        start_time = time.perf_counter()
        result = conn.execute(sql).fetchall()
        wall_time = time.perf_counter() - start_time

        rows_returned = len(result)
        cpu_time = 0.0
        peak_memory = 0.0

        # Disable profiling
        conn.execute("SET enable_profiling = 'no_output'")

        # Parse profiling output
        try:
            with open(profile_file, 'r') as f:
                content = f.read()

            # Find JSON object
            json_start = content.find('{')
            if json_start >= 0:
                brace_count = 0
                json_end = json_start
                for i, c in enumerate(content[json_start:], json_start):
                    if c == '{':
                        brace_count += 1
                    elif c == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            json_end = i + 1
                            break

                profile_data = json.loads(content[json_start:json_end])
                cpu_time = profile_data.get("cpu_time", 0.0)
                peak_memory = profile_data.get("system_peak_buffer_memory", 0) / (1024 * 1024)
        except (json.JSONDecodeError, FileNotFoundError, IOError):
            pass

        # Clean up
        try:
            os.remove(profile_file)
        except OSError:
            pass

        return rows_returned, wall_time, cpu_time, peak_memory, ""

    except Exception as e:
        return 0, 0.0, 0.0, 0.0, str(e)


def run_single_query(
    conn: duckdb.DuckDBPyConnection,
    platform: str,
    query_id: str,
    sql: str,
    config: dict,
    run_number: int,
    is_cold_start: bool,
    temp_dir: str,
) -> BenchmarkResult:
    """Execute a single query and return result."""

    rows, latency_sec, cpu_time_sec, peak_memory_mb, error = run_query_with_profiling(
        conn, sql, temp_dir, query_id, run_number
    )

    ram_mb = parse_size_to_mb(config["memory"])
    cost = calculate_cost(config["threads"], ram_mb, latency_sec, cpu_time_sec) if not error else MINIMUM_COST

    return BenchmarkResult(
        platform=platform,
        query_id=query_id,
        query_category="wide_scan",
        config_name=config["name"],
        threads=config["threads"],
        memory_limit=config["memory"],
        run_number=run_number,
        is_cold_start=is_cold_start,
        latency_ms=latency_sec * 1000,
        cpu_time_ms=cpu_time_sec * 1000,
        peak_memory_mb=peak_memory_mb,
        rows_returned=rows,
        calculated_cost=cost,
        error=error,
    )


# ============================================================================
# BENCHMARK RUNNER
# ============================================================================

def run_benchmark(
    backend: str,  # "local", "r2", or "both"
    local_data_path: str,
    configs: list,
    runs: int,
    track_cold_start: bool,
) -> list[BenchmarkResult]:
    """Run wide scan benchmark on specified backend(s)."""

    all_results = []
    temp_dir = tempfile.mkdtemp(prefix="duckdb_wide_scan_")

    # Get wide scan queries
    queries = get_queries("duckdb")

    backends_to_run = []
    if backend in ["local", "both"]:
        backends_to_run.append(("duckdb_local", False, local_data_path))
    if backend in ["r2", "both"]:
        backends_to_run.append(("duckdb_r2", True, ""))

    for platform, use_r2, base_path in backends_to_run:
        print(f"\n{'='*70}")
        print(f"Backend: {platform}")
        print(f"{'='*70}")

        for config in configs:
            print(f"\n  Config: {config['name']} ({config['threads']} threads, {config['memory']})")
            print(f"  {'-'*60}")

            for query in queries:
                # Transform SQL for DuckDB
                sql = transform_sql_for_duckdb(query.sql_template, base_path, use_r2)

                print(f"\n    {query.id}: {query.name}")

                # Cold start (fresh connection)
                if track_cold_start:
                    print(f"      Cold start...", end=" ", flush=True)
                    try:
                        cold_conn = create_connection(
                            config["threads"], config["memory"], config["disk"], temp_dir, use_r2
                        )
                        result = run_single_query(
                            cold_conn, platform, query.id, sql, config, 0, True, temp_dir
                        )
                        cold_conn.close()

                        if result.error:
                            print(f"ERROR: {result.error[:50]}")
                        else:
                            print(f"{result.latency_ms:.0f}ms, {result.rows_returned} rows")
                            all_results.append(result)
                    except Exception as e:
                        print(f"ERROR: {str(e)[:50]}")

                # Warm runs (reuse connection)
                try:
                    conn = create_connection(
                        config["threads"], config["memory"], config["disk"], temp_dir, use_r2
                    )

                    for i in range(runs):
                        print(f"      Run {i+1}...", end=" ", flush=True)
                        result = run_single_query(
                            conn, platform, query.id, sql, config, i + 1, False, temp_dir
                        )

                        if result.error:
                            print(f"ERROR: {result.error[:50]}")
                        else:
                            print(f"{result.latency_ms:.0f}ms, {result.rows_returned} rows, ${result.calculated_cost:.4f}")
                            all_results.append(result)

                    conn.close()
                except Exception as e:
                    print(f"      Connection error: {str(e)[:50]}")

    return all_results


# ============================================================================
# OUTPUT
# ============================================================================

def save_results(results: list[BenchmarkResult], output_dir: Path, backend: str):
    """Save results to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Group by platform
    platforms = set(r.platform for r in results)

    for platform in platforms:
        platform_results = [r for r in results if r.platform == platform]

        csv_file = output_dir / f"wide_scan_{platform}_{timestamp}.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "platform", "query_id", "query_category", "config", "threads", "memory",
                "run_number", "is_cold_start", "latency_ms", "cpu_time_ms",
                "peak_memory_mb", "rows_returned", "calculated_cost", "error", "timestamp"
            ])
            for r in platform_results:
                writer.writerow([
                    r.platform, r.query_id, r.query_category, r.config_name, r.threads,
                    r.memory_limit, r.run_number, r.is_cold_start, r.latency_ms,
                    r.cpu_time_ms, r.peak_memory_mb, r.rows_returned,
                    r.calculated_cost, r.error, r.timestamp
                ])

        print(f"\nSaved: {csv_file}")

    # Print summary
    print_summary(results)


def print_summary(results: list[BenchmarkResult]):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    # Group by platform
    platforms = sorted(set(r.platform for r in results))

    for platform in platforms:
        platform_results = [r for r in results if r.platform == platform and not r.error]
        if not platform_results:
            continue

        warm_results = [r for r in platform_results if not r.is_cold_start]
        cold_results = [r for r in platform_results if r.is_cold_start]

        print(f"\n{platform}:")
        print(f"  Total queries: {len(platform_results)}")

        if warm_results:
            latencies = [r.latency_ms for r in warm_results]
            print(f"  Warm median: {sorted(latencies)[len(latencies)//2]:.0f} ms")
            print(f"  Warm mean: {sum(latencies)/len(latencies):.0f} ms")

        if cold_results:
            cold_latencies = [r.latency_ms for r in cold_results]
            print(f"  Cold median: {sorted(cold_latencies)[len(cold_latencies)//2]:.0f} ms")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="DuckDB Wide Scan Benchmark (Q43-Q57)")
    parser.add_argument("--backend", type=str, choices=["local", "r2", "both"], default="local",
                        help="Storage backend: local, r2, or both")
    parser.add_argument("--local-data-path", type=str, default=DEFAULT_LOCAL_DATA_PATH,
                        help=f"Path to local parquet data (default: {DEFAULT_LOCAL_DATA_PATH})")
    parser.add_argument("--configs", type=str, default="small",
                        help="Comma-separated config names (minimal,small,medium,large,maximum)")
    parser.add_argument("--runs", type=int, default=3, help="Number of warm runs per query")
    parser.add_argument("--track-cold-start", action="store_true", help="Track cold start separately")
    parser.add_argument("--output-dir", type=Path, default=Path("results"),
                        help="Output directory for results")
    args = parser.parse_args()

    # Parse configs
    config_names = [c.strip().lower() for c in args.configs.split(",")]
    configs = [c for c in CONFIGS if c["name"] in config_names]

    if not configs:
        print(f"Error: No valid configs found. Available: {[c['name'] for c in CONFIGS]}")
        return

    print("=" * 70)
    print("DuckDB WIDE SCAN BENCHMARK (Q43-Q57)")
    print("=" * 70)
    print(f"Backend: {args.backend}")
    print(f"Configs: {[c['name'] for c in configs]}")
    print(f"Runs: {args.runs}")
    print(f"Cold start tracking: {args.track_cold_start}")
    print(f"Queries: Q43-Q57 ({len(WIDE_SCAN_QUERIES)} wide scan queries)")

    if args.backend in ["r2", "both"]:
        if not (R2_ENDPOINT and R2_BUCKET):
            print("\nWarning: R2 settings not set. Set environment variables:")
            print("  R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET")
            if args.backend == "r2":
                return

    # Run benchmark
    results = run_benchmark(
        backend=args.backend,
        local_data_path=args.local_data_path,
        configs=configs,
        runs=args.runs,
        track_cold_start=args.track_cold_start,
    )

    # Save results
    if results:
        save_results(results, args.output_dir, args.backend)
    else:
        print("\nNo results to save.")


if __name__ == "__main__":
    main()
