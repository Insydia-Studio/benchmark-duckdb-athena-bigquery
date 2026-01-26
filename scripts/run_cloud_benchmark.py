#!/usr/bin/env python3
"""
Cloud Platform Benchmark - BigQuery & Athena with full metrics capture.

Captures:
- BigQuery: latency, bytes_processed, bytes_billed, cache_hit, slot_ms, rows_returned
- Athena: latency, bytes_scanned, query_execution_time_ms, rows_returned

R2 Egress: FREE (Cloudflare R2 has zero egress fees)
"""

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Optional imports
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    from google.cloud import bigquery
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False


# Configuration
RESULTS_DIR = Path(__file__).parent / "results"

# Athena configuration
ATHENA_DATABASE = "ceta_benchmark"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = "s3://ceta-benchmark-eu/athena-results/"
ATHENA_REGION = "eu-west-1"

# BigQuery configuration
BIGQUERY_PROJECT = os.environ.get("GCP_PROJECT", "project-8521de6f-e81e-4971-b58")
BIGQUERY_DATASET = "ceta_benchmark"


@dataclass
class CloudBenchmarkResult:
    """Result of a single cloud benchmark run with full metrics."""
    platform: str
    query_id: str
    query_category: str
    run_number: int
    is_cold_start: bool

    # Timing
    latency_ms: float  # Total wall clock time
    execution_time_ms: float = 0  # Server-side execution time

    # Data metrics
    bytes_scanned: int = 0  # Bytes actually read
    bytes_billed: int = 0  # Bytes charged (BigQuery rounds up to 10MB min)
    rows_returned: int = 0

    # BigQuery-specific
    cache_hit: Optional[bool] = None
    slot_ms: int = 0  # BigQuery compute units (slot milliseconds)

    # Cost calculation
    calculated_cost: float = 0.0

    # Error
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# Import all 42 queries from queries.py
from queries import QUERIES as BASE_QUERIES
from queries_platform import get_queries_for_platform

# Convert to dict format for this script
def _build_queries_dict(platform: str) -> dict:
    """Build queries dict from BenchmarkQuery objects."""
    queries = get_queries_for_platform(platform)
    return {
        q.id: {
            "name": q.name,
            "category": q.category,
            "sql": q.sql_template,
        }
        for q in queries
    }

# We'll build platform-specific queries at runtime
QUERIES = _build_queries_dict("bigquery")  # Default, will be overridden per platform


def calculate_bigquery_cost(bytes_billed: int) -> float:
    """Calculate BigQuery cost based on bytes billed.

    Pricing: $5 per TB queried (on-demand)
    Minimum: 10 MB per query
    """
    COST_PER_TB = 5.0
    bytes_billed = max(bytes_billed, 10 * 1024 * 1024)  # 10 MB minimum
    cost = (bytes_billed / (1024**4)) * COST_PER_TB
    return round(cost, 6)


def calculate_athena_cost(bytes_scanned: int) -> float:
    """Calculate Athena cost based on bytes scanned.

    Pricing: $5 per TB scanned
    Minimum: 10 MB per query
    """
    COST_PER_TB = 5.0
    bytes_scanned = max(bytes_scanned, 10 * 1024 * 1024)  # 10 MB minimum
    cost = (bytes_scanned / (1024**4)) * COST_PER_TB
    return round(cost, 6)


class BigQueryRunner:
    """BigQuery benchmark runner with full metrics capture."""

    def __init__(self, use_cache: bool = False):
        self.client = None
        self.use_cache = use_cache

    def setup(self):
        if not HAS_BIGQUERY:
            raise ImportError("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        self.client = bigquery.Client(project=BIGQUERY_PROJECT)
        print(f"  Connected to BigQuery: {BIGQUERY_PROJECT}")
        print(f"  Dataset: {BIGQUERY_DATASET}")
        print(f"  Cache: {'enabled' if self.use_cache else 'disabled'}")

    def run_query(self, query_id: str, query_def: dict, run_number: int,
                  is_cold_start: bool = False) -> CloudBenchmarkResult:
        """Execute query and capture all metrics."""
        sql = query_def["sql"].format(table_prefix=f"{BIGQUERY_DATASET}.")

        start_time = time.perf_counter()
        try:
            job_config = bigquery.QueryJobConfig(use_query_cache=self.use_cache)
            job = self.client.query(sql, job_config=job_config)
            result = job.result()
            latency_ms = (time.perf_counter() - start_time) * 1000

            rows = list(result)

            # Extract all metrics from job
            return CloudBenchmarkResult(
                platform="bigquery" if not self.use_cache else "bigquery-cached",
                query_id=query_id,
                query_category=query_def["category"],
                run_number=run_number,
                is_cold_start=is_cold_start,
                latency_ms=latency_ms,
                execution_time_ms=job.slot_millis or 0,  # Execution time in slot-ms
                bytes_scanned=job.total_bytes_processed or 0,
                bytes_billed=job.total_bytes_billed or 0,
                rows_returned=len(rows),
                cache_hit=job.cache_hit,
                slot_ms=job.slot_millis or 0,
                calculated_cost=calculate_bigquery_cost(job.total_bytes_billed or 0) if not job.cache_hit else 0,
            )
        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            return CloudBenchmarkResult(
                platform="bigquery",
                query_id=query_id,
                query_category=query_def["category"],
                run_number=run_number,
                is_cold_start=is_cold_start,
                latency_ms=latency_ms,
                error=str(e),
            )

    def cleanup(self):
        if self.client:
            self.client.close()


class AthenaRunner:
    """Athena benchmark runner with full metrics capture."""

    def __init__(self):
        self.client = None

    def setup(self):
        if not HAS_BOTO3:
            raise ImportError("boto3 not installed. Run: pip install boto3")
        self.client = boto3.client("athena", region_name=ATHENA_REGION)
        print(f"  Connected to Athena: {ATHENA_REGION}")
        print(f"  Database: {ATHENA_DATABASE}")
        print(f"  Workgroup: {ATHENA_WORKGROUP}")

    def _wait_for_query(self, query_execution_id: str, timeout: int = 300) -> dict:
        """Wait for query completion."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return response["QueryExecution"]
            time.sleep(0.5)
        raise TimeoutError(f"Query {query_execution_id} timed out")

    def _get_result_count(self, query_execution_id: str) -> int:
        """Get row count from query results."""
        try:
            paginator = self.client.get_paginator('get_query_results')
            row_count = 0
            for page in paginator.paginate(QueryExecutionId=query_execution_id):
                row_count += len(page.get('ResultSet', {}).get('Rows', [])) - 1  # Subtract header
            return max(0, row_count)
        except Exception:
            return 0

    def run_query(self, query_id: str, query_def: dict, run_number: int,
                  is_cold_start: bool = False) -> CloudBenchmarkResult:
        """Execute query and capture all metrics."""
        # Athena uses different table names - no prefix needed if views exist
        sql = query_def["sql"].format(table_prefix="")

        start_time = time.perf_counter()
        try:
            # Disable result reuse for accurate benchmarking
            response = self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": ATHENA_DATABASE},
                WorkGroup=ATHENA_WORKGROUP,
                ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
                ResultReuseConfiguration={
                    "ResultReuseByAgeConfiguration": {"Enabled": False}
                },
            )
            query_execution_id = response["QueryExecutionId"]

            execution = self._wait_for_query(query_execution_id)
            latency_ms = (time.perf_counter() - start_time) * 1000

            if execution["Status"]["State"] == "SUCCEEDED":
                stats = execution.get("Statistics", {})
                bytes_scanned = stats.get("DataScannedInBytes", 0)
                exec_time = stats.get("EngineExecutionTimeInMillis", 0)

                rows_returned = self._get_result_count(query_execution_id)

                return CloudBenchmarkResult(
                    platform="athena",
                    query_id=query_id,
                    query_category=query_def["category"],
                    run_number=run_number,
                    is_cold_start=is_cold_start,
                    latency_ms=latency_ms,
                    execution_time_ms=exec_time,
                    bytes_scanned=bytes_scanned,
                    bytes_billed=bytes_scanned,  # Athena bills on scanned
                    rows_returned=rows_returned,
                    calculated_cost=calculate_athena_cost(bytes_scanned),
                )
            else:
                error = execution["Status"].get("StateChangeReason", "Unknown error")
                return CloudBenchmarkResult(
                    platform="athena",
                    query_id=query_id,
                    query_category=query_def["category"],
                    run_number=run_number,
                    is_cold_start=is_cold_start,
                    latency_ms=latency_ms,
                    error=error,
                )
        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            return CloudBenchmarkResult(
                platform="athena",
                query_id=query_id,
                query_category=query_def["category"],
                run_number=run_number,
                is_cold_start=is_cold_start,
                latency_ms=latency_ms,
                error=str(e),
            )

    def cleanup(self):
        pass


def save_results(results: list[CloudBenchmarkResult], output_dir: Path) -> Path:
    """Save results to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_file = output_dir / f"cloud_benchmark_{timestamp}.csv"

    fieldnames = [
        "platform", "query_id", "query_category", "run_number", "is_cold_start",
        "latency_ms", "execution_time_ms", "bytes_scanned", "bytes_billed",
        "rows_returned", "cache_hit", "slot_ms", "calculated_cost", "error", "timestamp"
    ]

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    print(f"\nResults saved to: {csv_file}")
    return csv_file


def print_summary(results: list[CloudBenchmarkResult]):
    """Print summary of benchmark results."""
    print("\n" + "="*80)
    print("BENCHMARK SUMMARY")
    print("="*80)

    for platform in set(r.platform for r in results):
        print(f"\n{platform.upper()}")
        print("-"*60)

        platform_results = [r for r in results if r.platform == platform]
        for query_id in sorted(set(r.query_id for r in platform_results)):
            query_results = [r for r in platform_results if r.query_id == query_id]

            cold = [r for r in query_results if r.is_cold_start and not r.error]
            warm = [r for r in query_results if not r.is_cold_start and not r.error]
            errors = [r for r in query_results if r.error]

            if errors:
                print(f"  {query_id}: ERROR - {errors[0].error[:50]}")
                continue

            if cold:
                c = cold[0]
                print(f"  {query_id} (cold):  {c.latency_ms:>8.1f}ms | "
                      f"bytes_billed: {c.bytes_billed/1024/1024:>6.1f}MB | "
                      f"cost: ${c.calculated_cost:.6f}")
                if c.cache_hit is not None:
                    print(f"                    cache_hit: {c.cache_hit} | slot_ms: {c.slot_ms}")

            if warm:
                import statistics
                latencies = [r.latency_ms for r in warm]
                avg_bytes = sum(r.bytes_billed for r in warm) / len(warm)
                total_cost = sum(r.calculated_cost for r in warm)

                print(f"  {query_id} (warm):  {statistics.mean(latencies):>8.1f}ms avg | "
                      f"bytes_billed: {avg_bytes/1024/1024:>6.1f}MB | "
                      f"cost: ${total_cost:.6f} (total {len(warm)} runs)")


def main():
    parser = argparse.ArgumentParser(description="Run cloud benchmark (BigQuery/Athena)")
    parser.add_argument("--platforms", type=str, default="bigquery,athena",
                        help="Comma-separated: bigquery, bigquery-cached, athena")
    parser.add_argument("--queries", type=str, default="Q01",
                        help="Comma-separated query IDs (e.g., Q01,Q02)")
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of warm runs per query")
    parser.add_argument("--track-cold-start", action="store_true", default=True,
                        help="Record first run as cold start")
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    args = parser.parse_args()

    platforms = [p.strip().lower() for p in args.platforms.split(",")]
    query_ids = [q.strip().upper() for q in args.queries.split(",")]

    print("="*80)
    print("CLOUD PLATFORM BENCHMARK - Full Metrics")
    print("="*80)
    print(f"Platforms: {', '.join(platforms)}")
    print(f"Queries: {', '.join(query_ids)}")
    print(f"Runs: {args.runs} (+ cold start)")
    print()

    all_results = []

    for platform in platforms:
        print(f"\n{'='*60}")
        print(f"Benchmarking: {platform.upper()}")
        print(f"{'='*60}")

        try:
            if platform == "bigquery":
                runner = BigQueryRunner(use_cache=False)
            elif platform == "bigquery-cached":
                runner = BigQueryRunner(use_cache=True)
            elif platform == "athena":
                runner = AthenaRunner()
            else:
                print(f"  Unknown platform: {platform}")
                continue

            runner.setup()
        except Exception as e:
            print(f"  ERROR setting up {platform}: {e}")
            continue

        # Get platform-specific queries
        platform_queries = _build_queries_dict(platform)

        for query_id in query_ids:
            if query_id not in platform_queries:
                print(f"  Unknown query: {query_id}")
                continue

            query_def = platform_queries[query_id]
            print(f"\n  Query {query_id}: {query_def['name']} ({query_def['category']})")

            # Cold start
            if args.track_cold_start:
                print(f"    Cold start...", end=" ", flush=True)
                result = runner.run_query(query_id, query_def, run_number=0, is_cold_start=True)
                if result.error:
                    print(f"ERROR: {result.error}")
                    continue
                print(f"{result.latency_ms:.1f}ms | {result.bytes_billed/1024/1024:.1f}MB | ${result.calculated_cost:.6f}")
                all_results.append(result)

            # Warm runs
            for i in range(args.runs):
                print(f"    Run {i+1}/{args.runs}...", end=" ", flush=True)
                result = runner.run_query(query_id, query_def, run_number=i+1, is_cold_start=False)
                if result.error:
                    print(f"ERROR: {result.error}")
                    break
                print(f"{result.latency_ms:.1f}ms | {result.bytes_billed/1024/1024:.1f}MB")
                if result.cache_hit:
                    print(f"         ^ CACHE HIT")
                all_results.append(result)

        runner.cleanup()

    if all_results:
        save_results(all_results, args.output_dir)
        print_summary(all_results)
    else:
        print("\nNo results collected.")


if __name__ == "__main__":
    main()
