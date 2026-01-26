#!/usr/bin/env python3
"""
Comprehensive benchmark analysis for cloud data platform comparison.

Analyzes results from:
- DuckDB Local (multiple configurations)
- DuckDB R2 (Cloudflare remote storage)
- BigQuery
- Athena

Outputs:
- analysis_summary.json: Aggregated metrics for content generation
- Markdown tables for direct use in content
"""

import csv
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

# File paths
RESULTS_DIR = Path(__file__).parent / "results"
OUTPUT_DIR = Path(__file__).parent / "content"

FILES = {
    "duckdb_local": RESULTS_DIR / "duckdb_local_benchmark_20260125_162021.csv",
    "duckdb_r2": RESULTS_DIR / "duckdb_r2_benchmark_20260125_181354.csv",
    "bigquery": RESULTS_DIR / "cloud_benchmark_20260125_205503.csv",
    "athena": RESULTS_DIR / "athena_benchmark_20260126_complete.csv",
}

# Platform display names
PLATFORM_NAMES = {
    "duckdb_local": "DuckDB (Local SSD)",
    "duckdb_r2": "DuckDB + R2",
    "bigquery": "BigQuery",
    "athena": "Athena",
}

# Query category display
CATEGORY_NAMES = {
    "scan": "Table Scan",
    "aggregation": "Aggregation",
    "window": "Window Functions",
    "join": "Joins",
    "analytics": "Complex Analytics",
    "timeseries": "Time Series",
    "quality": "Data Quality",
}


@dataclass
class QueryResult:
    """Single query execution result."""
    platform: str
    query_id: str
    query_category: str
    config: str  # For DuckDB: small/medium/large/maximum
    run_number: int
    is_cold_start: bool
    latency_ms: float
    cpu_time_ms: float = 0.0
    bytes_scanned: int = 0
    bytes_billed: int = 0
    calculated_cost: float = 0.0
    rows_scanned: int = 0
    rows_returned: int = 0
    error: str = ""


def load_duckdb_results(path: Path, platform: str) -> list[QueryResult]:
    """Load DuckDB format results (local or R2)."""
    results = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(QueryResult(
                platform=platform,
                query_id=row["query_id"],
                query_category=row["query_category"],
                config=row["config"],
                run_number=int(row["run_number"]),
                is_cold_start=row["is_cold_start"] == "True",
                latency_ms=float(row["latency_ms"]),
                cpu_time_ms=float(row.get("cpu_time_ms", 0) or 0),
                rows_scanned=int(row.get("rows_scanned", 0) or 0),
                rows_returned=int(row.get("rows_returned", 0) or 0),
                calculated_cost=float(row.get("calculated_cost", 0) or 0),
                error=row.get("error", ""),
            ))
    return results


def load_cloud_results(path: Path) -> list[QueryResult]:
    """Load cloud platform results (BigQuery or Athena)."""
    results = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(QueryResult(
                platform=row["platform"],
                query_id=row["query_id"],
                query_category=row["query_category"],
                config="default",  # Cloud platforms have single config
                run_number=int(row["run_number"]),
                is_cold_start=row["is_cold_start"] == "True",
                latency_ms=float(row["latency_ms"]),
                bytes_scanned=int(row.get("bytes_scanned", 0) or 0),
                bytes_billed=int(row.get("bytes_billed", 0) or 0),
                calculated_cost=float(row.get("calculated_cost", 0) or 0),
                rows_returned=int(row.get("rows_returned", 0) or 0),
                error=row.get("error", ""),
            ))
    return results


def load_all_results() -> dict[str, list[QueryResult]]:
    """Load all benchmark results."""
    all_results = {}

    # DuckDB Local
    if FILES["duckdb_local"].exists():
        all_results["duckdb_local"] = load_duckdb_results(FILES["duckdb_local"], "duckdb_local")
        print(f"Loaded {len(all_results['duckdb_local'])} DuckDB Local results")

    # DuckDB R2
    if FILES["duckdb_r2"].exists():
        all_results["duckdb_r2"] = load_duckdb_results(FILES["duckdb_r2"], "duckdb_r2")
        print(f"Loaded {len(all_results['duckdb_r2'])} DuckDB R2 results")

    # BigQuery
    if FILES["bigquery"].exists():
        all_results["bigquery"] = load_cloud_results(FILES["bigquery"])
        print(f"Loaded {len(all_results['bigquery'])} BigQuery results")

    # Athena
    if FILES["athena"].exists():
        all_results["athena"] = load_cloud_results(FILES["athena"])
        print(f"Loaded {len(all_results['athena'])} Athena results")

    return all_results


def calculate_stats(latencies: list[float]) -> dict:
    """Calculate statistics for a list of latencies."""
    if not latencies:
        return {"count": 0, "mean": 0, "median": 0, "min": 0, "max": 0, "stdev": 0}

    return {
        "count": len(latencies),
        "mean": round(statistics.mean(latencies), 2),
        "median": round(statistics.median(latencies), 2),
        "min": round(min(latencies), 2),
        "max": round(max(latencies), 2),
        "stdev": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
    }


def analyze_by_platform(results: dict[str, list[QueryResult]]) -> dict:
    """Analyze results grouped by platform."""
    analysis = {}

    for platform, platform_results in results.items():
        # Filter out errors and focus on default/small config for fair comparison
        valid_results = [r for r in platform_results if not r.error]

        # For DuckDB, use "small" config as baseline for fair comparison
        # Also analyze "maximum" config separately
        if platform in ["duckdb_local", "duckdb_r2"]:
            baseline_results = [r for r in valid_results if r.config == "small"]
            max_results = [r for r in valid_results if r.config == "maximum"]
        else:
            baseline_results = valid_results
            max_results = []

        # Warm queries (exclude cold start)
        warm_results = [r for r in baseline_results if not r.is_cold_start]
        cold_results = [r for r in baseline_results if r.is_cold_start]

        # Calculate warm latency stats
        warm_latencies = [r.latency_ms for r in warm_results]
        cold_latencies = [r.latency_ms for r in cold_results]

        analysis[platform] = {
            "name": PLATFORM_NAMES.get(platform, platform),
            "total_queries": len(baseline_results),
            "warm_stats": calculate_stats(warm_latencies),
            "cold_stats": calculate_stats(cold_latencies),
            "cold_start_overhead_pct": 0,
        }

        # Calculate cold start overhead
        if warm_latencies and cold_latencies:
            warm_mean = statistics.mean(warm_latencies)
            cold_mean = statistics.mean(cold_latencies)
            if warm_mean > 0:
                analysis[platform]["cold_start_overhead_pct"] = round(
                    ((cold_mean - warm_mean) / warm_mean) * 100, 1
                )

        # Add maximum config stats for DuckDB
        if max_results:
            max_warm = [r for r in max_results if not r.is_cold_start]
            analysis[platform]["max_config_warm_stats"] = calculate_stats(
                [r.latency_ms for r in max_warm]
            )

    return analysis


def analyze_by_category(results: dict[str, list[QueryResult]]) -> dict:
    """Analyze results grouped by query category."""
    categories = {}

    # Get all unique categories
    all_categories = set()
    for platform_results in results.values():
        for r in platform_results:
            all_categories.add(r.query_category)

    for category in sorted(all_categories):
        categories[category] = {
            "name": CATEGORY_NAMES.get(category, category.title()),
            "platforms": {}
        }

        for platform, platform_results in results.items():
            # Filter to this category, warm queries, small/default config
            if platform in ["duckdb_local", "duckdb_r2"]:
                cat_results = [
                    r for r in platform_results
                    if r.query_category == category
                    and not r.is_cold_start
                    and not r.error
                    and r.config == "small"
                ]
            else:
                cat_results = [
                    r for r in platform_results
                    if r.query_category == category
                    and not r.is_cold_start
                    and not r.error
                ]

            if cat_results:
                latencies = [r.latency_ms for r in cat_results]
                categories[category]["platforms"][platform] = {
                    "name": PLATFORM_NAMES.get(platform, platform),
                    "stats": calculate_stats(latencies),
                }

    return categories


def analyze_cost(results: dict[str, list[QueryResult]]) -> dict:
    """Analyze costs across platforms."""
    costs = {}

    for platform, platform_results in results.items():
        # Filter valid results
        if platform in ["duckdb_local", "duckdb_r2"]:
            valid = [r for r in platform_results if not r.error and r.config == "small"]
        else:
            valid = [r for r in platform_results if not r.error]

        if not valid:
            continue

        # Calculate total and per-query costs
        total_cost = sum(r.calculated_cost for r in valid)
        query_count = len(set(r.query_id for r in valid))

        # Get unique queries for per-query cost
        query_costs = {}
        for r in valid:
            if r.query_id not in query_costs:
                query_costs[r.query_id] = []
            query_costs[r.query_id].append(r.calculated_cost)

        avg_query_costs = {
            qid: round(statistics.mean(costs), 6)
            for qid, costs in query_costs.items()
        }

        # Calculate bytes scanned (for cloud platforms)
        if platform in ["bigquery", "athena"]:
            total_bytes = sum(r.bytes_scanned for r in valid)
            total_gb = total_bytes / (1024 ** 3)
        else:
            total_gb = 0

        costs[platform] = {
            "name": PLATFORM_NAMES.get(platform, platform),
            "total_cost": round(total_cost, 4),
            "avg_cost_per_query": round(total_cost / len(valid), 6) if valid else 0,
            "total_data_scanned_gb": round(total_gb, 2),
            "query_costs": avg_query_costs,
        }

    return costs


def find_best_platform_per_category(category_analysis: dict) -> dict:
    """Find the fastest platform for each query category."""
    best = {}

    for category, data in category_analysis.items():
        platforms = data.get("platforms", {})
        if not platforms:
            continue

        # Find platform with lowest median latency
        best_platform = None
        best_latency = float("inf")

        for platform, pdata in platforms.items():
            median = pdata["stats"]["median"]
            if median < best_latency:
                best_latency = median
                best_platform = platform

        if best_platform:
            best[category] = {
                "platform": best_platform,
                "name": PLATFORM_NAMES.get(best_platform, best_platform),
                "median_ms": best_latency,
            }

    return best


def generate_comparison_table(platform_analysis: dict) -> str:
    """Generate markdown table comparing platforms."""
    lines = [
        "| Platform | Warm Median (ms) | Warm Mean (ms) | Cold Start Overhead |",
        "|----------|------------------|----------------|---------------------|",
    ]

    # Sort by warm median latency
    sorted_platforms = sorted(
        platform_analysis.items(),
        key=lambda x: x[1]["warm_stats"]["median"]
    )

    for platform, data in sorted_platforms:
        warm = data["warm_stats"]
        overhead = data["cold_start_overhead_pct"]
        lines.append(
            f"| {data['name']} | {warm['median']:.0f} | {warm['mean']:.0f} | {overhead:.0f}% |"
        )

    return "\n".join(lines)


def generate_category_table(category_analysis: dict) -> str:
    """Generate markdown table by query category."""
    platforms = ["duckdb_local", "duckdb_r2", "bigquery", "athena"]

    header = "| Category | " + " | ".join(PLATFORM_NAMES[p] for p in platforms) + " |"
    sep = "|----------|" + "|".join("-" * 15 for _ in platforms) + "|"

    lines = [header, sep]

    for category, data in sorted(category_analysis.items()):
        row = [CATEGORY_NAMES.get(category, category)]
        for platform in platforms:
            if platform in data["platforms"]:
                median = data["platforms"][platform]["stats"]["median"]
                row.append(f"{median:.0f}ms")
            else:
                row.append("-")
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def generate_cost_table(cost_analysis: dict) -> str:
    """Generate markdown table for cost comparison."""
    lines = [
        "| Platform | Avg Cost/Query | Data Scanned | Cost Model |",
        "|----------|----------------|--------------|------------|",
    ]

    for platform, data in cost_analysis.items():
        if platform == "duckdb_local":
            model = "CPU time"
        elif platform == "duckdb_r2":
            model = "CPU time + R2 (free egress)"
        elif platform == "bigquery":
            model = "$5/TB billed"
        elif platform == "athena":
            model = "$5/TB scanned"
        else:
            model = "-"

        avg_cost = data["avg_cost_per_query"]
        if avg_cost < 0.001:
            cost_str = f"${avg_cost:.6f}"
        else:
            cost_str = f"${avg_cost:.4f}"

        gb = data["total_data_scanned_gb"]
        gb_str = f"{gb:.1f} GB" if gb > 0 else "N/A"

        lines.append(f"| {data['name']} | {cost_str} | {gb_str} | {model} |")

    return "\n".join(lines)


def generate_cold_start_analysis(results: dict[str, list[QueryResult]]) -> dict:
    """Detailed cold start analysis."""
    analysis = {}

    for platform, platform_results in results.items():
        if platform in ["duckdb_local", "duckdb_r2"]:
            valid = [r for r in platform_results if not r.error and r.config == "small"]
        else:
            valid = [r for r in platform_results if not r.error]

        # Group by query
        query_cold_warm = {}
        for r in valid:
            if r.query_id not in query_cold_warm:
                query_cold_warm[r.query_id] = {"cold": [], "warm": []}
            if r.is_cold_start:
                query_cold_warm[r.query_id]["cold"].append(r.latency_ms)
            else:
                query_cold_warm[r.query_id]["warm"].append(r.latency_ms)

        # Calculate overhead per query
        overheads = []
        for qid, data in query_cold_warm.items():
            if data["cold"] and data["warm"]:
                cold_avg = statistics.mean(data["cold"])
                warm_avg = statistics.mean(data["warm"])
                if warm_avg > 0:
                    overhead = ((cold_avg - warm_avg) / warm_avg) * 100
                    overheads.append({
                        "query_id": qid,
                        "cold_ms": round(cold_avg, 0),
                        "warm_ms": round(warm_avg, 0),
                        "overhead_pct": round(overhead, 1),
                    })

        analysis[platform] = {
            "name": PLATFORM_NAMES.get(platform, platform),
            "queries": sorted(overheads, key=lambda x: x["overhead_pct"], reverse=True),
            "avg_overhead_pct": round(
                statistics.mean([o["overhead_pct"] for o in overheads]), 1
            ) if overheads else 0,
        }

    return analysis


def main():
    print("=" * 60)
    print("Cloud Data Platform Benchmark Analysis")
    print("=" * 60)

    # Load all results
    results = load_all_results()

    if not results:
        print("ERROR: No results files found")
        return

    # Run analyses
    platform_analysis = analyze_by_platform(results)
    category_analysis = analyze_by_category(results)
    cost_analysis = analyze_cost(results)
    cold_start_analysis = generate_cold_start_analysis(results)
    best_per_category = find_best_platform_per_category(category_analysis)

    # Generate summary
    summary = {
        "platforms": platform_analysis,
        "categories": category_analysis,
        "costs": cost_analysis,
        "cold_starts": cold_start_analysis,
        "best_per_category": best_per_category,
        "tables": {
            "platform_comparison": generate_comparison_table(platform_analysis),
            "category_comparison": generate_category_table(category_analysis),
            "cost_comparison": generate_cost_table(cost_analysis),
        },
        "key_findings": {
            "fastest_overall": min(
                platform_analysis.items(),
                key=lambda x: x[1]["warm_stats"]["median"]
            )[0],
            "lowest_cold_start_overhead": min(
                platform_analysis.items(),
                key=lambda x: x[1]["cold_start_overhead_pct"]
            )[0],
            "lowest_cost": min(
                cost_analysis.items(),
                key=lambda x: x[1]["avg_cost_per_query"]
            )[0],
        }
    }

    # Save JSON summary
    OUTPUT_DIR.mkdir(exist_ok=True)
    json_path = OUTPUT_DIR / "analysis_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSaved: {json_path}")

    # Print tables
    print("\n" + "=" * 60)
    print("PLATFORM COMPARISON (Warm Queries)")
    print("=" * 60)
    print(summary["tables"]["platform_comparison"])

    print("\n" + "=" * 60)
    print("PERFORMANCE BY CATEGORY (Warm Median)")
    print("=" * 60)
    print(summary["tables"]["category_comparison"])

    print("\n" + "=" * 60)
    print("COST COMPARISON")
    print("=" * 60)
    print(summary["tables"]["cost_comparison"])

    print("\n" + "=" * 60)
    print("KEY FINDINGS")
    print("=" * 60)
    print(f"Fastest Overall: {PLATFORM_NAMES[summary['key_findings']['fastest_overall']]}")
    print(f"Lowest Cold Start Overhead: {PLATFORM_NAMES[summary['key_findings']['lowest_cold_start_overhead']]}")
    print(f"Lowest Cost: {PLATFORM_NAMES[summary['key_findings']['lowest_cost']]}")

    print("\nBest Platform by Category:")
    for cat, data in best_per_category.items():
        print(f"  {CATEGORY_NAMES.get(cat, cat)}: {data['name']} ({data['median_ms']:.0f}ms)")


if __name__ == "__main__":
    main()
