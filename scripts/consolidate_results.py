#!/usr/bin/env python3
"""
Consolidate all benchmark results and generate final report.

Combines:
- Original 42 queries (Q01-Q42)
- Wide scan queries (Q43-Q57)
"""

import csv
import json
import statistics
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
OUTPUT_DIR = Path(__file__).parent / "content"

# Original result files
ORIGINAL_FILES = {
    "duckdb_local": RESULTS_DIR / "duckdb_local_benchmark_20260125_162021.csv",
    "duckdb_r2": RESULTS_DIR / "duckdb_r2_benchmark_20260125_181354.csv",
    "bigquery": RESULTS_DIR / "cloud_benchmark_20260125_205503.csv",
    "athena": RESULTS_DIR / "athena_benchmark_20260126_complete.csv",
}

# Wide scan result files (find the latest)
def find_latest_wide_scan(platform):
    pattern = f"wide_scan_{platform}_*.csv"
    files = list(RESULTS_DIR.glob(pattern))
    if files:
        return max(files, key=lambda f: f.stat().st_mtime)
    return None

PLATFORM_NAMES = {
    "duckdb_local": "DuckDB (Local SSD)",
    "duckdb_r2": "DuckDB + R2",
    "bigquery": "BigQuery",
    "athena": "Athena",
}


def load_csv(path, platform_override=None):
    """Load CSV and return list of dicts."""
    results = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize fields
            r = {
                "platform": platform_override or row.get("platform", "unknown"),
                "query_id": row.get("query_id", ""),
                "query_category": row.get("query_category", ""),
                "run_number": int(row.get("run_number", 0)),
                "is_cold_start": row.get("is_cold_start", "False") == "True",
                "latency_ms": float(row.get("latency_ms", 0) or 0),
                "bytes_scanned": int(row.get("bytes_scanned", 0) or 0),
                "bytes_billed": int(row.get("bytes_billed", 0) or 0),
                "calculated_cost": float(row.get("calculated_cost", 0) or 0),
                "error": row.get("error", ""),
            }
            results.append(r)
    return results


def analyze_results(results):
    """Analyze results and return summary stats."""
    # Filter valid results
    valid = [r for r in results if not r["error"]]
    warm = [r for r in valid if not r["is_cold_start"]]
    cold = [r for r in valid if r["is_cold_start"]]

    if not warm:
        return None

    warm_latencies = [r["latency_ms"] for r in warm]
    cold_latencies = [r["latency_ms"] for r in cold]
    costs = [r["calculated_cost"] for r in valid]
    bytes_scanned = [r["bytes_scanned"] for r in valid if r["bytes_scanned"] > 0]

    return {
        "total_queries": len(valid),
        "warm_median": round(statistics.median(warm_latencies), 0),
        "warm_mean": round(statistics.mean(warm_latencies), 0),
        "cold_median": round(statistics.median(cold_latencies), 0) if cold_latencies else 0,
        "total_cost": round(sum(costs), 4),
        "avg_cost": round(sum(costs) / len(valid), 6) if valid else 0,
        "total_bytes_gb": round(sum(bytes_scanned) / (1024**3), 2) if bytes_scanned else 0,
    }


def main():
    print("=" * 60)
    print("CONSOLIDATED BENCHMARK ANALYSIS")
    print("=" * 60)

    all_results = {}
    summary = {}

    # Load original results
    print("\n--- Original Queries (Q01-Q42) ---")
    for platform, path in ORIGINAL_FILES.items():
        if path.exists():
            if platform in ["duckdb_local", "duckdb_r2"]:
                results = load_csv(path, platform)
                # Filter to small config only
                results = [r for r in results if "small" in str(r.get("config", "small"))]
            else:
                results = load_csv(path)

            all_results[f"{platform}_original"] = results
            stats = analyze_results(results)
            if stats:
                summary[f"{platform}_original"] = stats
                print(f"{PLATFORM_NAMES[platform]}: {stats['total_queries']} queries, "
                      f"${stats['total_cost']:.4f} total, ${stats['avg_cost']:.6f}/query")

    # Load wide scan results
    print("\n--- Wide Scan Queries (Q43-Q57) ---")
    for platform in ["bigquery", "athena"]:
        path = find_latest_wide_scan(platform)
        if path:
            results = load_csv(path)
            all_results[f"{platform}_wide"] = results
            stats = analyze_results(results)
            if stats:
                summary[f"{platform}_wide"] = stats
                print(f"{PLATFORM_NAMES[platform]}: {stats['total_queries']} queries, "
                      f"${stats['total_cost']:.4f} total, ${stats['avg_cost']:.6f}/query")

    # Combined analysis
    print("\n" + "=" * 60)
    print("COMBINED RESULTS (Original + Wide Scan)")
    print("=" * 60)

    combined = {}
    for platform in ["bigquery", "athena"]:
        orig = all_results.get(f"{platform}_original", [])
        wide = all_results.get(f"{platform}_wide", [])
        all_data = orig + wide
        stats = analyze_results(all_data)
        if stats:
            combined[platform] = stats
            print(f"\n{PLATFORM_NAMES[platform]}:")
            print(f"  Total queries: {stats['total_queries']}")
            print(f"  Warm median: {stats['warm_median']}ms")
            print(f"  Total cost: ${stats['total_cost']:.4f}")
            print(f"  Avg cost/query: ${stats['avg_cost']:.6f}")
            print(f"  Data scanned: {stats['total_bytes_gb']} GB")

    # DuckDB (original only since we didn't run wide scan)
    for platform in ["duckdb_local", "duckdb_r2"]:
        if f"{platform}_original" in summary:
            combined[platform] = summary[f"{platform}_original"]
            stats = combined[platform]
            print(f"\n{PLATFORM_NAMES[platform]}:")
            print(f"  Total queries: {stats['total_queries']}")
            print(f"  Warm median: {stats['warm_median']}ms")
            print(f"  Total cost: ${stats['total_cost']:.4f}")
            print(f"  Avg cost/query: ${stats['avg_cost']:.6f}")

    # Final comparison table
    print("\n" + "=" * 60)
    print("FINAL COST COMPARISON")
    print("=" * 60)
    print("\n| Platform | Queries | Avg $/Query | Warm Median |")
    print("|----------|---------|-------------|-------------|")

    for platform in ["duckdb_local", "duckdb_r2", "bigquery", "athena"]:
        if platform in combined:
            s = combined[platform]
            print(f"| {PLATFORM_NAMES[platform]} | {s['total_queries']} | ${s['avg_cost']:.4f} | {s['warm_median']:.0f}ms |")

    # Save consolidated summary
    OUTPUT_DIR.mkdir(exist_ok=True)
    output = {
        "original": summary,
        "combined": combined,
        "comparison": {
            platform: {
                "name": PLATFORM_NAMES[platform],
                "avg_cost_per_query": combined[platform]["avg_cost"],
                "warm_median_ms": combined[platform]["warm_median"],
                "total_queries": combined[platform]["total_queries"],
            }
            for platform in combined
        }
    }

    with open(OUTPUT_DIR / "consolidated_summary.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved: {OUTPUT_DIR / 'consolidated_summary.json'}")


if __name__ == "__main__":
    main()
