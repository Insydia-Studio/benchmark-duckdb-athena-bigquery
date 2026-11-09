# CLAUDE.md

<!-- shared:public-rules v2. Every public repo of ours carries this block word for word; change it in all of them at once, never in one repo alone. -->
## Rules for every change

- Never commit a secret, an API key, a server address, or anything that isn't meant to be public.
- Treat the default branch as released code: it can reach users without a further step.
- No AI attribution anywhere: no Co-Authored-By trailer, no "Generated with Claude Code" line and no session link in commits, PR bodies, issues or comments.
- One copy of everything. Never copy a block of code: reuse it, or move it into a shared helper. Instructions follow the same rule: each rule lives in one file and other files point to it, so nothing can drift apart. Keep files and functions small.
- Fix this repo's CLAUDE.md files in passing. When the code contradicts a line, or you had to dig for a fact a session needs, fix the nearest CLAUDE.md, outside this block, in the PR you're on. Write the current fact only, never its history: git keeps that. Add a folder CLAUDE.md only when the folder has facts its parent can't hold in a line or two.
- Write plainly: short sentences, lead with the point, no em-dash connectors. Code comments are one short line, only where the code needs one.
<!-- /shared:public-rules -->

## What this is

A benchmark of analytical query performance across DuckDB (local SSD and Cloudflare R2), BigQuery and Athena on ~20GB of financial time-series Parquet data. It holds the runner scripts, the query set (Q01-Q57) and the published result CSVs. `README.md` has the results summary, dataset and run instructions.

## Layout

- `scripts/`: runners, query definitions, analysis scripts and table DDL. Scripts import each other by bare module name (`from queries import ...`).
- `scripts/queries.py`: core queries Q01-Q42. `queries_platform.py` holds per-platform SQL overrides. `queries_wide_scan.py` holds Q43-Q57.
- `scripts/run_cloud_benchmark.py`: BigQuery and Athena runner.
- `scripts/run_duckdb_local_benchmark.py`, `run_duckdb_r2_benchmark.py`, `run_duckdb_wide_scan_benchmark.py`: DuckDB runners.
- `scripts/run_wide_scan_benchmark.py`: wide scan runner for all platforms.
- `scripts/analyze_all_results.py`, `consolidate_results.py`: turn result CSVs into summary JSON and Markdown tables.
- `scripts/setup_athena.sql`, `setup_bigquery.sql`: one-time table DDL, run by hand in each console.
- `results/`: the 8 published result CSVs. Treat them as data, not code.

## Setup from a fresh clone

No file pins a Python version; these steps were checked with Python 3.11. There is no requirements file; these are the third-party imports:

```bash
python3.11 -m venv .venv
.venv/bin/pip install duckdb google-cloud-bigquery boto3
```

- `duckdb` is a hard import in the DuckDB runners. `boto3` and `google-cloud-bigquery` are imported only when a cloud platform runs.
- Every real run needs credentials or data (not run here):
  - BigQuery: `GOOGLE_APPLICATION_CREDENTIALS` and `GCP_PROJECT`.
  - Athena: AWS credentials in the environment.
  - R2: `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`.
  - DuckDB local: a local copy of the Parquet data, passed with `--local-data-path`.

## Tests and CI gates

There are no tests and no CI. The only checks that run without credentials:

```bash
.venv/bin/python -m py_compile scripts/*.py
.venv/bin/python scripts/run_cloud_benchmark.py --help
.venv/bin/python scripts/run_duckdb_local_benchmark.py --help
.venv/bin/python scripts/run_duckdb_wide_scan_benchmark.py --help
.venv/bin/python scripts/run_wide_scan_benchmark.py --help
.venv/bin/python scripts/queries_wide_scan.py
```

- `run_duckdb_r2_benchmark.py --help` exits 1 unless the four R2 variables are set, because the check runs at import. Dummy values are enough for `--help`:
  `R2_ENDPOINT=x R2_ACCESS_KEY_ID=x R2_SECRET_ACCESS_KEY=x R2_BUCKET=x .venv/bin/python scripts/run_duckdb_r2_benchmark.py --help`
- Don't run a benchmark to "test" a change: each run queries billed cloud services or needs the local dataset.

## How changes ship

Nothing builds or deploys. A merge to `main` publishes the scripts and the CSVs in this public repo, and nothing else.

## Gotchas

- The scripts carry default cloud project, bucket and local data path values; R2 has none. Pass your own through the env vars and flags above.
- Athena database, workgroup, output location and region are module constants in `run_cloud_benchmark.py` and `run_wide_scan_benchmark.py`, with no flag or env var. `run_wide_scan_benchmark.py` also hard-codes its DuckDB data path (`DUCKDB_LOCAL_DATA`).
- Output locations differ by script:
  - `run_cloud_benchmark.py` and `run_wide_scan_benchmark.py` write to `scripts/results/`. `run_wide_scan_benchmark.py` creates that folder on import, even for `--help`.
  - The DuckDB runners write to `results/` relative to the working directory (`--output-dir` overrides).
- `analyze_all_results.py` and `consolidate_results.py` read hard-coded CSV names (and, in `consolidate_results.py`, the newest `wide_scan_*` CSVs) from `scripts/results/`, not the repo-root `results/`. They write to `scripts/content/`, which `.gitignore` excludes.
- `run_wide_scan_benchmark.py --platforms all` runs bigquery, athena and duckdb_local only. Name `duckdb_r2` explicitly.
- `run_cloud_benchmark.py --queries` defaults to `Q01` only.
- `README.md` Structure tree is stale: it omits `run_duckdb_wide_scan_benchmark.py` and the two `wide_scan_duckdb_*` CSVs.
- `README.md` Prerequisites lists `pandas`, which no script imports.
- Don't commit result files from a local run unless the change is meant to publish new results.
