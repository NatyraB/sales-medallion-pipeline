# 🩺 repo-health — Repository Health Report

`repo-health` is a small, **dependency-light** command-line tool that statically
analyzes this repository and prints a concise, prioritized health summary. It is
designed to run **locally without Spark or Databricks installed** — it uses only
the Python standard library (`ast`, `tokenize`, `re`, `pathlib`, `tomllib`) and
**never imports `pyspark`** or executes any pipeline code.

The tool lives under [`tools/repo_health/`](../tools/repo_health) so it is kept
cleanly separate from the pipeline logic under `src/`. Nothing here is deployed
by the Databricks Asset Bundle.

## What it analyzes

Each analyzer contributes a graded category to the overall report:

| Category | What it checks |
|---|---|
| **Code Quality** | AST-parses every `.py` (reporting syntax errors as findings), flags very long files, bare `except:` clauses, `print()`-vs-logging, TODO/FIXME markers, wildcard imports, and unused imports. |
| **Medallion Structure & Naming** | Confirms the bronze/silver/gold layers exist and that files follow a consistent, unambiguous `NN_name.py` numbering scheme — flags the **duplicate `01_/02_/03_` prefixes** where two pipelines share the layer directories. |
| **Test Coverage** | Detects whether any automated tests exist and, crucially, whether any exercise the pipeline under `src/`. The notebooks are not import-testable as written, so pipeline coverage is reported as **0% / absent** with a concrete recommendation. Summarizes any pytest/coverage config found. |
| **Dependency Health** | Flags a missing dependency manifest and third-party imports the pipeline uses that no manifest declares (e.g. `pyspark`, provided by the Databricks Runtime but undocumented). |
| **Security** | Scans source and config for hardcoded secrets/tokens/passwords, embedded workspace hostnames/URLs, and unsafe patterns. Environment-variable / secret-manager usage (`os.environ`, `dbutils.secrets`, `${{ secrets.* }}`) is treated as the **correct** pattern, not a finding. |
| **Cleanup & Refactor Hot-spots** | Finds duplicated code across the layer scripts (e.g. copied config/MERGE boilerplate), the largest files, and module-level dead code. |

Each category is scored out of 100 (penalties scale with finding severity), and
the categories are averaged into an overall **A–F grade**. The tool is
**resilient**: a malformed or unreadable file is reported as a finding rather
than crashing the run.

## Install the dev tooling

The tool itself has **no runtime dependencies**. Only the test suite needs
`pytest`, declared in the minimal [`pyproject.toml`](../pyproject.toml) under a
`dev` extra:

```bash
# From the repository root
python -m pip install -e ".[dev]"
```

> This manifest is scoped to **dev/tooling only**. The pipeline runs on the
> Databricks Runtime, so runtime packages (`pyspark`, `delta`, …) are
> intentionally not listed.

## Run it locally

```bash
# Concise ranked summary (default)
python -m tools.repo_health

# Machine-readable JSON
python -m tools.repo_health --json

# Or via the thin wrapper (runs from the repo root automatically)
scripts/repo-health
scripts/repo-health --json
```

Useful flags:

| Flag | Effect |
|---|---|
| `--json` | Emit a structured JSON report instead of the text summary. |
| `--top N` | Show the top *N* findings in the text summary (default 10). |
| `--min-severity {INFO,LOW,MEDIUM,HIGH,CRITICAL}` | Hide findings below a severity in the text summary. |
| `--fail-under SCORE` | Exit non-zero if the overall score is below `SCORE` (for CI gating). Off by default, so a normal run always exits 0. |
| `path` | Analyze a different repo root (default: auto-detect from the current directory). |

## Example output

Running `python -m tools.repo_health` against this repository currently prints
(abridged; grades, scores, and severities below match the tool's real output):

```
====================================================================
  REPOSITORY HEALTH REPORT
  /path/to/sales-medallion-pipeline
====================================================================

  OVERALL GRADE: B   (83/100)
  [█████████████████░░░]
  Findings: 1 high, 7 medium, 5 low

--------------------------------------------------------------------
  CATEGORY STATUS
--------------------------------------------------------------------
  B  Code Quality                        88/100
  F  Medallion Structure & Naming        55/100
  C  Test Coverage                       78/100
  A  Dependency Health                   91/100
  A  Security                            97/100
  A  Cleanup & Refactor Hot-spots        91/100

--------------------------------------------------------------------
  TOP FINDINGS (most severe first, showing up to 10)
--------------------------------------------------------------------
  ✖ HIGH     Pipeline source has no automated test coverage
        9 test file(s) exist but none reference `src/`. The 14 pipeline
        notebooks under `src/` are untested.
        ↳ Refactor transformation logic out of the notebooks into pure
          functions and cover them with unit tests.
  ▲ MEDIUM   Undeclared third-party imports (1)
        ... provided by the Databricks runtime but not documented: pyspark.
  ▲ MEDIUM   Duplicate `01_` prefix in `gold` layer
        src/gold/01_daily_sales_summary.py, src/gold/01_gold_accounts.py
  ▪ LOW      Bare `except:` clauses (14)
  ...
```

## Run the tests

```bash
python -m pytest
```

The suite is hermetic — it builds small fixture repositories under a temp
directory and never touches the network or Databricks.
