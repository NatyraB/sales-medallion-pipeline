"""repo-health: a dependency-light repository health report for this repo.

The tool statically analyzes the repository (no Spark/Databricks runtime, no
third-party imports) and prints a concise, prioritized health summary. See
``docs/repo_health.md`` for usage.
"""

from tools.repo_health.core import (
    CategoryReport,
    Finding,
    HealthReport,
    Severity,
    grade_for_score,
)

__version__ = "0.1.0"

__all__ = [
    "CategoryReport",
    "Finding",
    "HealthReport",
    "Severity",
    "grade_for_score",
    "__version__",
]
