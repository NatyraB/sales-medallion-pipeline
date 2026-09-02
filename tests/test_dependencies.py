"""Tests for the dependency-health analyzer."""

from tools.repo_health.analyzers.dependencies import DependencyAnalyzer
from tools.repo_health.core import Severity

_PYSPARK_SRC = "from pyspark.sql import functions as F\nx = F\n"


def test_no_manifest_is_high(make_ctx):
    ctx = make_ctx({"src/bronze/01_a.py": _PYSPARK_SRC})
    report = DependencyAnalyzer().analyze(ctx)
    assert any(
        f.severity == Severity.HIGH and "No dependency manifest" in f.title
        for f in report.findings
    )


def test_undeclared_pyspark_flagged(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": _PYSPARK_SRC,
            # Manifest exists but only declares dev tooling, not pyspark.
            "pyproject.toml": (
                "[project]\nname='x'\nversion='0'\ndependencies=[]\n"
                "[project.optional-dependencies]\ndev=['pytest>=7']\n"
            ),
        }
    )
    report = DependencyAnalyzer().analyze(ctx)
    assert "pyspark" in report.metrics["third_party_imports"]
    assert "pyspark" in report.metrics["undeclared_imports"]
    assert any("Undeclared third-party imports" in f.title for f in report.findings)


def test_declared_dependency_not_flagged(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "import requests\nx = requests\n",
            "requirements.txt": "requests==2.31.0\n",
        }
    )
    report = DependencyAnalyzer().analyze(ctx)
    assert report.metrics["undeclared_imports"] == []
    assert not any("Undeclared" in f.title for f in report.findings)


def test_stdlib_and_local_imports_not_third_party(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "import json\nimport os\nx = (json, os)\n",
            "requirements.txt": "\n",
        }
    )
    report = DependencyAnalyzer().analyze(ctx)
    assert report.metrics["third_party_imports"] == []
