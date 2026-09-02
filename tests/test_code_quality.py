"""Tests for the code-quality analyzer."""

from tools.repo_health.analyzers.code_quality import CodeQualityAnalyzer
from tools.repo_health.core import Severity
from tests.conftest import read_fixture


def _titles(report):
    return [f.title for f in report.findings]


def test_clean_module_has_no_quality_findings(make_ctx):
    ctx = make_ctx({"src/bronze/01_clean.py": read_fixture("clean_module.py")})
    report = CodeQualityAnalyzer().analyze(ctx)
    assert report.findings == []
    assert report.score == 100
    assert report.metrics["syntax_errors"] == 0


def test_messy_module_flags_all_patterns(make_ctx):
    ctx = make_ctx({"src/silver/01_messy.py": read_fixture("messy_module.py")})
    report = CodeQualityAnalyzer().analyze(ctx)
    m = report.metrics
    assert m["bare_excepts"] == 1
    assert m["wildcard_imports"] == 1
    assert m["print_files"] == 1
    assert m["unused_imports"] == 1  # `import json`
    assert m["todo_markers"] == 1
    joined = " ".join(_titles(report))
    assert "Bare `except:`" in joined
    assert "Wildcard imports" in joined


def test_syntax_error_is_high_finding_not_crash(make_ctx):
    ctx = make_ctx({"src/gold/01_broken.py": read_fixture("broken_module.py")})
    report = CodeQualityAnalyzer().analyze(ctx)
    assert report.metrics["syntax_errors"] == 1
    high = [f for f in report.findings if f.severity == Severity.HIGH]
    assert high and "syntax error" in high[0].title.lower()


def test_long_file_flagged(make_ctx):
    body = "x = 1\n" * 500
    ctx = make_ctx({"src/bronze/01_long.py": body})
    report = CodeQualityAnalyzer().analyze(ctx)
    assert any("Very long file" in f.title for f in report.findings)
    assert report.metrics["long_files"] == 1
