"""Tests for the test-coverage analyzer."""

from tools.repo_health.analyzers.testing import TestingAnalyzer
from tools.repo_health.core import Severity


def test_no_tests_is_high(make_ctx):
    ctx = make_ctx({"src/bronze/01_a.py": "a = 1\n"})
    report = TestingAnalyzer().analyze(ctx)
    high = [f for f in report.findings if f.severity == Severity.HIGH]
    assert high and "No automated tests" in high[0].title
    assert report.metrics["test_files"] == 0


def test_tests_present_but_not_touching_pipeline_is_medium(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "a = 1\n",
            "tests/test_tool.py": "def test_x():\n    assert 1 == 1\n",
        }
    )
    report = TestingAnalyzer().analyze(ctx)
    med = [f for f in report.findings if f.severity == Severity.MEDIUM]
    assert med and "no automated test coverage" in med[0].title.lower()
    assert report.metrics["test_files"] == 1
    assert report.metrics["tests_target_src"] is False


def test_tests_touching_pipeline_have_no_coverage_finding(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "a = 1\n",
            # A test that actually imports the pipeline package.
            "tests/test_pipeline.py": "from src.bronze import mod\n\ndef test_x():\n    assert mod\n",
        }
    )
    report = TestingAnalyzer().analyze(ctx)
    assert report.metrics["tests_target_src"] is True
    assert not any("no automated test coverage" in f.title.lower() for f in report.findings)


def test_pytest_config_detected(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "a = 1\n",
            "pyproject.toml": "[tool.pytest.ini_options]\ntestpaths = ['tests']\n",
        }
    )
    report = TestingAnalyzer().analyze(ctx)
    assert report.metrics["pytest_config"] == "pyproject.toml"
    assert not any("No pytest configuration" in f.title for f in report.findings)
