"""Tests for the security analyzer.

Secret-like content is generated at runtime (never committed as a realistic
credential) so nothing here trips secret scanners or push protection.
"""

from tools.repo_health.analyzers.security import SecurityAnalyzer
from tools.repo_health.core import Severity


def test_hardcoded_secret_is_critical(make_ctx):
    # Assemble a fake AWS-key-shaped token at runtime.
    fake_key = "AKIA" + "Z" * 16
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": (
                'password = "sup3rsecretvalue"\n'
                f'aws = "{fake_key}"\n'
            )
        }
    )
    report = SecurityAnalyzer().analyze(ctx)
    crit = [f for f in report.findings if f.severity == Severity.CRITICAL]
    assert crit and report.metrics["secret_hits"] >= 2


def test_env_var_usage_is_not_flagged(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": (
                "import os\n"
                'token = os.environ["DATABRICKS_TOKEN"]\n'
                'other = dbutils.secrets.get(scope="s", key="k")\n'
            )
        }
    )
    report = SecurityAnalyzer().analyze(ctx)
    assert report.metrics["secret_hits"] == 0
    assert report.metrics["env_reference_lines"] >= 1


def test_workspace_url_is_low(make_ctx):
    ctx = make_ctx(
        {
            "databricks.yml": "host: https://my-workspace.cloud.databricks.com\n",
            "src/bronze/01_a.py": "x = 1\n",
        }
    )
    report = SecurityAnalyzer().analyze(ctx)
    low = [f for f in report.findings if "workspace host URL" in f.title]
    assert low and low[0].severity == Severity.LOW
    assert report.metrics["url_hits"] >= 1


def test_clean_repo_has_no_secret_findings(make_ctx):
    ctx = make_ctx({"src/bronze/01_a.py": "x = 1\ny = 'hello world'\n"})
    report = SecurityAnalyzer().analyze(ctx)
    assert report.metrics["secret_hits"] == 0
    assert report.metrics["url_hits"] == 0


def test_placeholder_value_not_flagged(make_ctx):
    ctx = make_ctx({"src/bronze/01_a.py": 'password = "changeme"\n'})
    report = SecurityAnalyzer().analyze(ctx)
    assert report.metrics["secret_hits"] == 0
