"""End-to-end CLI tests: text mode, JSON mode, and exit codes."""

import json

from tools.repo_health.cli import main
from tools.repo_health.engine import run_analysis

# A miniature repo resembling the real one (two families, dup prefixes, no tests).
_MINI_REPO = {
    "databricks.yml": "bundle:\n  name: demo\nhost: https://x.cloud.databricks.com\n",
    "README.md": "# demo\n",
    "src/bronze/01_ingest_accounts.py": (
        "from pyspark.sql import functions as F\n"
        "try:\n    c = dbutils.widgets.get('c')\nexcept:\n    c = 'demo'\n"
        "print('ingest')\n"
    ),
    "src/bronze/01_ingest_customers.py": (
        "from pyspark.sql import functions as F\n"
        "try:\n    c = dbutils.widgets.get('c')\nexcept:\n    c = 'demo'\n"
        "print('ingest')\n"
    ),
    "src/silver/01_transform.py": "x = 1\n",
    "src/gold/01_agg.py": "y = 2\n",
}


def test_cli_text_mode_runs_and_exits_zero(make_repo, capsys):
    root = make_repo(_MINI_REPO)
    rc = main([str(root)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "REPOSITORY HEALTH REPORT" in out
    assert "OVERALL GRADE:" in out
    assert "CATEGORY STATUS" in out


def test_cli_json_mode_emits_valid_json(make_repo, capsys):
    root = make_repo(_MINI_REPO)
    rc = main(["--json", str(root)])
    out = capsys.readouterr().out
    assert rc == 0
    data = json.loads(out)  # must be valid JSON
    assert data["tool"] == "repo-health"
    assert set(data["overall"]) == {"score", "grade"}
    keys = [c["key"] for c in data["categories"]]
    assert keys == [
        "code_quality",
        "structure",
        "testing",
        "dependencies",
        "security",
        "hotspots",
    ]


def test_cli_fail_under_returns_nonzero(make_repo, capsys):
    root = make_repo(_MINI_REPO)
    rc = main(["--fail-under", "100", str(root)])
    capsys.readouterr()
    assert rc == 1  # a mini repo with issues cannot score 100


def test_cli_bad_path_returns_two(capsys):
    rc = main(["/nonexistent/path/xyz"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "not a directory" in err


def test_run_analysis_is_resilient_to_broken_files(make_repo):
    root = make_repo({**_MINI_REPO, "src/gold/02_broken.py": "def f(:\n    pass\n"})
    report = run_analysis(root)  # must not raise
    assert report.categories
    # The syntax error should surface as a finding, not a crash.
    titles = " ".join(f.title for f in report.all_findings())
    assert "syntax error" in titles.lower()
