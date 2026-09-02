"""Tests for the cleanup/refactor hot-spot analyzer."""

from tools.repo_health.analyzers.hotspots import HotspotAnalyzer

# A shared boilerplate block copied verbatim into two files.
_BOILERPLATE = (
    "try:\n"
    "    catalog = dbutils.widgets.get('catalog')\n"
    "    schema = dbutils.widgets.get('schema')\n"
    "except Exception:\n"
    "    catalog = 'demo'\n"
    "    schema = 'sales'\n"
)


def test_cross_file_duplication_detected(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": _BOILERPLATE + "a = 1\n",
            "src/silver/01_b.py": _BOILERPLATE + "b = 2\n",
        }
    )
    report = HotspotAnalyzer().analyze(ctx)
    assert report.metrics["duplicate_blocks"] >= 1
    assert report.metrics["files_with_duplication"] == 2
    assert any("Duplicated code" in f.title for f in report.findings)


def test_unique_files_not_duplicated(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "alpha = 1\nbeta = 2\ngamma = 3\ndelta = 4\n",
            "src/silver/01_b.py": "one = 1\ntwo = 2\nthree = 3\nfour = 4\n",
        }
    )
    report = HotspotAnalyzer().analyze(ctx)
    assert report.metrics["duplicate_blocks"] == 0


def test_dead_definition_detected(make_ctx):
    ctx = make_ctx(
        {"src/gold/01_a.py": "def never_called():\n    return 1\n\nx = 5\n"}
    )
    report = HotspotAnalyzer().analyze(ctx)
    assert report.metrics["dead_definitions"] == 1
    assert any("dead definitions" in f.title.lower() for f in report.findings)


def test_largest_files_reported(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_big.py": "x = 1\n" * 50,
            "src/silver/01_small.py": "y = 1\n",
        }
    )
    report = HotspotAnalyzer().analyze(ctx)
    largest = report.metrics["largest_files"]
    assert largest[0]["path"] == "src/bronze/01_big.py"
