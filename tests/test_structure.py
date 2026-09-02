"""Tests for the medallion structure & naming analyzer."""

from tools.repo_health.analyzers.structure import StructureAnalyzer


def test_duplicate_prefixes_detected(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_ingest_a.py": "a = 1\n",
            "src/bronze/01_ingest_b.py": "b = 1\n",  # duplicate 01_
            "src/silver/01_x.py": "c = 1\n",
            "src/gold/01_y.py": "d = 1\n",
        }
    )
    report = StructureAnalyzer().analyze(ctx)
    dup = [f for f in report.findings if "Duplicate `01_` prefix" in f.title]
    assert dup and "bronze" in dup[0].title
    assert report.metrics["duplicate_prefix_groups"] >= 1


def test_missing_layer_flagged(make_ctx):
    ctx = make_ctx({"src/bronze/01_a.py": "a = 1\n", "src/silver/01_b.py": "b = 1\n"})
    report = StructureAnalyzer().analyze(ctx)
    assert any("Missing `gold` layer" in f.title for f in report.findings)
    assert "gold" in report.metrics["layers_missing"]


def test_healthy_unique_prefixes_clean(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "a = 1\n",
            "src/silver/01_b.py": "b = 1\n",
            "src/gold/01_c.py": "c = 1\n",
            "src/gold/02_d.py": "d = 1\n",
        }
    )
    report = StructureAnalyzer().analyze(ctx)
    assert report.metrics["duplicate_prefix_groups"] == 0
    assert report.metrics["layers_missing"] == []
    assert report.findings == []


def test_nonstandard_name_flagged(make_ctx):
    ctx = make_ctx(
        {
            "src/bronze/01_a.py": "a = 1\n",
            "src/silver/01_b.py": "b = 1\n",
            "src/gold/CamelCase.py": "c = 1\n",  # no NN_ prefix, not snake
        }
    )
    report = StructureAnalyzer().analyze(ctx)
    assert any("Non-standard file names" in f.title for f in report.findings)
