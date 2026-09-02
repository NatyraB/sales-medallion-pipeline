"""Tests for repository discovery and resilient parsing."""

from tools.repo_health.discovery import build_context, find_repo_root
from tests.conftest import read_fixture


def test_find_repo_root_detects_marker(tmp_path):
    (tmp_path / "databricks.yml").write_text("bundle: {}\n", encoding="utf-8")
    nested = tmp_path / "src" / "bronze"
    nested.mkdir(parents=True)
    assert find_repo_root(nested) == tmp_path.resolve()


def test_build_context_collects_src_and_excludes_noise(make_repo):
    root = make_repo(
        {
            "src/bronze/01_a.py": "x = 1\n",
            "src/silver/01_b.py": "y = 2\n",
            ".venv/lib/pkg.py": "import sys\n",
            "tests/test_thing.py": "def test_x():\n    assert True\n",
            "databricks.yml": "bundle: {}\n",
        }
    )
    ctx = build_context(root)
    rels = {sf.rel for sf in ctx.source_files}
    assert "src/bronze/01_a.py" in rels
    assert "src/silver/01_b.py" in rels
    # Excluded directories are not analyzed as source.
    assert not any(r.startswith(".venv/") for r in rels)
    assert not any(r.startswith("tests/") for r in rels)
    # YAML is collected as a text file.
    assert any(tf.rel == "databricks.yml" for tf in ctx.text_files)


def test_build_context_records_syntax_error(make_repo):
    root = make_repo({"src/bronze/broken.py": read_fixture("broken_module.py")})
    ctx = build_context(root)
    broken = [sf for sf in ctx.source_files if sf.rel.endswith("broken.py")][0]
    assert broken.parse_error is not None
    assert broken.tree is None


def test_build_context_does_not_crash_on_bad_file(make_repo):
    root = make_repo({"src/gold/x.py": read_fixture("broken_module.py")})
    # Should return a context, not raise.
    ctx = build_context(root)
    assert len(ctx.source_layer_files()) == 1
