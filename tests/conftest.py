"""Shared pytest fixtures and helpers.

Tests build small, self-contained repositories under ``tmp_path`` so they are
fully hermetic — no network, no Databricks, no dependence on the real repo.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

from tools.repo_health.discovery import AnalysisContext, build_context

FIXTURES = Path(__file__).parent / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def write_repo(root: Path, files: Dict[str, str]) -> Path:
    """Materialize a mapping of {relative_path: content} under ``root``."""
    for rel, content in files.items():
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
    return root


@pytest.fixture
def make_repo(tmp_path: Path):
    """Return a factory that writes a repo and returns its root Path."""

    def _factory(files: Dict[str, str]) -> Path:
        return write_repo(tmp_path, files)

    return _factory


@pytest.fixture
def make_ctx(make_repo):
    """Return a factory that writes a repo and returns its AnalysisContext."""

    def _factory(files: Dict[str, str]) -> AnalysisContext:
        root = make_repo(files)
        return build_context(root)

    return _factory
