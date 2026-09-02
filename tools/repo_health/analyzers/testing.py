"""Test-coverage analyzer.

The pipeline steps are Databricks notebooks (module-level code that depends on
`spark`/`dbutils`), so they are not import-and-unit-test friendly as written.
This analyzer reports whether any automated tests exist and, crucially, whether
any of them actually exercise the pipeline under ``src/``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import EXCLUDE_DIR_NAMES, AnalysisContext

TEST_FILE_RE = re.compile(r"(^test_.*\.py$)|(.*_test\.py$)")
PYTEST_CONFIG_FILES = ("pytest.ini", "tox.ini", "setup.cfg")
COVERAGE_CONFIG_FILES = (".coveragerc",)


def _find_test_files(root: Path) -> List[str]:
    tests: List[str] = []
    for path in sorted(root.rglob("*.py")):
        rel_parts = path.relative_to(root).parts
        # Skip virtualenvs / build dirs but DO look inside a `tests/` dir.
        if any(p in EXCLUDE_DIR_NAMES and p != "tests" for p in rel_parts):
            continue
        if TEST_FILE_RE.match(path.name):
            tests.append(path.relative_to(root).as_posix())
    return tests


class TestingAnalyzer(Analyzer):
    key = "testing"
    title = "Test Coverage"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()
        root = ctx.root

        test_files = _find_test_files(root)
        source_count = len(ctx.source_layer_files())

        # Detect pytest / coverage configuration.
        pytest_config = None
        for name in PYTEST_CONFIG_FILES:
            if (root / name).exists():
                pytest_config = name
                break
        pyproject = root / "pyproject.toml"
        if pytest_config is None and pyproject.exists():
            try:
                if "[tool.pytest" in pyproject.read_text(encoding="utf-8"):
                    pytest_config = "pyproject.toml"
            except OSError:
                pass
        coverage_config = next(
            (n for n in COVERAGE_CONFIG_FILES if (root / n).exists()), None
        )

        # Do any tests actually exercise the pipeline? We look for an import of
        # the pipeline package (`import src...` / `from src...`), not just a
        # mention of the string "src/" (which fixtures and path literals contain).
        tests_touch_src = False
        for rel in test_files:
            try:
                text = (root / rel).read_text(encoding="utf-8")
            except OSError:
                continue
            if re.search(r"^\s*(?:from|import)\s+src\b", text, re.MULTILINE):
                tests_touch_src = True
                break

        if not test_files:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.HIGH,
                    title="No automated tests found",
                    detail="The repository has no `test_*.py` / `*_test.py` files.",
                    recommendation=(
                        "Add a pytest suite. Extract pure transformation helpers from the "
                        "notebooks into importable modules so they can be unit tested."
                    ),
                )
            )
        elif source_count > 0 and not tests_touch_src:
            # The pipeline under src/ is the repository's primary code. Tests
            # existing elsewhere (e.g. for tooling) do not mitigate the fact
            # that none of the pipeline is exercised, so this is a HIGH gap —
            # the category grade should reflect zero pipeline coverage, not be
            # softened to an A by a single MEDIUM penalty.
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.HIGH,
                    title="Pipeline source has no automated test coverage",
                    detail=(
                        f"{len(test_files)} test file(s) exist but none reference `src/`. The "
                        f"{source_count} pipeline notebooks under `src/` are untested."
                    ),
                    recommendation=(
                        "Refactor transformation logic out of the notebooks into pure functions "
                        "and cover them with unit tests."
                    ),
                )
            )
        else:
            report.notes.append(
                f"{len(test_files)} test file(s) found, and at least one exercises `src/`."
            )

        if pytest_config is None:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title="No pytest configuration",
                    detail="No pytest config found (pytest.ini / setup.cfg / [tool.pytest.*]).",
                    recommendation="Add a `[tool.pytest.ini_options]` section to pyproject.toml.",
                )
            )
        else:
            report.notes.append(f"pytest configuration detected in `{pytest_config}`.")

        report.metrics = {
            "test_files": len(test_files),
            "source_files": source_count,
            "tests_target_src": tests_touch_src,
            "pytest_config": pytest_config,
            "coverage_config": coverage_config,
            # Coverage of the pipeline itself is effectively absent regardless of
            # tooling tests, because the notebooks are not import-testable.
            "pipeline_coverage": "0% (absent)" if not tests_touch_src else "partial",
        }
        return report
