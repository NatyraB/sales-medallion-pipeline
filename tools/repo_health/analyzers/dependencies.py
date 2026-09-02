"""Dependency-health analyzer.

Flags a missing dependency manifest and third-party imports used by the
pipeline that no manifest declares (e.g. ``pyspark``). Uses ``tomllib`` (Python
3.11+ stdlib) to read pyproject.toml — no third-party parser required.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from typing import Dict, List, Set

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for < 3.11
    tomllib = None  # type: ignore[assignment]

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import AnalysisContext, SourceFile

# Module roots that are provided by the Databricks runtime / notebook env and
# therefore never appear as imports but should not be treated as missing deps.
DATABRICKS_RUNTIME = {"pyspark", "delta", "databricks", "dbutils", "mlflow"}

MANIFEST_FILES = (
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "setup.py",
    "setup.cfg",
    "Pipfile",
    "environment.yml",
)

_REQ_NAME_RE = re.compile(r"^([A-Za-z0-9._-]+)")


def _import_roots(sf: SourceFile) -> Set[str]:
    roots: Set[str] = set()
    if sf.tree is None:
        return roots
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                roots.add(node.module.split(".")[0])
    return roots


def _declared_from_pyproject(text: str) -> Set[str]:
    declared: Set[str] = set()
    if tomllib is None:
        return declared
    try:
        data = tomllib.loads(text)
    except (tomllib.TOMLDecodeError, ValueError):
        return declared
    project = data.get("project", {})
    specs: List[str] = list(project.get("dependencies", []) or [])
    for group in (project.get("optional-dependencies", {}) or {}).values():
        specs.extend(group or [])
    for spec in specs:
        match = _REQ_NAME_RE.match(spec.strip())
        if match:
            declared.add(match.group(1).lower())
    return declared


def _declared_from_requirements(text: str) -> Set[str]:
    declared: Set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        match = _REQ_NAME_RE.match(line)
        if match:
            declared.add(match.group(1).lower())
    return declared


class DependencyAnalyzer(Analyzer):
    key = "dependencies"
    title = "Dependency Health"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()
        root: Path = ctx.root

        # Local top-level module names (so we don't mistake them for third-party).
        local_names = {d for d in ctx.present_dirs}
        local_names.update(
            p.stem for p in root.glob("*.py") if p.stem != "__init__"
        )

        stdlib = set(getattr(sys, "stdlib_module_names", set()))

        third_party: Set[str] = set()
        for sf in ctx.source_layer_files():
            for rootmod in _import_roots(sf):
                if rootmod in stdlib or rootmod in local_names:
                    continue
                third_party.add(rootmod)

        # Which manifests exist, and what do they declare?
        manifests = [name for name in MANIFEST_FILES if (root / name).exists()]
        declared: Set[str] = set()
        for name in manifests:
            try:
                text = (root / name).read_text(encoding="utf-8")
            except OSError:
                continue
            if name == "pyproject.toml":
                declared |= _declared_from_pyproject(text)
            elif name.startswith("requirements") or name == "Pipfile":
                declared |= _declared_from_requirements(text)

        undeclared = sorted(m for m in third_party if m.lower() not in declared)

        if not manifests:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.HIGH,
                    title="No dependency manifest",
                    detail=(
                        "No pyproject.toml / requirements.txt / setup.py found. Environments "
                        "cannot be reproduced from the repo."
                    ),
                    recommendation="Add a manifest declaring the project's dependencies.",
                )
            )
        else:
            report.notes.append(
                "Manifest(s) present: " + ", ".join(sorted(manifests)) + "."
            )

        if undeclared:
            runtime_provided = [m for m in undeclared if m in DATABRICKS_RUNTIME]
            other = [m for m in undeclared if m not in DATABRICKS_RUNTIME]
            detail_bits = []
            if runtime_provided:
                detail_bits.append(
                    "provided by the Databricks runtime but not documented: "
                    + ", ".join(runtime_provided)
                )
            if other:
                detail_bits.append("not declared anywhere: " + ", ".join(other))
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.MEDIUM,
                    title=f"Undeclared third-party imports ({len(undeclared)})",
                    detail=(
                        "Pipeline code imports packages no manifest declares — "
                        + "; ".join(detail_bits)
                        + "."
                    ),
                    recommendation=(
                        "Declare runtime dependencies (or pin the Databricks Runtime version in a "
                        "README/manifest) so the environment is reproducible and auditable."
                    ),
                )
            )

        report.metrics = {
            "manifests": manifests,
            "third_party_imports": sorted(third_party),
            "undeclared_imports": undeclared,
            "declared_packages": sorted(declared),
        }
        return report
