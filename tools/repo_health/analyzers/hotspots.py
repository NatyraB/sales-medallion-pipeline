"""Cleanup / refactor hot-spot analyzer.

Surfaces duplicated code across the layer scripts, the largest files, and
module-level dead code (functions/classes defined but never referenced).
"""

from __future__ import annotations

import ast
import hashlib
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import AnalysisContext, SourceFile

# Sliding-window size (in significant lines) for cross-file duplicate detection.
_WINDOW = 4


def _significant_lines(sf: SourceFile) -> List[str]:
    """Normalized lines with blanks, comments, and notebook magics removed."""
    out: List[str] = []
    for raw in sf.text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):  # comments + `# MAGIC`, `# COMMAND ----` markers
            continue
        # Collapse internal whitespace so trivial reformatting still matches.
        out.append(" ".join(line.split()))
    return out


def _dead_defs(sf: SourceFile) -> List[Tuple[str, int]]:
    """Module-level functions/classes never referenced by name in the file."""
    if sf.tree is None:
        return []
    defined: List[Tuple[str, int]] = []
    for node in sf.tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if not node.name.startswith("_"):
                defined.append((node.name, node.lineno))
    if not defined:
        return []
    used: Set[str] = set()
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(node.id)
    return [(name, line) for name, line in defined if name not in used]


class HotspotAnalyzer(Analyzer):
    key = "hotspots"
    title = "Cleanup & Refactor Hot-spots"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()
        files = ctx.source_layer_files()

        # ---- Largest files -------------------------------------------------
        largest = sorted(((sf.lines, sf.rel) for sf in files), reverse=True)[:3]

        # ---- Cross-file duplication ---------------------------------------
        window_files: Dict[str, Set[str]] = defaultdict(set)
        for sf in files:
            if sf.decode_error:
                continue
            sig = _significant_lines(sf)
            for i in range(len(sig) - _WINDOW + 1):
                block = "\n".join(sig[i : i + _WINDOW])
                digest = hashlib.sha1(block.encode("utf-8")).hexdigest()
                window_files[digest].add(sf.rel)

        duplicated_files: Set[str] = set()
        duplicate_blocks = 0
        for _, involved in window_files.items():
            if len(involved) >= 2:
                duplicate_blocks += 1
                duplicated_files |= involved

        if duplicate_blocks:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.MEDIUM,
                    title=f"Duplicated code across {len(duplicated_files)} layer scripts",
                    detail=(
                        f"{duplicate_blocks} repeated block(s) of >= {_WINDOW} lines span multiple "
                        "files — typically the widget/config setup and MERGE templates copied "
                        "between layers."
                    ),
                    recommendation=(
                        "Extract shared helpers (config resolution, MERGE-upsert, metadata tagging) "
                        "into a common module imported by each step."
                    ),
                )
            )

        # ---- Dead code -----------------------------------------------------
        dead_locs: List[str] = []
        for sf in files:
            for name, line in _dead_defs(sf):
                dead_locs.append(f"{sf.rel}:{line} ({name})")
        if dead_locs:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Possibly dead definitions ({len(dead_locs)})",
                    detail=(
                        "Module-level functions/classes never referenced in their file: "
                        + ", ".join(sorted(dead_locs)[:6])
                    ),
                    recommendation="Remove if unused, or add a call/test that exercises them.",
                )
            )

        if largest:
            report.notes.append(
                "Largest files: "
                + ", ".join(f"{rel} ({lines} lines)" for lines, rel in largest)
                + "."
            )

        report.metrics = {
            "largest_files": [{"path": rel, "lines": lines} for lines, rel in largest],
            "duplicate_blocks": duplicate_blocks,
            "files_with_duplication": len(duplicated_files),
            "dead_definitions": len(dead_locs),
        }
        return report
